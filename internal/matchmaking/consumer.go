package matchmaking

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"multiplayer-backend-sim/internal/liveops"

	"github.com/redis/go-redis/v9"
)

const ConsumerGroupName = "matchmaking_group"

// StaleMessageTimeout is how long a message must sit un-ACKed in a consumer's
// PEL before another healthy worker is allowed to claim and reprocess it.
const StaleMessageTimeout = 5 * time.Second

type Consumer struct {
	redisClient    *redis.Client
	queueStore     QueueStore
	matchStore     MatchStore
	logger         *slog.Logger
	consumerName   string // unique per worker instance
	staleTimeout   time.Duration
	blockTimeout   time.Duration // how long XReadGroup blocks per iteration
	configProvider ConfigProvider
}

type staticConfigProvider struct {
	cfg liveops.MatchmakingConfigPayload
}

func (s *staticConfigProvider) GetConfig(ctx context.Context) liveops.MatchmakingConfigPayload {
	return s.cfg
}

func NewConsumer(
	redisClient *redis.Client,
	qs QueueStore,
	ms MatchStore,
	logger *slog.Logger,
	consumerName string, // now injected rather than hardcoded
) *Consumer {
	return &Consumer{
		redisClient:    redisClient,
		queueStore:     qs,
		matchStore:     ms,
		logger:         logger,
		consumerName:   consumerName,
		staleTimeout:   StaleMessageTimeout,
		blockTimeout:   2 * time.Second,
		configProvider: &staticConfigProvider{cfg: DefaultMatchmakingConfig()},
	}
}

func (c *Consumer) WithConfigProvider(cp ConfigProvider) *Consumer {
	if cp != nil {
		c.configProvider = cp
	}
	return c
}

// WithStaleTimeout overrides the default stale message timeout (useful for tests).
func (c *Consumer) WithStaleTimeout(d time.Duration) *Consumer {
	c.staleTimeout = d
	return c
}

// WithBlockTimeout overrides how long XReadGroup blocks waiting for new messages.
// Shorter values mean the loop re-runs XAutoClaim more frequently, which is
// important in tests where we need the worker to re-evaluate a PEL entry quickly
// after a LiveOps config change. Production default is 2 s.
func (c *Consumer) WithBlockTimeout(d time.Duration) *Consumer {
	c.blockTimeout = d
	return c
}

func (c *Consumer) SetupGroup(ctx context.Context) error {
	err := c.redisClient.XGroupCreateMkStream(ctx, MatchmakingStreamKey, ConsumerGroupName, "$").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

func (c *Consumer) Run(ctx context.Context) {
	c.logger.Info("matchmaking consumer started", "consumer", c.consumerName)

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("matchmaking consumer shutting down", "consumer", c.consumerName)
			return
		default:
		}

		// 1. RECOVERY PATH: Claim any messages that have been sitting
		// un-ACKed in any consumer's PEL longer than staleTimeout.
		// This absorbs work from crashed worker instances automatically.
		// Crucially, it also re-evaluates PEL entries with the *current*
		// LiveOps config on every loop, which is what makes config-change
		// rollouts work without requiring new stream messages to arrive.
		claimed, _, err := c.redisClient.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   MatchmakingStreamKey,
			Group:    ConsumerGroupName,
			Consumer: c.consumerName,
			MinIdle:  c.staleTimeout,
			Start:    "0-0", // scan from the very beginning of the PEL
			Count:    50,
		}).Result()

		if err != nil && !errors.Is(err, redis.Nil) {
			c.logger.Error("xautoclaim failed", "error", err, "consumer", c.consumerName)
		}

		if len(claimed) > 0 {
			c.logger.Info("reclaimed stale messages", "count", len(claimed), "consumer", c.consumerName)
			c.processMessages(ctx, claimed)
		}

		// 2. NORMAL PATH: Read new, never-delivered messages using ">".
		// Block for at most blockTimeout so the loop can cycle back to
		// XAutoClaim promptly (important after a config change).
		streams, err := c.redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    ConsumerGroupName,
			Consumer: c.consumerName,
			Streams:  []string{MatchmakingStreamKey, ">"},
			Count:    50,
			Block:    c.blockTimeout,
		}).Result()

		if err != nil {
			// redis.Nil means the blocking read timed out with no new messages —
			// that is normal and expected; just loop back to XAutoClaim.
			if errors.Is(err, redis.Nil) {
				continue
			}
			// Context cancelled: clean shutdown.
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return
			}
			c.logger.Error("failed to read from redis stream", "error", err)
			time.Sleep(1 * time.Second)
			continue
		}

		if len(streams) == 0 || len(streams[0].Messages) == 0 {
			continue
		}

		c.processMessages(ctx, streams[0].Messages)
	}
}

// processMessages is the shared processing path for both fresh messages
// (from XReadGroup ">") and recovered messages (from XAutoClaim).
func (c *Consumer) processMessages(ctx context.Context, messages []redis.XMessage) {
	var batch []MatchMakingJob
	messageIDs := make(map[string]string) // PlayerID -> Redis Message ID

	for _, msg := range messages {
		payloadStr, ok := msg.Values["payload"].(string)
		if !ok {
			c.logger.Error("malformed payload in stream", "msg_id", msg.ID)
			c.redisClient.XAck(ctx, MatchmakingStreamKey, ConsumerGroupName, msg.ID)
			continue
		}

		var job MatchMakingJob
		if err := json.Unmarshal([]byte(payloadStr), &job); err != nil {
			c.logger.Error("failed to unmarshal job", "error", err, "msg_id", msg.ID)
			c.redisClient.XAck(ctx, MatchmakingStreamKey, ConsumerGroupName, msg.ID)
			continue
		}
		batch = append(batch, job)
		messageIDs[job.PlayerID] = msg.ID
	}

	// 1. Retrieve the active LiveOps configuration (cached in memory)
	cfg := c.configProvider.GetConfig(ctx)

	// 2. Pair players according to dynamic MaxSkillDifference and MatchSize
	matches := ProcessBatch(batch, cfg)

	for _, matchGroup := range matches {
		playerIDs := make([]string, len(matchGroup))
		for i, job := range matchGroup {
			playerIDs[i] = job.PlayerID
		}

		// 3. Atomically persist the match and its participants in PostgreSQL
		_, err := c.matchStore.CreateMatch(ctx, matchGroup[0].Region, playerIDs)
		if err != nil {
			c.logger.Error("failed to create match in postgres", "error", err)
			// Do NOT ACK — leave in PEL so XAutoClaim can recover it later
			continue
		}

		allStatusUpdated := true
		var msgIDsToAck []string

		// 4. Transition each player's ticket status to MATCHED in PostgreSQL
		for _, job := range matchGroup {
			activeEntry, err := c.queueStore.GetActiveByPlayerID(ctx, job.PlayerID)
			if err != nil {
				c.logger.Error("failed to find active queue entry for player", "player_id", job.PlayerID, "error", err)
				allStatusUpdated = false
				continue
			}

			if err := c.queueStore.UpdateStatus(ctx, activeEntry.ID, QueueStatusMatched); err != nil {
				c.logger.Error("failed to update queue entry status to MATCHED", "entry_id", activeEntry.ID, "player_id", job.PlayerID, "error", err)
				allStatusUpdated = false
				continue
			}

			if msgID, exists := messageIDs[job.PlayerID]; exists {
				msgIDsToAck = append(msgIDsToAck, msgID)
			}
		}

		// 5. Commit acknowledgement in Redis only if all database state was successfully committed
		if allStatusUpdated && len(msgIDsToAck) > 0 {
			c.redisClient.XAck(ctx, MatchmakingStreamKey, ConsumerGroupName, msgIDsToAck...)
		} else {
			c.logger.Warn("skipped redis XAck because one or more queue status updates failed", "players", playerIDs)
		}

		c.logger.Info("match created", "region", matchGroup[0].Region, "players", playerIDs, "match_size", len(playerIDs))
	}
}
