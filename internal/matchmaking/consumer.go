package matchmaking

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

const ConsumerGroupName = "matchmaking_group"

// StaleMessageTimeout is how long a message must sit un-ACKed in a consumer's
// PEL before another healthy worker is allowed to claim and reprocess it.
const StaleMessageTimeout = 5 * time.Second

type Consumer struct {
	redisClient  *redis.Client
	queueStore   QueueStore
	matchStore   MatchStore
	logger       *slog.Logger
	consumerName string // unique per worker instance
	staleTimeout time.Duration
}

func NewConsumer(
	redisClient *redis.Client,
	qs QueueStore,
	ms MatchStore,
	logger *slog.Logger,
	consumerName string, // now injected rather than hardcoded
) *Consumer {
	return &Consumer{
		redisClient:  redisClient,
		queueStore:   qs,
		matchStore:   ms,
		logger:       logger,
		consumerName: consumerName,
		staleTimeout: StaleMessageTimeout,
	}
}

// WithStaleTimeout overrides the default stale message timeout (useful for tests).
func (c *Consumer) WithStaleTimeout(d time.Duration) *Consumer {
	c.staleTimeout = d
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
		// un-ACKed in any consumer's PEL longer than StaleMessageTimeout.
		// This absorbs work from crashed worker instances automatically.
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
		streams, err := c.redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    ConsumerGroupName,
			Consumer: c.consumerName,
			Streams:  []string{MatchmakingStreamKey, ">"},
			Count:    50,
			Block:    2 * time.Second,
		}).Result()

		if err != nil {
			if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
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

	matches := ProcessBatch(batch)

	for _, matchPair := range matches {
		playerIDs := []string{matchPair[0].PlayerID, matchPair[1].PlayerID}

		_, err := c.matchStore.CreateMatch(ctx, matchPair[0].Region, playerIDs)
		if err != nil {
			c.logger.Error("failed to create match in postgres", "error", err)
			// Do NOT ACK — leave in PEL for next recovery cycle
			continue
		}

		allStatusUpdated := true
		var msgIDsToAck []string

		for _, job := range matchPair {
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

		// Only ACK in Redis if all queue status transitions were successfully written to Postgres
		if allStatusUpdated && len(msgIDsToAck) > 0 {
			c.redisClient.XAck(ctx, MatchmakingStreamKey, ConsumerGroupName, msgIDsToAck...)
		} else {
			c.logger.Warn("skipped redis XAck because one or more queue status updates failed", "players", playerIDs)
		}

		c.logger.Info("match created", "region", matchPair[0].Region, "players", playerIDs)
	}
}