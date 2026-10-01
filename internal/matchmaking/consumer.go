package matchmaking

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"
	"github.com/redis/go-redis/v9"
)

const (
	ConsumerGroupName = "matchmaking_group"
	ConsumerName      = "matchmaking_worker_1"
)

type Consumer struct {
	redisClient *redis.Client
	queueStore QueueStore
	matchStore MatchStore
	logger *slog.Logger
}


func NewConsumer(redisClient *redis.Client, qs QueueStore,ms MatchStore, logger *slog.Logger) *Consumer {
	return &Consumer{
		redisClient: redisClient,
		queueStore:  qs,
		matchStore:  ms,
		logger:      logger,
	}
}

func (c *Consumer) SetupGroup(ctx context.Context) error {
	err:= c.redisClient.XGroupCreateMkStream(ctx,MatchmakingStreamKey,ConsumerGroupName,"$").Err()

	if err!=nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

func (c *Consumer) Run(ctx context.Context) {
	c.logger.Info("matchmaking consumer started and listening for jobs")

	// infinite background loop
	for {
		select {
		case <- ctx.Done():
			c.logger.Info("matchmaking consumer shutting down")
			return
		default:
		}

		streams,err := c.redisClient.XReadGroup(ctx,&redis.XReadGroupArgs{
			Group: ConsumerGroupName,
			Consumer: ConsumerName,
			Streams: []string{MatchmakingStreamKey,">"},
			Count: 50,
			Block: 2*time.Second,
		}).Result()

		if err!=nil {
			if errors.Is(err,redis.Nil) || errors.Is(err,context.Canceled) {
				return
			}
			c.logger.Error("failed to read from redis stream", "error", err)
			time.Sleep(1 * time.Second) // Prevent tight loop if Redis temporarily crashes
			continue
		}

		if len(streams) == 0 || len(streams[0].Messages) == 0 {
			continue
		}

		var batch []MatchMakingJob
		messageIDs := make(map[string]string) // Maps PlayerID -> Redis Message ID for ACKing

		for _,msg := range streams[0].Messages {
			payloadStr,ok := msg.Values["payload"].(string)

			if !ok {
				c.logger.Error("malformed payload in stream", "msg_id", msg.ID)
				c.redisClient.XAck(ctx, MatchmakingStreamKey, ConsumerGroupName, msg.ID)
				continue
			}

			var job MatchMakingJob
			if err := json.Unmarshal([]byte(payloadStr),&job); err!=nil {
				c.logger.Error("failed to unmarshal job", "error", err, "msg_id", msg.ID)
				c.redisClient.XAck(ctx, MatchmakingStreamKey, ConsumerGroupName, msg.ID)
				continue
			}
			batch = append(batch, job)
			messageIDs[job.PlayerID] = msg.ID
		}

		matches := ProcessBatch(batch)

		for _,matchPair := range matches{
			playerIDs := []string{matchPair[0].PlayerID,matchPair[1].PlayerID}
			_,err := c.matchStore.CreateMatch(ctx,matchPair[0].Region,playerIDs)
			if err != nil {
				c.logger.Error("failed to create match in postgres", "error", err)
				continue // Skip ACKing! Allow Redis to keep this message pending so we don't lose the players.
			}

			var msgIDsToAck []string

			for _,job :=range matchPair {
				activeEntry, err := c.queueStore.GetActiveByPlayerID(ctx, job.PlayerID)
				if err == nil {
					_ = c.queueStore.UpdateStatus(ctx, activeEntry.ID, QueueStatusMatched)
				}
				if msgID, exists := messageIDs[job.PlayerID]; exists {
					msgIDsToAck = append(msgIDsToAck, msgID)
				}
			}

			if len(msgIDsToAck) > 0 {
				c.redisClient.XAck(ctx, MatchmakingStreamKey, ConsumerGroupName, msgIDsToAck...)
			}
			
			c.logger.Info("match successfully created", "region", matchPair[0].Region, "players", playerIDs)
		}
	}
}