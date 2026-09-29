package matchmaking

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
)

const MatchmakingStreamKey = "matchmaking_stream"

type MatchMakingJob struct {
	PlayerID    string `json:"player_id"`
	SkillRating int    `json:"skill_rating"`
	Region      string `json:"region"`
}

type QueuePublisher interface {
	Publish(ctx context.Context, job MatchMakingJob) error
}

type RedisQueuePublisher struct {
	client *redis.Client
}
func NewRedisQueuePublisher(client *redis.Client) *RedisQueuePublisher {
	return &RedisQueuePublisher{
		client: client,
	}
}

func (p *RedisQueuePublisher) Publish(ctx context.Context,job MatchMakingJob) error {
	payload,err := json.Marshal(job)
	if err!= nil {
		return fmt.Errorf("marshaling matchmaking job: %w", err)
	}
	err = p.client.XAdd(ctx,&redis.XAddArgs{
		Stream: MatchmakingStreamKey,
		Values: map[string]interface{}{
			"payload":payload,
		},
	}).Err()

	if err!=nil {
		return fmt.Errorf("publishing to redis stream: %w", err)
	}
	return nil
}