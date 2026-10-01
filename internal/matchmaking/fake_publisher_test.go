package matchmaking

import (
	"context"
)

// FakeQueuePublisher is an in-memory mock implementation for unit testing.
type FakeQueuePublisher struct {
	JobsPublished []MatchMakingJob
	ErrToReturn   error
}

// NewFakeQueuePublisher creates a new FakeQueuePublisher.
func NewFakeQueuePublisher() *FakeQueuePublisher {
	return &FakeQueuePublisher{
		JobsPublished: make([]MatchMakingJob, 0),
	}
}

// Publish stores the job in memory so tests can assert it was sent.
func (f *FakeQueuePublisher) Publish(ctx context.Context, job MatchMakingJob) error {
	if f.ErrToReturn != nil {
		return f.ErrToReturn
	}
	f.JobsPublished = append(f.JobsPublished, job)
	return nil
}