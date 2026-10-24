// Package frontier provides a distributed URL crawl queue (frontier) using Redis Streams
// and consumer groups for horizontal worker scalability, fault tolerance, and dead-letter queueing.
package frontier

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

var (
	// ErrEmptyFrontier is returned when no items are available in the queue.
	ErrEmptyFrontier = errors.New("frontier queue is empty")

	// ErrClosed is returned when operating on a closed frontier.
	ErrClosed = errors.New("frontier is closed")

	// ErrInvalidItem is returned when a queue item has an invalid URL.
	ErrInvalidItem = errors.New("invalid frontier item: empty or malformed URL")
)

// Item represents a single URL crawl task inside the distributed frontier stream.
type Item struct {
	ID        string    `json:"id,omitempty"`        // Redis stream message ID (e.g. "1698064800000-0")
	URL       string    `json:"url"`                 // Target URL to fetch
	Depth     int       `json:"depth"`               // Depth level from seed
	Domain    string    `json:"domain"`              // Extracted domain for politeness partitioning
	Retries   int       `json:"retries"`             // Number of failed fetch attempts
	EnqueuedAt time.Time `json:"enqueued_at,omitempty"` // Timestamp when URL was enqueued
}

// DLQItem represents a failed crawl task routed to the Dead Letter Queue.
type DLQItem struct {
	Item         Item      `json:"item"`
	Reason       string    `json:"reason"`
	FailedAt     time.Time `json:"failed_at"`
	OriginalID   string    `json:"original_id"`
}

// Frontier defines the interface for distributed or local crawl queue backends.
type Frontier interface {
	// Enqueue adds a single item to the frontier stream.
	Enqueue(ctx context.Context, item Item) (string, error)

	// EnqueueBatch adds multiple items atomically or via pipeline.
	EnqueueBatch(ctx context.Context, items []Item) error

	// Dequeue fetches up to count items assigned to the named consumer.
	// If block > 0, it waits up to block duration for new messages.
	Dequeue(ctx context.Context, consumer string, count int64, block time.Duration) ([]Item, error)

	// Ack acknowledges successful processing of one or more message IDs.
	Ack(ctx context.Context, messageIDs ...string) error

	// RouteToDLQ routes a poison pill or max-retried item to the Dead Letter Queue.
	RouteToDLQ(ctx context.Context, item Item, reason string) error

	// ClaimStale reclaims unacknowledged messages from crashed worker pods.
	ClaimStale(ctx context.Context, consumer string, minIdle time.Duration, count int64) ([]Item, error)

	// Length returns the current number of pending items in the active queue.
	Length(ctx context.Context) (int64, error)

	// DLQLength returns the number of items currently in the Dead Letter Queue.
	DLQLength(ctx context.Context) (int64, error)

	// Close terminates the frontier connection.
	Close() error
}

// QueueStreamKey returns the Redis Stream key for a crawl job's active queue.
func QueueStreamKey(jobID string) string {
	if jobID == "" {
		jobID = "default"
	}
	return fmt.Sprintf("frontier:queue:%s", jobID)
}

// DLQStreamKey returns the Redis Stream key for a crawl job's Dead Letter Queue.
func DLQStreamKey(jobID string) string {
	if jobID == "" {
		jobID = "default"
	}
	return fmt.Sprintf("frontier:dlq:%s", jobID)
}

// ConsumerGroupName returns the consumer group identifier for a crawl job.
func ConsumerGroupName(jobID string) string {
	if jobID == "" {
		jobID = "default"
	}
	return fmt.Sprintf("frontier:workers:%s", jobID)
}

// NewItem creates and validates an Item with normalized domain and enqueued timestamp.
func NewItem(rawURL string, depth int) (Item, error) {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return Item{}, ErrInvalidItem
	}

	u, err := url.Parse(trimmed)
	if err != nil || u.Host == "" {
		return Item{}, fmt.Errorf("%w: %v", ErrInvalidItem, err)
	}

	return Item{
		URL:        trimmed,
		Depth:      depth,
		Domain:     strings.ToLower(u.Host),
		Retries:    0,
		EnqueuedAt: time.Now().UTC(),
	}, nil
}

// MarshalItem encodes an Item into JSON bytes.
func MarshalItem(item Item) ([]byte, error) {
	return json.Marshal(item)
}

// UnmarshalItem decodes JSON bytes into an Item.
func UnmarshalItem(data []byte) (Item, error) {
	var item Item
	err := json.Unmarshal(data, &item)
	return item, err
}
