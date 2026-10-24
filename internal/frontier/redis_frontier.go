package frontier

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisFrontier implements Frontier backed by Redis Streams (XADD, XREADGROUP, XACK, XAUTOCLAIM).
type RedisFrontier struct {
	client    *redis.Client
	jobID     string
	group     string
	streamKey string
	dlqKey    string
}

// Config holds connection parameters for RedisFrontier.
type Config struct {
	Addr     string // host:port e.g. "localhost:6379"
	Password string // optional Redis password
	DB       int    // Redis logical database index
	JobID    string // Crawl Job UUID
	Group    string // Consumer Group Name (defaults to ConsumerGroupName(jobID))
}

// NewRedisFrontier initializes a Redis client, configures stream keys, and creates the consumer group.
func NewRedisFrontier(ctx context.Context, cfg Config) (*RedisFrontier, error) {
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:6379"
	}
	if cfg.JobID == "" {
		cfg.JobID = "default"
	}
	if cfg.Group == "" {
		cfg.Group = ConsumerGroupName(cfg.JobID)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	rf := &RedisFrontier{
		client:    rdb,
		jobID:     cfg.JobID,
		group:     cfg.Group,
		streamKey: QueueStreamKey(cfg.JobID),
		dlqKey:    DLQStreamKey(cfg.JobID),
	}

	if err := rf.InitGroup(ctx); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("failed to initialize consumer group: %w", err)
	}

	return rf, nil
}

// NewFromClient creates a RedisFrontier from an existing redis.Client.
func NewFromClient(ctx context.Context, client *redis.Client, jobID, group string) (*RedisFrontier, error) {
	if jobID == "" {
		jobID = "default"
	}
	if group == "" {
		group = ConsumerGroupName(jobID)
	}

	rf := &RedisFrontier{
		client:    client,
		jobID:     jobID,
		group:     group,
		streamKey: QueueStreamKey(jobID),
		dlqKey:    DLQStreamKey(jobID),
	}

	if err := rf.InitGroup(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize consumer group: %w", err)
	}

	return rf, nil
}

// InitGroup creates the Redis Streams consumer group with MKSTREAM if it does not already exist.
func (rf *RedisFrontier) InitGroup(ctx context.Context) error {
	err := rf.client.XGroupCreateMkStream(ctx, rf.streamKey, rf.group, "$").Err()
	if err != nil {
		// Ignore BUSYGROUP error: group already exists
		if strings.Contains(err.Error(), "BUSYGROUP") {
			return nil
		}
		return err
	}
	return nil
}

// Enqueue adds a single Item to the Redis Stream.
func (rf *RedisFrontier) Enqueue(ctx context.Context, item Item) (string, error) {
	if item.URL == "" {
		return "", ErrInvalidItem
	}
	if item.EnqueuedAt.IsZero() {
		item.EnqueuedAt = time.Now().UTC()
	}

	payload, err := json.Marshal(item)
	if err != nil {
		return "", fmt.Errorf("failed to marshal item: %w", err)
	}

	msgID, err := rf.client.XAdd(ctx, &redis.XAddArgs{
		Stream: rf.streamKey,
		Values: map[string]interface{}{
			"payload": string(payload),
			"url":     item.URL,
			"domain":  item.Domain,
		},
	}).Result()
	if err != nil {
		return "", fmt.Errorf("redis XADD error: %w", err)
	}

	return msgID, nil
}

// EnqueueBatch adds multiple items atomically using a Redis pipeline.
func (rf *RedisFrontier) EnqueueBatch(ctx context.Context, items []Item) error {
	if len(items) == 0 {
		return nil
	}

	pipe := rf.client.Pipeline()
	now := time.Now().UTC()

	for _, item := range items {
		if item.URL == "" {
			continue
		}
		if item.EnqueuedAt.IsZero() {
			item.EnqueuedAt = now
		}
		payload, err := json.Marshal(item)
		if err != nil {
			return fmt.Errorf("failed to marshal batch item %s: %w", item.URL, err)
		}

		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: rf.streamKey,
			Values: map[string]interface{}{
				"payload": string(payload),
				"url":     item.URL,
				"domain":  item.Domain,
			},
		})
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("redis pipeline XADD error: %w", err)
	}

	return nil
}

// Dequeue fetches up to count items assigned to the named consumer using XREADGROUP.
func (rf *RedisFrontier) Dequeue(ctx context.Context, consumer string, count int64, block time.Duration) ([]Item, error) {
	if count <= 0 {
		count = 10
	}

	res, err := rf.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    rf.group,
		Consumer: consumer,
		Streams:  []string{rf.streamKey, ">"},
		Count:    count,
		Block:    block,
	}).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("redis XREADGROUP error: %w", err)
	}

	var items []Item
	for _, stream := range res {
		for _, msg := range stream.Messages {
			item, parseErr := parseMessage(msg)
			if parseErr != nil {
				// Log or skip malformed message
				continue
			}
			items = append(items, item)
		}
	}

	return items, nil
}

// Ack acknowledges completion of one or more message IDs in the consumer group.
func (rf *RedisFrontier) Ack(ctx context.Context, messageIDs ...string) error {
	if len(messageIDs) == 0 {
		return nil
	}
	return rf.client.XAck(ctx, rf.streamKey, rf.group, messageIDs...).Err()
}

// RouteToDLQ logs a permanent failure to the Dead Letter Queue, then ACKs and deletes the message from the active queue.
func (rf *RedisFrontier) RouteToDLQ(ctx context.Context, item Item, reason string) error {
	dlqEntry := DLQItem{
		Item:       item,
		Reason:     reason,
		FailedAt:   time.Now().UTC(),
		OriginalID: item.ID,
	}

	data, err := json.Marshal(dlqEntry)
	if err != nil {
		return fmt.Errorf("failed to marshal DLQ item: %w", err)
	}

	_, err = rf.client.XAdd(ctx, &redis.XAddArgs{
		Stream: rf.dlqKey,
		Values: map[string]interface{}{
			"payload": string(data),
			"url":     item.URL,
			"reason":  reason,
		},
	}).Result()
	if err != nil {
		return fmt.Errorf("failed to push to DLQ stream: %w", err)
	}

	// Ack and remove from main stream
	if item.ID != "" {
		_ = rf.client.XAck(ctx, rf.streamKey, rf.group, item.ID).Err()
		_ = rf.client.XDel(ctx, rf.streamKey, item.ID).Err()
	}

	return nil
}

// ClaimStale uses Redis XAUTOCLAIM to reclaim pending un-ACKed messages from crashed or stalled worker pods.
func (rf *RedisFrontier) ClaimStale(ctx context.Context, consumer string, minIdle time.Duration, count int64) ([]Item, error) {
	if count <= 0 {
		count = 50
	}

	messages, _, err := rf.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   rf.streamKey,
		Group:    rf.group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    "0-0",
		Count:    count,
	}).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("redis XAUTOCLAIM error: %w", err)
	}

	var items []Item
	for _, msg := range messages {
		item, parseErr := parseMessage(msg)
		if parseErr != nil {
			continue
		}
		items = append(items, item)
	}

	return items, nil
}

// Length returns the current length of the active queue stream.
func (rf *RedisFrontier) Length(ctx context.Context) (int64, error) {
	return rf.client.XLen(ctx, rf.streamKey).Result()
}

// DLQLength returns the number of items in the Dead Letter Queue.
func (rf *RedisFrontier) DLQLength(ctx context.Context) (int64, error) {
	return rf.client.XLen(ctx, rf.dlqKey).Result()
}

// Close shuts down the Redis client connection.
func (rf *RedisFrontier) Close() error {
	return rf.client.Close()
}

// StreamKey returns the active Redis Stream key.
func (rf *RedisFrontier) StreamKey() string {
	return rf.streamKey
}

// DLQKey returns the Dead Letter Queue stream key.
func (rf *RedisFrontier) DLQKey() string {
	return rf.dlqKey
}

// Group returns the consumer group name.
func (rf *RedisFrontier) Group() string {
	return rf.group
}

// JanitorConfig configures the background orphan recovery janitor.
type JanitorConfig struct {
	Interval   time.Duration // Time between janitor scan cycles (default: 30s)
	MinIdle    time.Duration // Threshold after which a message is considered orphaned (default: 60s)
	MaxRetries int           // Max retries before routing to DLQ (default: 3)
	BatchSize  int64         // Maximum messages to reclaim per cycle (default: 50)
	Consumer   string        // Consumer identity for the janitor (default: "janitor")
}

// RunJanitor runs a periodic loop that scans for stale in-flight messages from dead workers,
// reclaims them, and either re-enqueues them or routes them to the DLQ if retry limits are exceeded.
func (rf *RedisFrontier) RunJanitor(ctx context.Context, cfg JanitorConfig) error {
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.MinIdle <= 0 {
		cfg.MinIdle = 60 * time.Second
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 50
	}
	if cfg.Consumer == "" {
		cfg.Consumer = "janitor"
	}

	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			staleItems, err := rf.ClaimStale(ctx, cfg.Consumer, cfg.MinIdle, cfg.BatchSize)
			if err != nil {
				continue
			}

			for _, item := range staleItems {
				if item.Retries >= cfg.MaxRetries {
					_ = rf.RouteToDLQ(ctx, item, fmt.Sprintf("stale message exceeded max retries (%d)", cfg.MaxRetries))
				} else {
					// Increment retry counter, re-enqueue, and ACK old message
					item.Retries++
					_, _ = rf.Enqueue(ctx, item)
					_ = rf.Ack(ctx, item.ID)
				}
			}
		}
	}
}

func parseMessage(msg redis.XMessage) (Item, error) {
	item := Item{ID: msg.ID}

	if rawPayload, ok := msg.Values["payload"].(string); ok && rawPayload != "" {
		if err := json.Unmarshal([]byte(rawPayload), &item); err == nil {
			item.ID = msg.ID
			return item, nil
		}
	}

	// Fallback to individual fields if payload json isn't present
	if rawURL, ok := msg.Values["url"].(string); ok {
		item.URL = rawURL
	}
	if rawDomain, ok := msg.Values["domain"].(string); ok {
		item.Domain = rawDomain
	}

	return item, nil
}
