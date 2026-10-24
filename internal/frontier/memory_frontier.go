package frontier

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MemoryFrontier is an in-memory, thread-safe implementation of Frontier.
// Useful for local testing, embedded crawls, and zero-dependency environments.
type MemoryFrontier struct {
	mu       sync.Mutex
	items    []Item
	pending  map[string]Item        // consumer/in-flight message ID -> Item
	pendingTimes map[string]time.Time // message ID -> dequeue time
	dlq      []DLQItem
	seq      int64
	closed   bool
	notifyCh chan struct{}
}

// NewMemoryFrontier creates a new in-memory Frontier.
func NewMemoryFrontier() *MemoryFrontier {
	return &MemoryFrontier{
		items:        make([]Item, 0),
		pending:      make(map[string]Item),
		pendingTimes: make(map[string]time.Time),
		dlq:          make([]DLQItem, 0),
		notifyCh:     make(chan struct{}, 1),
	}
}

func (m *MemoryFrontier) notify() {
	select {
	case m.notifyCh <- struct{}{}:
	default:
	}
}

// Enqueue adds a single item to the in-memory queue.
func (m *MemoryFrontier) Enqueue(ctx context.Context, item Item) (string, error) {
	if item.URL == "" {
		return "", ErrInvalidItem
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return "", ErrClosed
	}

	m.seq++
	item.ID = fmt.Sprintf("%d-0", m.seq)
	if item.EnqueuedAt.IsZero() {
		item.EnqueuedAt = time.Now().UTC()
	}

	m.items = append(m.items, item)
	m.notify()

	return item.ID, nil
}

// EnqueueBatch adds multiple items.
func (m *MemoryFrontier) EnqueueBatch(ctx context.Context, items []Item) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrClosed
	}

	now := time.Now().UTC()
	for _, item := range items {
		if item.URL == "" {
			continue
		}
		m.seq++
		item.ID = fmt.Sprintf("%d-0", m.seq)
		if item.EnqueuedAt.IsZero() {
			item.EnqueuedAt = now
		}
		m.items = append(m.items, item)
	}

	m.notify()
	return nil
}

// Dequeue returns up to count items, blocking up to block duration if queue is empty.
func (m *MemoryFrontier) Dequeue(ctx context.Context, consumer string, count int64, block time.Duration) ([]Item, error) {
	if count <= 0 {
		count = 10
	}

	deadline := time.Now().Add(block)

	for {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return nil, ErrClosed
		}

		if len(m.items) > 0 {
			n := int64(len(m.items))
			if n > count {
				n = count
			}

			result := make([]Item, n)
			copy(result, m.items[:n])
			m.items = m.items[n:]

			now := time.Now().UTC()
			for _, item := range result {
				m.pending[item.ID] = item
				m.pendingTimes[item.ID] = now
			}

			m.mu.Unlock()
			return result, nil
		}
		m.mu.Unlock()

		if block <= 0 {
			return nil, nil
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-m.notifyCh:
		case <-time.After(remaining):
			return nil, nil
		}
	}
}

// Ack removes pending items.
func (m *MemoryFrontier) Ack(ctx context.Context, messageIDs ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, id := range messageIDs {
		delete(m.pending, id)
		delete(m.pendingTimes, id)
	}
	return nil
}

// RouteToDLQ moves an item to the dead letter queue.
func (m *MemoryFrontier) RouteToDLQ(ctx context.Context, item Item, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.dlq = append(m.dlq, DLQItem{
		Item:       item,
		Reason:     reason,
		FailedAt:   time.Now().UTC(),
		OriginalID: item.ID,
	})

	delete(m.pending, item.ID)
	delete(m.pendingTimes, item.ID)
	return nil
}

// ClaimStale reclaims pending messages that have been in-flight longer than minIdle.
func (m *MemoryFrontier) ClaimStale(ctx context.Context, consumer string, minIdle time.Duration, count int64) ([]Item, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().UTC()
	var claimed []Item

	for id, item := range m.pending {
		if int64(len(claimed)) >= count {
			break
		}
		deqTime := m.pendingTimes[id]
		if now.Sub(deqTime) >= minIdle {
			claimed = append(claimed, item)
			// Reset the timer for reclaimed message
			m.pendingTimes[id] = now
		}
	}

	return claimed, nil
}

// Length returns the number of unconsumed items in the queue.
func (m *MemoryFrontier) Length(ctx context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(len(m.items)), nil
}

// DLQLength returns the number of items in the Dead Letter Queue.
func (m *MemoryFrontier) DLQLength(ctx context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(len(m.dlq)), nil
}

// Close marks the frontier as closed.
func (m *MemoryFrontier) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.notify()
	return nil
}
