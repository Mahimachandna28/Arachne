package frontier

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestFrontier_NewItem(t *testing.T) {
	tests := []struct {
		name       string
		rawURL     string
		depth      int
		wantDomain string
		wantErr    bool
	}{
		{
			name:       "valid http url",
			rawURL:     "http://example.com/path",
			depth:      1,
			wantDomain: "example.com",
			wantErr:    false,
		},
		{
			name:       "valid https with uppercase domain and port",
			rawURL:     "https://EXAMPLE.ORG:8080/deep/link",
			depth:      3,
			wantDomain: "example.org:8080",
			wantErr:    false,
		},
		{
			name:       "empty url",
			rawURL:     "   ",
			depth:      0,
			wantDomain: "",
			wantErr:    true,
		},
		{
			name:       "no host",
			rawURL:     "/relative/path",
			depth:      1,
			wantDomain: "",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item, err := NewItem(tt.rawURL, tt.depth)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if item.Domain != tt.wantDomain {
				t.Errorf("Domain mismatch: got %q, want %q", item.Domain, tt.wantDomain)
			}
			if item.Depth != tt.depth {
				t.Errorf("Depth mismatch: got %d, want %d", item.Depth, tt.depth)
			}
			if item.EnqueuedAt.IsZero() {
				t.Errorf("EnqueuedAt should not be zero")
			}
		})
	}
}

func TestFrontier_Keys(t *testing.T) {
	jobID := "a1b2c3d4-e5f6-7890"

	qKey := QueueStreamKey(jobID)
	if qKey != "frontier:queue:a1b2c3d4-e5f6-7890" {
		t.Errorf("unexpected QueueStreamKey: %q", qKey)
	}

	dlqKey := DLQStreamKey(jobID)
	if dlqKey != "frontier:dlq:a1b2c3d4-e5f6-7890" {
		t.Errorf("unexpected DLQStreamKey: %q", dlqKey)
	}

	group := ConsumerGroupName(jobID)
	if group != "frontier:workers:a1b2c3d4-e5f6-7890" {
		t.Errorf("unexpected ConsumerGroupName: %q", group)
	}

	// Test default fallback
	if QueueStreamKey("") != "frontier:queue:default" {
		t.Errorf("unexpected fallback for empty jobID")
	}
}

func TestFrontier_Serialization(t *testing.T) {
	item, err := NewItem("https://example.com/test", 2)
	if err != nil {
		t.Fatalf("failed to create item: %v", err)
	}
	item.ID = "1698064800000-0"
	item.Retries = 1

	data, err := MarshalItem(item)
	if err != nil {
		t.Fatalf("failed to marshal item: %v", err)
	}

	decoded, err := UnmarshalItem(data)
	if err != nil {
		t.Fatalf("failed to unmarshal item: %v", err)
	}

	if decoded.URL != item.URL {
		t.Errorf("URL mismatch: got %q, want %q", decoded.URL, item.URL)
	}
	if decoded.Domain != item.Domain {
		t.Errorf("Domain mismatch: got %q, want %q", decoded.Domain, item.Domain)
	}
	if decoded.Depth != item.Depth {
		t.Errorf("Depth mismatch: got %d, want %d", decoded.Depth, item.Depth)
	}
	if decoded.Retries != item.Retries {
		t.Errorf("Retries mismatch: got %d, want %d", decoded.Retries, item.Retries)
	}
}

func TestMemoryFrontier_EnqueueDequeueAck(t *testing.T) {
	ctx := context.Background()
	f := NewMemoryFrontier()
	defer f.Close()

	// Initially empty
	l, err := f.Length(ctx)
	if err != nil || l != 0 {
		t.Fatalf("expected length 0, got %d, err %v", l, err)
	}

	// Enqueue single item
	item1, _ := NewItem("https://example.com/page1", 0)
	id1, err := f.Enqueue(ctx, item1)
	if err != nil || id1 == "" {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Enqueue batch
	item2, _ := NewItem("https://example.com/page2", 1)
	item3, _ := NewItem("https://example.com/page3", 1)
	err = f.EnqueueBatch(ctx, []Item{item2, item3})
	if err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	l, _ = f.Length(ctx)
	if l != 3 {
		t.Fatalf("expected length 3, got %d", l)
	}

	// Dequeue 2 items
	items, err := f.Dequeue(ctx, "worker-1", 2, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	if items[0].URL != "https://example.com/page1" {
		t.Errorf("unexpected first item: %s", items[0].URL)
	}

	// Ack first item
	err = f.Ack(ctx, items[0].ID)
	if err != nil {
		t.Fatalf("failed to ack: %v", err)
	}

	// Length should be 1 remaining
	l, _ = f.Length(ctx)
	if l != 1 {
		t.Fatalf("expected 1 remaining, got %d", l)
	}

	// Dequeue the remaining item
	items2, err := f.Dequeue(ctx, "worker-2", 5, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to dequeue remaining: %v", err)
	}
	if len(items2) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items2))
	}
	if items2[0].URL != "https://example.com/page3" {
		t.Errorf("unexpected item: %s", items2[0].URL)
	}
}

func TestMemoryFrontier_RouteToDLQ(t *testing.T) {
	ctx := context.Background()
	f := NewMemoryFrontier()
	defer f.Close()

	item, _ := NewItem("https://malformed-or-poison.com", 2)
	_, _ = f.Enqueue(ctx, item)

	deq, err := f.Dequeue(ctx, "worker-1", 1, 100*time.Millisecond)
	if err != nil || len(deq) == 0 {
		t.Fatalf("failed to dequeue: %v", err)
	}

	err = f.RouteToDLQ(ctx, deq[0], "HTTP 500 max retries exceeded")
	if err != nil {
		t.Fatalf("failed to route to DLQ: %v", err)
	}

	dlqLen, err := f.DLQLength(ctx)
	if err != nil || dlqLen != 1 {
		t.Fatalf("expected DLQ length 1, got %d, err %v", dlqLen, err)
	}
}

func TestMemoryFrontier_ClaimStale(t *testing.T) {
	ctx := context.Background()
	f := NewMemoryFrontier()
	defer f.Close()

	item, _ := NewItem("https://example.com/crash", 1)
	_, _ = f.Enqueue(ctx, item)

	deq, _ := f.Dequeue(ctx, "crashed-worker", 1, 50*time.Millisecond)
	if len(deq) != 1 {
		t.Fatalf("expected 1 dequeued item")
	}

	// Before minIdle, no stale items should be claimed
	claimed, err := f.ClaimStale(ctx, "janitor", 1*time.Second, 10)
	if err != nil || len(claimed) != 0 {
		t.Fatalf("expected 0 stale items, got %d", len(claimed))
	}

	// Sleep tiny bit and check with zero or very low minIdle
	time.Sleep(10 * time.Millisecond)
	claimed, err = f.ClaimStale(ctx, "janitor", 5*time.Millisecond, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("expected 1 claimed stale item, got %d, err: %v", len(claimed), err)
	}
	if claimed[0].URL != "https://example.com/crash" {
		t.Errorf("claimed item mismatch: %s", claimed[0].URL)
	}
}

func TestMemoryFrontier_ConcurrentWorkers(t *testing.T) {
	ctx := context.Background()
	f := NewMemoryFrontier()
	defer f.Close()

	numItems := 100
	for i := 0; i < numItems; i++ {
		item, _ := NewItem(fmt.Sprintf("https://example.com/page/%d", i), 1)
		_, _ = f.Enqueue(ctx, item)
	}

	var wg sync.WaitGroup
	var countMu sync.Mutex
	consumedCount := 0

	numWorkers := 5
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			consumer := fmt.Sprintf("worker-%d", workerID)
			for {
				items, err := f.Dequeue(ctx, consumer, 5, 20*time.Millisecond)
				if err != nil || len(items) == 0 {
					return
				}
				for _, it := range items {
					_ = f.Ack(ctx, it.ID)
					countMu.Lock()
					consumedCount++
					countMu.Unlock()
				}
			}
		}(w)
	}

	wg.Wait()

	if consumedCount != numItems {
		t.Errorf("expected %d consumed items, got %d", numItems, consumedCount)
	}
}

func TestRedisFrontier_ParseMessage(t *testing.T) {
	// Test parseMessage with standard JSON payload
	rawJSON := `{"url":"https://example.com/json","depth":2,"domain":"example.com","retries":1}`
	msgJSON := redis.XMessage{
		ID: "1698064800000-0",
		Values: map[string]interface{}{
			"payload": rawJSON,
			"url":     "https://example.com/json",
			"domain":  "example.com",
		},
	}

	parsed, err := parseMessage(msgJSON)
	if err != nil {
		t.Fatalf("failed to parse message: %v", err)
	}
	if parsed.URL != "https://example.com/json" {
		t.Errorf("URL mismatch: got %q", parsed.URL)
	}
	if parsed.Depth != 2 {
		t.Errorf("Depth mismatch: got %d", parsed.Depth)
	}
	if parsed.ID != "1698064800000-0" {
		t.Errorf("ID mismatch: got %q", parsed.ID)
	}

	// Test fallback when payload is missing
	msgFallback := redis.XMessage{
		ID: "1698064800001-0",
		Values: map[string]interface{}{
			"url":    "https://example.com/fallback",
			"domain": "example.com",
		},
	}
	parsedFallback, err := parseMessage(msgFallback)
	if err != nil {
		t.Fatalf("failed to parse fallback message: %v", err)
	}
	if parsedFallback.URL != "https://example.com/fallback" {
		t.Errorf("Fallback URL mismatch: got %q", parsedFallback.URL)
	}
}
