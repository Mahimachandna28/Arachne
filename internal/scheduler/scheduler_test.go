package scheduler

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFIFOWithinHost(t *testing.T) {
	s := New(nil)
	s.Hold()
	for i := 0; i < 5; i++ {
		s.Push(Job{URL: fmt.Sprint(i), Host: "a"})
	}
	s.Release()
	for i := 0; i < 5; i++ {
		j, ok := s.Next(context.Background())
		if !ok || j.URL != fmt.Sprint(i) {
			t.Fatalf("step %d: got %+v ok=%v", i, j, ok)
		}
		s.Done(j)
	}
	select {
	case <-s.Wait():
	default:
		t.Fatal("scheduler should be done once every job is Done")
	}
	if _, ok := s.Next(context.Background()); ok {
		t.Fatal("Next after completion must return ok=false")
	}
}

func TestHoldPreventsEarlyCompletion(t *testing.T) {
	s := New(nil)
	s.Hold()
	got := make(chan bool, 1)
	go func() { _, ok := s.Next(context.Background()); got <- ok }()
	time.Sleep(20 * time.Millisecond)
	select {
	case <-got:
		t.Fatal("Next returned while a producer still held the scheduler")
	default:
	}
	s.Push(Job{URL: "x", Host: "h"})
	if ok := <-got; !ok {
		t.Fatal("expected job after push")
	}
}

func TestPolitenessInterval(t *testing.T) {
	s := New(func(string) time.Duration { return 50 * time.Millisecond })
	s.Hold()
	for i := 0; i < 3; i++ {
		s.Push(Job{URL: fmt.Sprint(i), Host: "slow"})
	}
	s.Release()
	start := time.Now()
	for i := 0; i < 3; i++ {
		j, _ := s.Next(context.Background())
		s.Done(j)
	}
	if el := time.Since(start); el < 100*time.Millisecond {
		t.Fatalf("3 requests with 50ms spacing finished in %v", el)
	}
}

func TestOtherHostsNotBlocked(t *testing.T) {
	s := New(func(h string) time.Duration {
		if h == "slow" {
			return time.Hour
		}
		return 0
	})
	s.Hold()
	s.Push(Job{URL: "s1", Host: "slow"})
	s.Push(Job{URL: "s2", Host: "slow"})
	for i := 0; i < 100; i++ {
		s.Push(Job{URL: fmt.Sprint("f", i), Host: "fast"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var fast int
	for fast < 100 {
		j, ok := s.Next(ctx)
		if !ok {
			t.Fatalf("stalled after %d fast jobs", fast)
		}
		if j.Host == "fast" {
			fast++
		}
		s.Done(j)
	}
}

func TestRetryBacksOffHost(t *testing.T) {
	s := New(nil)
	s.Hold()
	s.Push(Job{URL: "a", Host: "h"})
	s.Release()
	j, _ := s.Next(context.Background())
	start := time.Now()
	s.Retry(j, 60*time.Millisecond)
	j2, ok := s.Next(context.Background())
	if !ok || j2.URL != "a" || j2.Attempt != 1 {
		t.Fatalf("got %+v ok=%v", j2, ok)
	}
	if el := time.Since(start); el < 55*time.Millisecond {
		t.Fatalf("retry was not delayed: %v", el)
	}
	s.Done(j2)
	<-s.Wait()
}

func TestCancelUnblocksNext(t *testing.T) {
	s := New(nil)
	s.Hold()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Next(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Next did not return after cancel")
	}
}

// TestConcurrentProducersConsumers simulates a crawl: each job fans out into
// children until a fixed total is reached, with many workers in parallel.
func TestConcurrentProducersConsumers(t *testing.T) {
	const total = 20000
	s := New(nil)
	var produced, consumed atomic.Int64
	s.Hold()
	produced.Add(1)
	s.Push(Job{URL: "0", Host: "h0"})
	s.Release()

	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				j, ok := s.Next(context.Background())
				if !ok {
					return
				}
				consumed.Add(1)
				for c := 0; c < 5; c++ {
					if n := produced.Add(1); n <= total {
						s.Push(Job{URL: fmt.Sprint(n), Host: fmt.Sprint("h", n%37)})
					}
				}
				s.Done(j)
			}
		}()
	}
	wg.Wait()
	if consumed.Load() != total {
		t.Fatalf("consumed %d, want %d", consumed.Load(), total)
	}
}

func BenchmarkPushNext(b *testing.B) {
	s := New(nil)
	s.Hold()
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		s.Push(Job{URL: "u", Host: fmt.Sprint("h", i%1000)})
		j, _ := s.Next(ctx)
		s.Done(j)
	}
}
