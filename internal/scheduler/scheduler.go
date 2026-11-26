// Package scheduler is the in-process URL frontier. It replaces the old
// bounded job channel with a Mercator-style design:
//
//	       Push(job)                         Next(ctx)
//	           │                                 ▲
//	           ▼                                 │ pop host whose
//	┌─────────────────────┐   ready heap   ┌─────┴──────┐ nextAllowed <= now
//	│ host A: [u1 u2 u3]  │──────────────▶│ min-heap by │
//	│ host B: [u4]        │               │ nextAllowed │
//	│ host C: [u5 u6]     │               └────────────┘
//	└─────────────────────┘
//
// Properties that matter for a crawler:
//
//   - Push never blocks. The old design let workers block while sending
//     discovered links into the same bounded channel they consumed from,
//     which deadlocked once the frontier outgrew the buffer.
//   - Politeness lives in the scheduler, not the worker. A worker is only
//     handed a URL whose host is allowed to be contacted now, so no worker
//     sleeps on a rate limiter while other hosts have ready work
//     (no head-of-line blocking).
//   - Termination is exact: the crawl is finished when no job is queued,
//     in flight, or held by a producer (seeding, sitemap discovery).
package scheduler

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

// Job is one unit of crawl work.
type Job struct {
	URL     string
	Host    string
	Depth   int
	Attempt int // 0 for the first try, incremented on each retry
}

// IntervalFunc returns the minimum spacing between request starts to host.
type IntervalFunc func(host string) time.Duration

// Stats is a point-in-time view of the frontier.
type Stats struct {
	Queued   int // jobs waiting in host queues
	InFlight int // jobs handed out and not yet Done/Retry'd
	Hosts    int // hosts with at least one queued job
}

type hostQueue struct {
	host        string
	jobs        []Job // FIFO; jobs[head:] are live
	head        int
	nextAllowed time.Time
	heapIndex   int // -1 when not in the ready heap
}

func (q *hostQueue) len() int { return len(q.jobs) - q.head }

func (q *hostQueue) pop() Job {
	j := q.jobs[q.head]
	q.jobs[q.head] = Job{}
	q.head++
	if q.head > 64 && q.head*2 >= len(q.jobs) { // compact
		q.jobs = append(q.jobs[:0], q.jobs[q.head:]...)
		q.head = 0
	}
	return j
}

// Scheduler is safe for concurrent use.
type Scheduler struct {
	mu       sync.Mutex
	hosts    map[string]*hostQueue
	ready    readyHeap
	interval IntervalFunc
	now      func() time.Time

	queued   int
	inFlight int
	holds    int

	changed chan struct{} // closed and replaced on every state change (broadcast)
	done    chan struct{}
	closed  bool
}

// New returns an empty scheduler. interval may be nil (no politeness delay).
func New(interval IntervalFunc) *Scheduler {
	if interval == nil {
		interval = func(string) time.Duration { return 0 }
	}
	return &Scheduler{
		hosts:    make(map[string]*hostQueue),
		interval: interval,
		now:      time.Now,
		changed:  make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Hold registers a producer that may still Push work (e.g. the seeding loop
// or an async sitemap fetch). The crawl cannot finish while any hold is
// outstanding. Each Hold must be matched by exactly one Release.
func (s *Scheduler) Hold() {
	s.mu.Lock()
	s.holds++
	s.mu.Unlock()
}

// Release ends a Hold.
func (s *Scheduler) Release() {
	s.mu.Lock()
	s.holds--
	s.checkDoneLocked()
	s.broadcastLocked()
	s.mu.Unlock()
}

// Push enqueues a job. It never blocks.
func (s *Scheduler) Push(j Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.pushLocked(j, false)
	s.broadcastLocked()
}

func (s *Scheduler) pushLocked(j Job, front bool) {
	q, ok := s.hosts[j.Host]
	if !ok {
		q = &hostQueue{host: j.Host, heapIndex: -1}
		s.hosts[j.Host] = q
	}
	if front && q.head > 0 {
		q.head--
		q.jobs[q.head] = j
	} else if front {
		q.jobs = append([]Job{j}, q.jobs[q.head:]...)
		q.head = 0
	} else {
		q.jobs = append(q.jobs, j)
	}
	s.queued++
	if q.heapIndex < 0 {
		heap.Push(&s.ready, q)
	}
}

// Next blocks until a job is ready to be fetched, the crawl is complete, or
// ctx is cancelled. ok is false in the latter two cases.
func (s *Scheduler) Next(ctx context.Context) (job Job, ok bool) {
	s.mu.Lock()
	for {
		if s.closed || ctx.Err() != nil {
			s.mu.Unlock()
			return Job{}, false
		}

		var wait time.Duration = -1
		if s.ready.Len() > 0 {
			q := s.ready[0]
			now := s.now()
			if !now.Before(q.nextAllowed) {
				j := q.pop()
				s.queued--
				s.inFlight++
				q.nextAllowed = now.Add(s.interval(q.host))
				if q.len() == 0 {
					heap.Remove(&s.ready, q.heapIndex)
					q.jobs, q.head = nil, 0
				} else {
					heap.Fix(&s.ready, q.heapIndex)
				}
				s.mu.Unlock()
				return j, true
			}
			wait = q.nextAllowed.Sub(now)
		}

		changed := s.changed
		s.mu.Unlock()

		var timer <-chan time.Time
		if wait >= 0 {
			t := time.NewTimer(wait)
			timer = t.C
			select {
			case <-ctx.Done():
			case <-changed:
			case <-s.done:
			case <-timer:
			}
			t.Stop()
		} else {
			select {
			case <-ctx.Done():
			case <-changed:
			case <-s.done:
			}
		}
		s.mu.Lock()
	}
}

// Done marks an in-flight job as finished. Any children must be Pushed
// before calling Done so the crawl cannot be observed as complete early.
func (s *Scheduler) Done(Job) {
	s.mu.Lock()
	s.inFlight--
	s.checkDoneLocked()
	s.broadcastLocked()
	s.mu.Unlock()
}

// Retry returns an in-flight job to the front of its host queue and pushes
// the host's next allowed contact time back by at least delay. Backing off
// the whole host (not just the URL) is deliberate: 5xx and 429 responses are
// a signal that the server is struggling.
func (s *Scheduler) Retry(j Job, delay time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inFlight--
	if s.closed {
		s.checkDoneLocked()
		return
	}
	j.Attempt++
	s.pushLocked(j, true)
	q := s.hosts[j.Host]
	if t := s.now().Add(delay); t.After(q.nextAllowed) {
		q.nextAllowed = t
		heap.Fix(&s.ready, q.heapIndex)
	}
	s.broadcastLocked()
}

// Wait returns a channel closed when the crawl is complete.
func (s *Scheduler) Wait() <-chan struct{} { return s.done }

// Stats returns current frontier counters.
func (s *Scheduler) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{Queued: s.queued, InFlight: s.inFlight, Hosts: s.ready.Len()}
}

func (s *Scheduler) checkDoneLocked() {
	if !s.closed && s.queued == 0 && s.inFlight == 0 && s.holds == 0 {
		s.closed = true
		close(s.done)
	}
}

func (s *Scheduler) broadcastLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

// readyHeap orders hosts by the earliest time they may be contacted.
type readyHeap []*hostQueue

func (h readyHeap) Len() int           { return len(h) }
func (h readyHeap) Less(i, j int) bool { return h[i].nextAllowed.Before(h[j].nextAllowed) }
func (h readyHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}
func (h *readyHeap) Push(x any) {
	q := x.(*hostQueue)
	q.heapIndex = len(*h)
	*h = append(*h, q)
}
func (h *readyHeap) Pop() any {
	old := *h
	n := len(old)
	q := old[n-1]
	old[n-1] = nil
	q.heapIndex = -1
	*h = old[:n-1]
	return q
}
