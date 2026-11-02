package jobqueue

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Entry is a scheduled job: due at Due, then (when Every is set) again every Every.
type Entry struct {
	Message
	Due   time.Time
	Every time.Duration
}

// Store keeps scheduled entries (pgq.Store, memq.Store).
type Store interface {
	// Put adds an entry, or replaces the one with the same ID.
	Put(ctx context.Context, e Entry) error
	// Due answers up to limit entries due at now, oldest first.
	Due(ctx context.Context, now time.Time, limit int) ([]Entry, error)
	// Done removes e once it was sent, or, when next is not nil, moves it to next. It does nothing when the entry was
	// replaced meanwhile (its Due differs).
	Done(ctx context.Context, e Entry, next *time.Time) error
}

// Lease lets one holder at a time do something (the scheduler loop) for a while (pgq, natsq, dynamolease,
// firestorelease, memq).
type Lease interface {
	// Acquire takes the lease name for holder until ttl from now, or renews it when holder has it. It answers whether
	// holder has it now.
	Acquire(ctx context.Context, name, holder string, ttl time.Duration) (bool, error)
	// Release gives it up if holder has it.
	Release(ctx context.Context, name, holder string) error
}

// Scheduler moves due entries from Store to the queue. Only the holder of Lease does so; every worker runs the loop,
// so another one takes over within TTL when the holder dies.
type Scheduler struct {
	Store Store
	Lease Lease
	// Name of the lease (default "jobqueue-scheduler"); Holder is this process (required, e.g. host name and pid).
	Name   string
	Holder string
	// TTL of the lease (default 30s); Poll is how often due entries are looked for (default 1s).
	TTL  time.Duration
	Poll time.Duration
	// Logger (default slog.Default()).
	Logger *slog.Logger
}

func (s *Scheduler) defaults() {
	if s.Name == "" {
		s.Name = "jobqueue-scheduler"
	}
	if s.TTL <= 0 {
		s.TTL = 30 * time.Second
	}
	if s.Poll <= 0 {
		s.Poll = time.Second
	}
	if s.Logger == nil {
		s.Logger = slog.Default()
	}
}

// run is the loop: renew or take the lease; while held, send what is due.
func (s *Scheduler) run(ctx context.Context, q *Queue) {
	s.defaults()
	held := false
	lastRenew := time.Time{}
	defer func() {
		if held {
			rel, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			_ = s.Lease.Release(rel, s.Name, s.Holder)
			cancel()
		}
	}()
	for ctx.Err() == nil {
		if time.Since(lastRenew) >= s.TTL/3 {
			ok, err := s.Lease.Acquire(ctx, s.Name, s.Holder, s.TTL)
			if err != nil {
				if ctx.Err() == nil {
					s.Logger.Warn("jobqueue: scheduler lease", "error", err.Error())
				}
				ok = false
			}
			held = ok
			lastRenew = time.Now()
		}
		if held {
			if err := s.Tick(ctx, q); err != nil && ctx.Err() == nil {
				s.Logger.Warn("jobqueue: scheduler", "error", err.Error())
			}
		}
		sleep(ctx, s.Poll)
	}
}

// Tick sends the entries due now once (the loop calls it while it holds the lease; tests may call it directly).
func (s *Scheduler) Tick(ctx context.Context, q *Queue) error {
	s.defaults()
	now := q.now()
	for {
		due, err := s.Store.Due(ctx, now, 100)
		if err != nil {
			return err
		}
		for _, e := range due {
			m := e.Message
			if e.Every > 0 {
				m.ID = Occurrence(e.ID, e.Due) // each run of a repeat is a job of its own
			}
			if err := q.d.Send(ctx, m, 0); err != nil {
				return err
			}
			var next *time.Time
			if e.Every > 0 {
				n := e.Due.Add(e.Every)
				for !n.After(now) {
					n = n.Add(e.Every)
				}
				next = &n
			}
			if err := s.Store.Done(ctx, e, next); err != nil {
				return err
			}
		}
		if len(due) < 100 {
			return nil
		}
	}
}

// Occurrence is the id of one run of a repeating job: "<id>@<unix seconds>". Drivers and the Scheduler give each run
// its own id, so de-duplication and a DoneLog treat runs apart.
func Occurrence(id string, at time.Time) string { return fmt.Sprintf("%s@%d", id, at.Unix()) }
