// Package memq is jobqueue in memory: a Driver, a scheduler Store and Lease, and a DoneLog. For tests and for an app
// that runs everything in one process. Nothing survives a restart.
package memq

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/ez-technologies/jobqueue"
)

// Dead is a message that failed for good.
type Dead struct {
	Message  jobqueue.Message
	Attempts int
	Error    string
}

type item struct {
	m       jobqueue.Message
	attempt int
}

// Driver is an in-memory broker. It holds any delay and repeats natively.
type Driver struct {
	mu      sync.Mutex
	ready   map[string][]item
	wake    chan struct{}
	timers  map[*time.Timer]struct{}
	repeats map[string]*time.Ticker
	dead    []Dead
	closed  bool
	poll    time.Duration
}

// New returns an empty driver. Receive waits up to poll (default 100 ms) for a message.
func New(poll ...time.Duration) *Driver {
	d := &Driver{ready: map[string][]item{}, wake: make(chan struct{}), timers: map[*time.Timer]struct{}{},
		repeats: map[string]*time.Ticker{}, poll: 100 * time.Millisecond}
	if len(poll) > 0 && poll[0] > 0 {
		d.poll = poll[0]
	}
	return d
}

var _ jobqueue.Driver = (*Driver)(nil)
var _ jobqueue.Repeater = (*Driver)(nil)

// MaxDelay is Unlimited.
func (d *Driver) MaxDelay() time.Duration { return jobqueue.Unlimited }

func (d *Driver) push(it item) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return
	}
	d.ready[it.m.Kind] = append(d.ready[it.m.Kind], it)
	close(d.wake)
	d.wake = make(chan struct{})
}

func (d *Driver) later(delay time.Duration, it item) {
	if delay <= 0 {
		d.push(it)
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	var t *time.Timer
	t = time.AfterFunc(delay, func() {
		d.mu.Lock()
		delete(d.timers, t)
		d.mu.Unlock()
		d.push(it)
	})
	d.timers[t] = struct{}{}
}

// Send queues m after delay.
func (d *Driver) Send(_ context.Context, m jobqueue.Message, delay time.Duration) error {
	d.later(delay, item{m: m, attempt: 1})
	return nil
}

// SendEvery queues m every interval, replacing an earlier repeat with the same id.
func (d *Driver) SendEvery(_ context.Context, m jobqueue.Message, every time.Duration) error {
	d.mu.Lock()
	if old, ok := d.repeats[m.ID]; ok {
		old.Stop()
	}
	t := time.NewTicker(every)
	d.repeats[m.ID] = t
	d.mu.Unlock()
	go func() {
		for range t.C {
			d.mu.Lock()
			current := d.repeats[m.ID] == t && !d.closed
			d.mu.Unlock()
			if !current {
				return
			}
			run := m
			run.ID = jobqueue.Occurrence(m.ID, time.Now())
			d.push(item{m: run, attempt: 1})
		}
	}()
	return nil
}

// Receive takes the next message of kind, waiting up to the poll time.
func (d *Driver) Receive(ctx context.Context, kind string) (jobqueue.Delivery, error) {
	deadline := time.NewTimer(d.poll)
	defer deadline.Stop()
	for {
		d.mu.Lock()
		if q := d.ready[kind]; len(q) > 0 {
			it := q[0]
			d.ready[kind] = q[1:]
			d.mu.Unlock()
			return &delivery{d: d, it: it}, nil
		}
		wake := d.wake
		d.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, nil
		case <-deadline.C:
			return nil, nil
		case <-wake:
		}
	}
}

// DeadLetter keeps m (see Dead).
func (d *Driver) DeadLetter(_ context.Context, m jobqueue.Message, attempts int, cause error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	d.dead = append(d.dead, Dead{Message: m, Attempts: attempts, Error: msg})
	return nil
}

// DeadLetters answers what went to the dead letters so far.
func (d *Driver) DeadLetters() []Dead {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]Dead(nil), d.dead...)
}

// Pending is how many messages of kind wait to be received now.
func (d *Driver) Pending(kind string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.ready[kind])
}

// Close stops timers and repeats.
func (d *Driver) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	for t := range d.timers {
		t.Stop()
	}
	for _, t := range d.repeats {
		t.Stop()
	}
	return nil
}

type delivery struct {
	d  *Driver
	it item
}

func (x *delivery) Message() jobqueue.Message    { return x.it.m }
func (x *delivery) Attempt() int                 { return x.it.attempt }
func (x *delivery) Ack(context.Context) error    { return nil }
func (x *delivery) Extend(context.Context) error { return nil }
func (x *delivery) Retry(_ context.Context, delay time.Duration) error {
	x.d.later(delay, item{m: x.it.m, attempt: x.it.attempt + 1})
	return nil
}

// Store is an in-memory scheduler store.
type Store struct {
	mu      sync.Mutex
	entries map[string]jobqueue.Entry
}

// NewStore returns an empty store.
func NewStore() *Store { return &Store{entries: map[string]jobqueue.Entry{}} }

var _ jobqueue.Store = (*Store)(nil)

func (s *Store) Put(_ context.Context, e jobqueue.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[e.ID] = e
	return nil
}

func (s *Store) Due(_ context.Context, now time.Time, limit int) ([]jobqueue.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []jobqueue.Entry
	for _, e := range s.entries {
		if !e.Due.After(now) {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Due.Before(out[j].Due) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) Done(_ context.Context, e jobqueue.Entry, next *time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.entries[e.ID]
	if !ok || !cur.Due.Equal(e.Due) {
		return nil
	}
	if next == nil {
		delete(s.entries, e.ID)
		return nil
	}
	cur.Due = *next
	s.entries[e.ID] = cur
	return nil
}

// Len is the number of entries.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

// Lease is an in-memory lease (for one process; tests).
type Lease struct {
	mu   sync.Mutex
	held map[string]lease
	Now  func() time.Time
}

type lease struct {
	holder  string
	expires time.Time
}

// NewLease returns a lease table.
func NewLease() *Lease { return &Lease{held: map[string]lease{}, Now: time.Now} }

var _ jobqueue.Lease = (*Lease)(nil)

func (l *Lease) Acquire(_ context.Context, name, holder string, ttl time.Duration) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.Now()
	cur, ok := l.held[name]
	if ok && cur.holder != holder && cur.expires.After(now) {
		return false, nil
	}
	l.held[name] = lease{holder: holder, expires: now.Add(ttl)}
	return true, nil
}

func (l *Lease) Release(_ context.Context, name, holder string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if cur, ok := l.held[name]; ok && cur.holder == holder {
		delete(l.held, name)
	}
	return nil
}

// DoneLog is an in-memory done-log.
type DoneLog struct {
	mu   sync.Mutex
	keys map[string]struct{}
}

// NewDoneLog returns an empty log.
func NewDoneLog() *DoneLog { return &DoneLog{keys: map[string]struct{}{}} }

var _ jobqueue.DoneLog = (*DoneLog)(nil)

func (l *DoneLog) Claim(_ context.Context, key string) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.keys[key]; ok {
		return false, nil
	}
	l.keys[key] = struct{}{}
	return true, nil
}

func (l *DoneLog) Forget(_ context.Context, key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.keys, key)
	return nil
}
