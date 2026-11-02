// Package jobqueue runs background jobs the same way on a laptop, on AWS and on Google Cloud.
//
// An app publishes jobs (a kind, an id and a JSON payload) to a [Queue]; workers run the handler registered for
// each kind and acknowledge the job only after the handler returned without an error. A failing job is retried with
// back-off and, after the last attempt, kept in a dead-letter destination. Jobs may be delayed or repeated.
//
// The broker is a [Driver], one package per platform, so an app links only the SDK it uses:
//
//   - natsq: NATS JetStream (any host; delays and repeats are native)
//   - kafkaq: Kafka, MSK, Confluent, Redpanda (acknowledgement and retries emulated)
//   - rabbitq: RabbitMQ, Amazon MQ, CloudAMQP (delays through wait queues)
//   - redisq: Redis / Valkey Streams (delays through a sorted set)
//   - memq: in memory (tests, single-process apps)
//   - sqsq: AWS SQS, standard or FIFO (delays up to 15 minutes are native)
//   - pubsubq: Google Cloud Pub/Sub (no native delays)
//
// Jobs are unordered unless they carry a Key: jobs of a kind with the same key run one at a time in publish order.
// PublishBatch and WorkOptions.BatchSize move jobs in batches; each is still acknowledged on its own.
//
// Delays and repeats a driver cannot hold go through a [Scheduler]: a loop, run by one worker at a time (a [Lease]:
// pgq, natsq, dynamolease, firestorelease, memq), that moves due entries from a [Store] (pgq, memq) to the queue.
//
// Delivery is at least once. Make handlers idempotent; [Once] with a [DoneLog] (pgq, memq) helps.
package jobqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

// Unlimited is a Driver's MaxDelay when it holds any delay itself.
const Unlimited = time.Duration(1<<63 - 1)

// Message is a job as it travels through a broker.
type Message struct {
	// ID identifies the job within its kind. Brokers that de-duplicate (NATS) drop a second publish of the same kind and id within their
	// window; a DoneLog keyed by it makes the effect happen once everywhere.
	ID string `json:"id"`
	// Kind selects the handler, e.g. "email.send". Letters, digits, '.', '_' and '-'.
	Kind string `json:"kind"`
	// Key, when set, orders the job: jobs of one kind with the same key run one at a time, in publish order (see
	// "Ordering" in the README for what each driver guarantees). Empty: no order, the fastest.
	Key string `json:"key,omitempty"`
	// Payload is the job's JSON.
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Job is a message being handled.
type Job struct {
	Message
	// Attempt is 1 on the first delivery.
	Attempt int
}

// Decode unmarshals the payload into v.
func (j Job) Decode(v any) error { return json.Unmarshal(j.Payload, v) }

// Handler runs one job. Return nil when done: the job is acknowledged. An error retries it (after a back-off), or
// sends it to the dead letters when it was the last attempt or the error is [Permanent]. ctx ends when the worker
// must stop (after its stop grace).
type Handler func(ctx context.Context, job Job) error

// Driver is a broker adapter. The Queue adds retries, back-off, dead letters, scheduling and graceful stop on top.
type Driver interface {
	// Send puts m on its kind's queue, to be delivered no sooner than delay from now (delay <= MaxDelay).
	Send(ctx context.Context, m Message, delay time.Duration) error
	// MaxDelay is the longest delay Send holds natively: 0 for none, Unlimited for any.
	MaxDelay() time.Duration
	// Receive waits a short while (a poll) for the next message of kind. It answers nil, nil when none came.
	Receive(ctx context.Context, kind string) (Delivery, error)
	// DeadLetter keeps a message that failed for good, with the last error.
	DeadLetter(ctx context.Context, m Message, attempts int, cause error) error
	// Close releases the driver's connections (not the client the caller gave it).
	Close() error
}

// Delivery is one received message, owned by the worker until it is acknowledged or released.
type Delivery interface {
	Message() Message
	// Attempt is 1 on the first delivery (best effort on brokers that do not count).
	Attempt() int
	// Ack removes the message for good.
	Ack(ctx context.Context) error
	// Retry releases the message to be delivered again after about delay (the driver clamps it to what it can do).
	Retry(ctx context.Context, delay time.Duration) error
	// Extend tells the broker the job is still running (keeps it from being redelivered meanwhile).
	Extend(ctx context.Context) error
}

// Repeater is a Driver that repeats a message natively (NATS schedules). Sending the same id again replaces it.
type Repeater interface {
	SendEvery(ctx context.Context, m Message, every time.Duration) error
}

// BatchSender is a Driver that sends many undelayed messages in few round trips (SQS batches of 10, a NATS async
// publish window, Kafka and Pub/Sub batches, a Redis pipeline).
type BatchSender interface {
	SendBatch(ctx context.Context, ms []Message) error
}

// BatchReceiver is a Driver that fetches up to max messages of kind in one call. Each is still acknowledged on its own.
type BatchReceiver interface {
	ReceiveBatch(ctx context.Context, kind string, max int) ([]Delivery, error)
}

// Orderer is a Driver that keeps jobs with the same Key in order across all workers by itself. For other drivers the
// Queue runs keyed jobs one at a time per key within each process only.
type Orderer interface {
	OrdersByKey() bool
}

// ErrNoScheduler is returned for a delay or repeat the driver cannot hold when the Queue has no Scheduler.
var ErrNoScheduler = errors.New("jobqueue: this driver cannot hold the delay or repeat, and no Scheduler is set")

// ErrInvalidKind is returned for a kind with characters other than letters, digits, '.', '_' and '-'.
var ErrInvalidKind = errors.New("jobqueue: invalid kind")

var kindRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$`)

// ValidKind reports whether kind may be used.
func ValidKind(kind string) bool { return kindRE.MatchString(kind) }

// permanent marks an error that must not be retried.
type permanent struct{ err error }

func (p permanent) Error() string { return p.err.Error() }
func (p permanent) Unwrap() error { return p.err }

// Permanent wraps err so the job goes to the dead letters at once, without retries.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanent{err}
}

// IsPermanent reports whether err was wrapped by Permanent.
func IsPermanent(err error) bool {
	var p permanent
	return errors.As(err, &p)
}

// PublishOption changes one Publish.
type PublishOption func(*publishOptions)

type publishOptions struct {
	key   string
	after time.Duration
	at    time.Time
	every time.Duration
}

// Key orders the job among the jobs of its kind with the same key (see Message.Key).
func Key(key string) PublishOption { return func(o *publishOptions) { o.key = key } }

// After delays the job by d.
func After(d time.Duration) PublishOption { return func(o *publishOptions) { o.after = d } }

// At delays the job until t.
func At(t time.Time) PublishOption { return func(o *publishOptions) { o.at = t } }

// Every repeats the job every d (at least one second), first after d. Publishing the same id again replaces it. Each
// run reaches the handler with its own id, Occurrence(id, time).
func Every(d time.Duration) PublishOption { return func(o *publishOptions) { o.every = d } }

// Queue publishes jobs and runs workers over a Driver.
type Queue struct {
	d     Driver
	sched *Scheduler
	now   func() time.Time
}

// Option configures a Queue.
type Option func(*Queue)

// WithScheduler holds delays and repeats the driver cannot (see Scheduler). Its loop runs inside Work.
func WithScheduler(s *Scheduler) Option { return func(q *Queue) { q.sched = s } }

// WithClock sets the clock used for At and the scheduler (tests).
func WithClock(now func() time.Time) Option { return func(q *Queue) { q.now = now } }

// New builds a Queue over a driver.
func New(d Driver, opts ...Option) *Queue {
	q := &Queue{d: d, now: time.Now}
	for _, o := range opts {
		o(q)
	}
	return q
}

// Driver is the queue's driver.
func (q *Queue) Driver() Driver { return q.d }

// Publish sends a job of kind with the given id; payload is marshalled to JSON (a json.RawMessage is used as it is).
func (q *Queue) Publish(ctx context.Context, kind, id string, payload any, opts ...PublishOption) error {
	if !ValidKind(kind) {
		return fmt.Errorf("%w: %q", ErrInvalidKind, kind)
	}
	if id == "" {
		return errors.New("jobqueue: empty job id")
	}
	var raw json.RawMessage
	switch p := payload.(type) {
	case nil:
	case json.RawMessage:
		raw = p
	default:
		b, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("jobqueue: payload: %w", err)
		}
		raw = b
	}
	var o publishOptions
	for _, opt := range opts {
		opt(&o)
	}
	m := Message{ID: id, Kind: kind, Key: o.key, Payload: raw}
	if o.every > 0 {
		if o.every < time.Second {
			return errors.New("jobqueue: Every must be at least one second")
		}
		if r, ok := q.d.(Repeater); ok {
			return r.SendEvery(ctx, m, o.every)
		}
		if q.sched == nil {
			return ErrNoScheduler
		}
		return q.sched.Store.Put(ctx, Entry{Message: m, Due: q.now().Add(o.every), Every: o.every})
	}
	delay := o.after
	if !o.at.IsZero() {
		delay = o.at.Sub(q.now())
	}
	if delay < 0 {
		delay = 0
	}
	if delay <= q.d.MaxDelay() {
		return q.d.Send(ctx, m, delay)
	}
	if q.sched == nil {
		return ErrNoScheduler
	}
	return q.sched.Store.Put(ctx, Entry{Message: m, Due: q.now().Add(delay)})
}

// Pub is one job of PublishBatch.
type Pub struct {
	Kind, ID, Key string
	Payload       any
}

func marshal(payload any) (json.RawMessage, error) {
	switch p := payload.(type) {
	case nil:
		return nil, nil
	case json.RawMessage:
		return p, nil
	default:
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("jobqueue: payload: %w", err)
		}
		return b, nil
	}
}

// PublishBatch sends undelayed jobs in as few round trips as the driver allows (a BatchSender), else one by one.
func (q *Queue) PublishBatch(ctx context.Context, jobs []Pub) error {
	ms := make([]Message, 0, len(jobs))
	for _, j := range jobs {
		if !ValidKind(j.Kind) {
			return fmt.Errorf("%w: %q", ErrInvalidKind, j.Kind)
		}
		if j.ID == "" {
			return errors.New("jobqueue: empty job id")
		}
		raw, err := marshal(j.Payload)
		if err != nil {
			return err
		}
		ms = append(ms, Message{ID: j.ID, Kind: j.Kind, Key: j.Key, Payload: raw})
	}
	if b, ok := q.d.(BatchSender); ok {
		return b.SendBatch(ctx, ms)
	}
	for _, m := range ms {
		if err := q.d.Send(ctx, m, 0); err != nil {
			return err
		}
	}
	return nil
}

// Close closes the driver.
func (q *Queue) Close() error { return q.d.Close() }
