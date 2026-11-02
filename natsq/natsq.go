// Package natsq is the NATS JetStream adapter of jobqueue: a Driver (delays and repeats held by the server's message
// schedules, NATS 2.12 or later) and a Lease on a JetStream key-value bucket.
//
// Layout: one work-queue stream (default "JOBS") with subjects <prefix>.run.<kind> (jobs) and <prefix>.sched.<id>
// (schedules), one durable consumer per kind shared by every worker, and a stream "<stream>_DEAD" with subjects
// <prefix>.dead.<kind>.
package natsq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ez-technologies/jobqueue"
)

// Config of the driver. The zero value works.
type Config struct {
	// Prefix of the subjects (default "jobs"); Stream names the stream (default the prefix in upper case).
	Prefix string
	Stream string
	// Poll is how long Receive waits for a message (default 2s).
	Poll time.Duration
	// AckWait is how long a running job stays hidden from other workers without an Extend (default 60s).
	AckWait time.Duration
	// Replicas of the streams (default 1; 3 on a cluster).
	Replicas int
	// MaxAge drops jobs nobody took for this long (default 7 days); DeadMaxAge, dead letters (default 30 days).
	MaxAge     time.Duration
	DeadMaxAge time.Duration
	// Duplicates is the window in which a second publish of the same job id is dropped (default 10 minutes).
	Duplicates time.Duration
	// Storage is file (default) or memory.
	Storage jetstream.StorageType
	// KeyShards is how many ordered lanes each kind has for keyed jobs (default 4). A key always goes to the same
	// lane, and a lane hands out one job at a time across all workers (a consumer with MaxAckPending 1).
	KeyShards int
}

// Driver is the JetStream driver.
type Driver struct {
	js  jetstream.JetStream
	cfg Config

	mu        sync.Mutex
	consumers map[string]jetstream.Consumer
}

var (
	_ jobqueue.Driver        = (*Driver)(nil)
	_ jobqueue.Repeater      = (*Driver)(nil)
	_ jobqueue.BatchSender   = (*Driver)(nil)
	_ jobqueue.BatchReceiver = (*Driver)(nil)
	_ jobqueue.Orderer       = (*Driver)(nil)
)

// OrdersByKey is true: keyed jobs go through ordered lanes (see Config.KeyShards).
func (d *Driver) OrdersByKey() bool { return true }

func (d *Driver) shard(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(d.cfg.KeyShards))
}

// target is the subject a message goes to: its kind's, or its key's lane.
func (d *Driver) target(m jobqueue.Message) string {
	if m.Key != "" {
		return fmt.Sprintf("%s.key.%d.%s", d.cfg.Prefix, d.shard(m.Key), m.Kind)
	}
	return d.run(m.Kind)
}

// SendBatch publishes undelayed messages asynchronously and waits for all acknowledgements.
func (d *Driver) SendBatch(ctx context.Context, ms []jobqueue.Message) error {
	futures := make([]jetstream.PubAckFuture, 0, len(ms))
	for _, m := range ms {
		data, err := encode(m)
		if err != nil {
			return err
		}
		f, err := d.js.PublishAsync(d.target(m), data, jetstream.WithMsgID(m.Kind+":"+m.ID))
		if err != nil {
			return err
		}
		futures = append(futures, f)
	}
	for _, f := range futures {
		select {
		case <-f.Ok():
		case err := <-f.Err():
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// New makes (or updates) the streams and returns the driver.
func New(ctx context.Context, js jetstream.JetStream, cfg Config) (*Driver, error) {
	if cfg.Prefix == "" {
		cfg.Prefix = "jobs"
	}
	if cfg.Stream == "" {
		cfg.Stream = strings.ToUpper(token(cfg.Prefix))
	}
	if cfg.Poll <= 0 {
		cfg.Poll = 2 * time.Second
	}
	if cfg.AckWait <= 0 {
		cfg.AckWait = time.Minute
	}
	if cfg.Replicas <= 0 {
		cfg.Replicas = 1
	}
	if cfg.MaxAge <= 0 {
		cfg.MaxAge = 7 * 24 * time.Hour
	}
	if cfg.DeadMaxAge <= 0 {
		cfg.DeadMaxAge = 30 * 24 * time.Hour
	}
	if cfg.Duplicates <= 0 {
		cfg.Duplicates = 10 * time.Minute
	}
	if cfg.KeyShards <= 0 {
		cfg.KeyShards = 4
	}
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:              cfg.Stream,
		Subjects:          []string{cfg.Prefix + ".run.>", cfg.Prefix + ".key.>", cfg.Prefix + ".sched.>"},
		Retention:         jetstream.WorkQueuePolicy,
		Storage:           cfg.Storage,
		Replicas:          cfg.Replicas,
		MaxAge:            cfg.MaxAge,
		Duplicates:        cfg.Duplicates,
		AllowMsgSchedules: true,
		AllowRollup:       true,
	})
	if err != nil {
		return nil, fmt.Errorf("natsq: stream %s (NATS 2.12 or later is needed for schedules): %w", cfg.Stream, err)
	}
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     cfg.Stream + "_DEAD",
		Subjects: []string{cfg.Prefix + ".dead.>"},
		Storage:  cfg.Storage,
		Replicas: cfg.Replicas,
		MaxAge:   cfg.DeadMaxAge,
	})
	if err != nil {
		return nil, fmt.Errorf("natsq: dead-letter stream: %w", err)
	}
	return &Driver{js: js, cfg: cfg, consumers: map[string]jetstream.Consumer{}}, nil
}

var unsafe = regexp.MustCompile(`[^A-Za-z0-9_-]`)

// token makes s usable as one subject token or a name.
func token(s string) string { return unsafe.ReplaceAllString(s, "_") }

func (d *Driver) run(kind string) string { return d.cfg.Prefix + ".run." + kind }

// MaxDelay is Unlimited: the server holds schedules (to the second).
func (d *Driver) MaxDelay() time.Duration { return jobqueue.Unlimited }

func encode(m jobqueue.Message) ([]byte, error) { return json.Marshal(m) }

// Send publishes m, or a schedule for it when delayed. A second Send of the same id within Duplicates is dropped.
func (d *Driver) Send(ctx context.Context, m jobqueue.Message, delay time.Duration) error {
	data, err := encode(m)
	if err != nil {
		return err
	}
	if delay <= 0 {
		_, err = d.js.Publish(ctx, d.target(m), data, jetstream.WithMsgID(m.Kind+":"+m.ID))
		return err
	}
	// Schedules are to the second: round up, so it is never early.
	at := time.Now().Add(delay).Truncate(time.Second).Add(time.Second)
	msg := nats.NewMsg(d.cfg.Prefix + ".sched." + token(m.Kind+"."+m.ID))
	msg.Data = data
	msg.Header.Set("Nats-Rollup", "sub")
	_, err = d.js.PublishMsg(ctx, msg, jetstream.WithScheduleAt(at), jetstream.WithScheduleTarget(d.target(m)))
	return err
}

// SendEvery keeps one repeating schedule per job id (sending again replaces it).
func (d *Driver) SendEvery(ctx context.Context, m jobqueue.Message, every time.Duration) error {
	data, err := encode(m)
	if err != nil {
		return err
	}
	msg := nats.NewMsg(d.cfg.Prefix + ".sched." + token(m.Kind+"."+m.ID))
	msg.Data = data
	msg.Header.Set("Nats-Rollup", "sub")
	_, err = d.js.PublishMsg(ctx, msg, jetstream.WithScheduleEvery(every), jetstream.WithScheduleTarget(d.target(m)))
	return err
}

// consumer is the durable consumer of a subject (lane -1: the kind's unkeyed jobs; else a key lane, one job at a time).
func (d *Driver) consumer(ctx context.Context, kind string, lane int) (jetstream.Consumer, error) {
	name := token(d.cfg.Stream + "_" + kind)
	filter := d.run(kind)
	maxAck := 1000
	if lane >= 0 {
		name = fmt.Sprintf("%s_k%d", name, lane)
		filter = fmt.Sprintf("%s.key.%d.%s", d.cfg.Prefix, lane, kind)
		maxAck = 1
	}
	d.mu.Lock()
	c, ok := d.consumers[name]
	d.mu.Unlock()
	if ok {
		return c, nil
	}
	c, err := d.js.CreateOrUpdateConsumer(ctx, d.cfg.Stream, jetstream.ConsumerConfig{
		Durable:       name,
		FilterSubject: filter,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       d.cfg.AckWait,
		MaxDeliver:    -1, // the Queue counts attempts and dead-letters
		MaxAckPending: maxAck,
	})
	if err != nil {
		return nil, fmt.Errorf("natsq: consumer for %s: %w", kind, err)
	}
	d.mu.Lock()
	d.consumers[name] = c
	d.mu.Unlock()
	return c, nil
}

// Receive fetches the next job of kind: from a key lane if one has a job ready, else from the kind's unkeyed jobs,
// waiting up to Poll.
func (d *Driver) Receive(ctx context.Context, kind string) (jobqueue.Delivery, error) {
	ds, err := d.ReceiveBatch(ctx, kind, 1)
	if len(ds) == 0 {
		return nil, err
	}
	return ds[0], err
}

// ReceiveBatch fetches up to max jobs of kind: at most one per key lane (without waiting), the rest unkeyed.
func (d *Driver) ReceiveBatch(ctx context.Context, kind string, max int) ([]jobqueue.Delivery, error) {
	var out []jobqueue.Delivery
	start := int(time.Now().UnixNano() % int64(d.cfg.KeyShards))
	for i := 0; i < d.cfg.KeyShards && len(out) < max; i++ {
		c, err := d.consumer(ctx, kind, (start+i)%d.cfg.KeyShards)
		if err != nil {
			return out, err
		}
		batch, err := c.FetchNoWait(1)
		if err != nil {
			continue
		}
		out = append(out, d.decode(ctx, kind, batch)...)
	}
	if len(out) >= max {
		return out, nil
	}
	c, err := d.consumer(ctx, kind, -1)
	if err != nil {
		return out, err
	}
	var batch jetstream.MessageBatch
	if len(out) > 0 {
		batch, err = c.FetchNoWait(max - len(out))
	} else {
		batch, err = c.Fetch(max, jetstream.FetchMaxWait(d.cfg.Poll))
	}
	if err != nil {
		if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.Canceled) {
			return out, nil
		}
		return out, err
	}
	out = append(out, d.decode(ctx, kind, batch)...)
	if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, context.Canceled) {
		return out, err
	}
	return out, nil
}

func (d *Driver) decode(ctx context.Context, kind string, batch jetstream.MessageBatch) []jobqueue.Delivery {
	var out []jobqueue.Delivery
	for msg := range batch.Messages() {
		var m jobqueue.Message
		if err := json.Unmarshal(msg.Data(), &m); err != nil || m.Kind == "" {
			// Not ours: keep it as a dead letter rather than retrying forever.
			_ = d.DeadLetter(ctx, jobqueue.Message{ID: "undecodable", Kind: kind, Payload: json.RawMessage(quote(msg.Data()))}, 1,
				errors.New("natsq: message is not a jobqueue message"))
			_ = msg.Term()
			continue
		}
		attempt := 1
		if meta, err := msg.Metadata(); err == nil {
			attempt = int(meta.NumDelivered)
			if h := msg.Headers(); h.Get("Nats-Scheduler") != "" && h.Get("Nats-Schedule-Next") != "purge" {
				m.ID = jobqueue.Occurrence(m.ID, meta.Timestamp) // one run of a repeat
			}
		}
		out = append(out, &delivery{msg: msg, m: m, attempt: attempt})
	}
	return out
}

func quote(b []byte) []byte {
	q, _ := json.Marshal(string(b))
	return q
}

// DeadLetter publishes m on <prefix>.dead.<kind> with the attempts and the error as headers.
func (d *Driver) DeadLetter(ctx context.Context, m jobqueue.Message, attempts int, cause error) error {
	data, err := encode(m)
	if err != nil {
		return err
	}
	msg := nats.NewMsg(d.cfg.Prefix + ".dead." + m.Kind)
	msg.Data = data
	msg.Header.Set("Jobqueue-Attempts", fmt.Sprint(attempts))
	if cause != nil {
		e := cause.Error()
		if len(e) > 1000 {
			e = e[:1000]
		}
		msg.Header.Set("Jobqueue-Error", e)
	}
	_, err = d.js.PublishMsg(ctx, msg)
	return err
}

// Close does nothing: the connection is the caller's.
func (d *Driver) Close() error { return nil }

type delivery struct {
	msg     jetstream.Msg
	m       jobqueue.Message
	attempt int
}

func (x *delivery) Message() jobqueue.Message        { return x.m }
func (x *delivery) Attempt() int                     { return x.attempt }
func (x *delivery) Ack(ctx context.Context) error    { return x.msg.DoubleAck(ctx) }
func (x *delivery) Extend(ctx context.Context) error { return x.msg.InProgress() }
func (x *delivery) Retry(_ context.Context, delay time.Duration) error {
	if delay <= 0 {
		return x.msg.Nak()
	}
	return x.msg.NakWithDelay(delay)
}
