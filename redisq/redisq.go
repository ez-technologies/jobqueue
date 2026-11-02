// Package redisq is the Redis / Valkey Streams adapter of jobqueue (also ElastiCache and Memorystore): one stream per
// kind ("<prefix>:<kind>") read by a consumer group, stuck jobs (a worker died) taken over with XAUTOCLAIM after
// Visibility, delays held in a sorted set ("<prefix>:delayed") that Receive moves to the streams when due, and dead
// letters in the stream "<prefix>:dead".
//
// Keys are kept in order within each worker process only (the Queue's keyed lock): a consumer group spreads a stream
// over all consumers. Delivery is at least once.
package redisq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ez-technologies/jobqueue"
)

// Config of the driver.
type Config struct {
	// Prefix of the keys (default "jobs"); Group is the consumer group (default "workers").
	Prefix string
	Group  string
	// Consumer names this process in the group (default host name, pid and a random part).
	Consumer string
	// Poll is how long Receive blocks for a job (default 2s).
	Poll time.Duration
	// Visibility is how long a job may run without an Extend before another worker takes it over (default 60s).
	Visibility time.Duration
	// DeadMaxLen trims the dead-letter stream to about this many entries (default 10000).
	DeadMaxLen int64
}

// Driver is the Streams driver.
type Driver struct {
	c   redis.UniversalClient
	cfg Config

	mu        sync.Mutex
	groups    map[string]bool
	lastMoved time.Time
}

var (
	_ jobqueue.Driver        = (*Driver)(nil)
	_ jobqueue.BatchSender   = (*Driver)(nil)
	_ jobqueue.BatchReceiver = (*Driver)(nil)
)

// New returns a driver over a Redis client (the caller's).
func New(c redis.UniversalClient, cfg Config) *Driver {
	if cfg.Prefix == "" {
		cfg.Prefix = "jobs"
	}
	if cfg.Group == "" {
		cfg.Group = "workers"
	}
	if cfg.Consumer == "" {
		host, _ := os.Hostname()
		b := make([]byte, 4)
		_, _ = rand.Read(b)
		cfg.Consumer = fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(b))
	}
	if cfg.Poll <= 0 {
		cfg.Poll = 2 * time.Second
	}
	if cfg.Visibility <= 0 {
		cfg.Visibility = time.Minute
	}
	if cfg.DeadMaxLen <= 0 {
		cfg.DeadMaxLen = 10000
	}
	return &Driver{c: c, cfg: cfg, groups: map[string]bool{}}
}

func (d *Driver) stream(kind string) string { return d.cfg.Prefix + ":" + kind }
func (d *Driver) delayed() string           { return d.cfg.Prefix + ":delayed" }

// MaxDelay is Unlimited (the delayed set).
func (d *Driver) MaxDelay() time.Duration { return jobqueue.Unlimited }

// group makes the kind's stream and consumer group once (reading from the start, so nothing sent before is missed).
func (d *Driver) group(ctx context.Context, kind string) error {
	d.mu.Lock()
	ok := d.groups[kind]
	d.mu.Unlock()
	if ok {
		return nil
	}
	err := d.c.XGroupCreateMkStream(ctx, d.stream(kind), d.cfg.Group, "0").Err()
	if err != nil && !isBusyGroup(err) {
		return err
	}
	d.mu.Lock()
	d.groups[kind] = true
	d.mu.Unlock()
	return nil
}

func isBusyGroup(err error) bool {
	return err != nil && len(err.Error()) >= 9 && err.Error()[:9] == "BUSYGROUP"
}

func fields(m jobqueue.Message, attempt int) (map[string]any, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return map[string]any{"m": string(b), "a": attempt}, nil
}

type held struct {
	Stream  string `json:"s"`
	Message string `json:"m"` // the message's JSON, passed through untouched
	Attempt int    `json:"a"`
	Nonce   string `json:"n"`
}

func (d *Driver) hold(ctx context.Context, pipe redis.Pipeliner, m jobqueue.Message, attempt int, delay time.Duration) error {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	mj, err := json.Marshal(m)
	if err != nil {
		return err
	}
	member, err := json.Marshal(held{Stream: d.stream(m.Route()), Message: string(mj), Attempt: attempt, Nonce: hex.EncodeToString(b)})
	if err != nil {
		return err
	}
	return pipe.ZAdd(ctx, d.delayed(), redis.Z{Score: float64(time.Now().Add(delay).UnixMilli()), Member: string(member)}).Err()
}

// Send adds m to its stream, or to the delayed set.
func (d *Driver) Send(ctx context.Context, m jobqueue.Message, delay time.Duration) error {
	if delay > 0 {
		pipe := d.c.Pipeline()
		if err := d.hold(ctx, pipe, m, 1, delay); err != nil {
			return err
		}
		_, err := pipe.Exec(ctx)
		return err
	}
	return d.SendBatch(ctx, []jobqueue.Message{m})
}

// SendBatch adds messages in one pipeline.
func (d *Driver) SendBatch(ctx context.Context, ms []jobqueue.Message) error {
	pipe := d.c.Pipeline()
	for _, m := range ms {
		if err := d.group(ctx, m.Route()); err != nil {
			return err
		}
		f, err := fields(m, 1)
		if err != nil {
			return err
		}
		pipe.XAdd(ctx, &redis.XAddArgs{Stream: d.stream(m.Route()), Values: f})
	}
	_, err := pipe.Exec(ctx)
	return err
}

// moveDue moves due delayed jobs to their streams, atomically per job.
var moveDue = redis.NewScript(`
local due = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, 100)
for _, member in ipairs(due) do
	local h = cjson.decode(member)
	redis.call("XADD", h["s"], "*", "m", h["m"], "a", h["a"])
	redis.call("ZREM", KEYS[1], member)
end
return #due`)

func (d *Driver) move(ctx context.Context) {
	d.mu.Lock()
	if time.Since(d.lastMoved) < 200*time.Millisecond {
		d.mu.Unlock()
		return
	}
	d.lastMoved = time.Now()
	d.mu.Unlock()
	_ = moveDue.Run(ctx, d.c, []string{d.delayed()}, time.Now().UnixMilli()).Err()
}

// Receive takes one job of kind.
func (d *Driver) Receive(ctx context.Context, kind string) (jobqueue.Delivery, error) {
	ds, err := d.ReceiveBatch(ctx, kind, 1)
	if len(ds) == 0 {
		return nil, err
	}
	return ds[0], err
}

// ReceiveBatch takes up to max jobs of kind: stuck ones first (XAUTOCLAIM), then new ones (XREADGROUP, blocking up
// to Poll).
func (d *Driver) ReceiveBatch(ctx context.Context, kind string, max int) ([]jobqueue.Delivery, error) {
	if err := d.group(ctx, kind); err != nil {
		return nil, err
	}
	d.move(ctx)
	stream := d.stream(kind)
	claimed, _, err := d.c.XAutoClaim(ctx, &redis.XAutoClaimArgs{Stream: stream, Group: d.cfg.Group,
		Consumer: d.cfg.Consumer, MinIdle: d.cfg.Visibility, Start: "0-0", Count: int64(max)}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	if len(claimed) > 0 {
		return d.deliveries(ctx, kind, claimed, true), nil
	}
	block := d.cfg.Poll
	if dl, ok := ctx.Deadline(); ok && time.Until(dl) < block {
		block = max0(time.Until(dl))
	}
	res, err := d.c.XReadGroup(ctx, &redis.XReadGroupArgs{Group: d.cfg.Group, Consumer: d.cfg.Consumer,
		Streams: []string{stream, ">"}, Count: int64(max), Block: block}).Result()
	if errors.Is(err, redis.Nil) || ctx.Err() != nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []jobqueue.Delivery
	for _, s := range res {
		out = append(out, d.deliveries(ctx, kind, s.Messages, false)...)
	}
	return out, nil
}

func max0(d time.Duration) time.Duration {
	if d < time.Millisecond {
		return time.Millisecond
	}
	return d
}

func (d *Driver) deliveries(ctx context.Context, kind string, xs []redis.XMessage, claimed bool) []jobqueue.Delivery {
	var out []jobqueue.Delivery
	for _, x := range xs {
		raw, _ := x.Values["m"].(string)
		var m jobqueue.Message
		if err := json.Unmarshal([]byte(raw), &m); err != nil || m.Kind == "" {
			_ = d.c.XAck(ctx, d.stream(kind), d.cfg.Group, x.ID).Err() // not ours
			continue
		}
		attempt := 1
		if a, ok := x.Values["a"].(string); ok {
			attempt, _ = strconv.Atoi(a)
		}
		if claimed {
			// Taken over from a worker that stopped: count its deliveries too.
			if p, err := d.c.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: d.stream(kind), Group: d.cfg.Group,
				Start: x.ID, End: x.ID, Count: 1}).Result(); err == nil && len(p) == 1 && p[0].RetryCount > 1 {
				attempt += int(p[0].RetryCount) - 1
			}
		}
		out = append(out, &delivery{d: d, stream: d.stream(kind), id: x.ID, m: m, attempt: attempt})
	}
	return out
}

// DeadLetter adds m to "<prefix>:dead".
func (d *Driver) DeadLetter(ctx context.Context, m jobqueue.Message, attempts int, cause error) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	e := ""
	if cause != nil {
		e = cause.Error()
	}
	return d.c.XAdd(ctx, &redis.XAddArgs{Stream: d.cfg.Prefix + ":dead", MaxLen: d.cfg.DeadMaxLen, Approx: true,
		Values: map[string]any{"kind": m.Kind, "m": string(b), "attempts": attempts, "error": e}}).Err()
}

// Close does nothing: the client is the caller's.
func (d *Driver) Close() error { return nil }

type delivery struct {
	d       *Driver
	stream  string
	id      string
	m       jobqueue.Message
	attempt int
}

func (x *delivery) Message() jobqueue.Message { return x.m }

// Origin is the stream and the entry id.
func (x *delivery) Origin() string { return "redis:" + x.stream + "#" + x.id }
func (x *delivery) Attempt() int   { return x.attempt }

func (x *delivery) Ack(ctx context.Context) error {
	pipe := x.d.c.TxPipeline()
	pipe.XAck(ctx, x.stream, x.d.cfg.Group, x.id)
	pipe.XDel(ctx, x.stream, x.id)
	_, err := pipe.Exec(ctx)
	return err
}

// Retry adds the job again (now, or through the delayed set) with the next attempt, and removes this entry.
func (x *delivery) Retry(ctx context.Context, delay time.Duration) error {
	pipe := x.d.c.TxPipeline()
	if delay > 0 {
		if err := x.d.hold(ctx, pipe, x.m, x.attempt+1, delay); err != nil {
			return err
		}
	} else {
		f, err := fields(x.m, x.attempt+1)
		if err != nil {
			return err
		}
		pipe.XAdd(ctx, &redis.XAddArgs{Stream: x.stream, Values: f})
	}
	pipe.XAck(ctx, x.stream, x.d.cfg.Group, x.id)
	pipe.XDel(ctx, x.stream, x.id)
	_, err := pipe.Exec(ctx)
	return err
}

// Extend claims the entry again for this consumer, which resets its idle time.
func (x *delivery) Extend(ctx context.Context) error {
	return x.d.c.XClaimJustID(ctx, &redis.XClaimArgs{Stream: x.stream, Group: x.d.cfg.Group, Consumer: x.d.cfg.Consumer,
		MinIdle: 0, Messages: []string{x.id}}).Err()
}

// VersionStore keeps the newest version per key in Redis ("<prefix>:version:<key>"), raised atomically by a script.
type VersionStore struct {
	c      redis.UniversalClient
	prefix string
}

// NewVersionStore returns versions under prefix (default "jobs").
func NewVersionStore(c redis.UniversalClient, prefix string) *VersionStore {
	if prefix == "" {
		prefix = "jobs"
	}
	return &VersionStore{c: c, prefix: prefix}
}

var _ jobqueue.VersionStore = (*VersionStore)(nil)

var raise = redis.NewScript(`local cur = redis.call("GET", KEYS[1])
if not cur or tonumber(cur) < tonumber(ARGV[1]) then redis.call("SET", KEYS[1], ARGV[1]) end
return 1`)

func (v *VersionStore) key(k string) string {
	return v.prefix + ":version:" + hex.EncodeToString([]byte(k))
}

func (v *VersionStore) Observe(ctx context.Context, key string, version int64) error {
	return raise.Run(ctx, v.c, []string{v.key(key)}, version).Err()
}

func (v *VersionStore) Latest(ctx context.Context, key string) (int64, bool, error) {
	n, err := v.c.Get(ctx, v.key(key)).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	return n, err == nil, err
}
