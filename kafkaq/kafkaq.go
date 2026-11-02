// Package kafkaq is the Kafka adapter of jobqueue, on franz-go: Apache Kafka, AWS MSK, Confluent Cloud, Redpanda.
//
// Kafka has no per-message acknowledgement and no delayed delivery, so this driver emulates them:
//
//   - Each kind has a topic "<prefix>.<kind>" read by the consumer group "<prefix>.<kind>". A job's Key is the record
//     key, so a key always lands in the same partition, and the driver never hands out a record while an earlier one
//     with its key (in this partition) is still running: per-key order across all workers.
//   - Offsets are committed per partition only up to the first job not yet done, so a crash redelivers everything
//     unfinished (at least once). One slow job holds back the commit (not the work) of its partition.
//   - A retry or a delayed job is a record on a retry tier, "<prefix>.<kind>.retry-<tier>" (default 5s, 1m0s, 10m0s:
//     the smallest tier at least as long as the delay), with a not-before time; the driver reads each tier in order,
//     waits until a record is due and puts it back on the kind's topic. All records of a tier wait about as long, so
//     none holds up a shorter one; delays beyond the last tier go through the Queue's Scheduler.
//   - Dead letters go to "<prefix>.dead".
package kafkaq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ez-technologies/jobqueue"
)

// Config of the driver.
type Config struct {
	// Brokers are the seed brokers (required). Options are added to every client (TLS, SASL for MSK or Confluent).
	Brokers []string
	Options []kgo.Opt
	// Prefix of topics and groups (default "jobs").
	Prefix string
	// CreateTopics makes missing topics with Partitions partitions (default 6) and ReplicationFactor (default -1:
	// the broker's default).
	CreateTopics      bool
	Partitions        int32
	ReplicationFactor int16
	// Poll is how long Receive waits (default 2s).
	Poll time.Duration
	// RetryTiers are the retry topics' delays (default 5s, 1m, 10m); MaxDelay is the last.
	RetryTiers []time.Duration
}

// Driver is the Kafka driver.
type Driver struct {
	cfg  Config
	prod *kgo.Client

	mu       sync.Mutex
	createMu sync.Mutex
	kinds    map[string]*consumer
	topics   map[string]bool
	closed   bool
}

var (
	_ jobqueue.Driver        = (*Driver)(nil)
	_ jobqueue.BatchSender   = (*Driver)(nil)
	_ jobqueue.BatchReceiver = (*Driver)(nil)
	_ jobqueue.Orderer       = (*Driver)(nil)
)

// New connects the producer.
func New(cfg Config) (*Driver, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafkaq: Brokers are required")
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "jobs"
	}
	if cfg.Partitions <= 0 {
		cfg.Partitions = 6
	}
	if cfg.ReplicationFactor == 0 {
		cfg.ReplicationFactor = -1
	}
	if cfg.Poll <= 0 {
		cfg.Poll = 2 * time.Second
	}
	if len(cfg.RetryTiers) == 0 {
		cfg.RetryTiers = []time.Duration{5 * time.Second, time.Minute, 10 * time.Minute}
	}
	sort.Slice(cfg.RetryTiers, func(i, j int) bool { return cfg.RetryTiers[i] < cfg.RetryTiers[j] })
	opts := append([]kgo.Opt{kgo.SeedBrokers(cfg.Brokers...), kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.ProducerLinger(5 * time.Millisecond)}, cfg.Options...)
	prod, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Driver{cfg: cfg, prod: prod, kinds: map[string]*consumer{}, topics: map[string]bool{}}, nil
}

var unsafe = regexp.MustCompile(`[^A-Za-z0-9._-]`)

func (d *Driver) topic(kind string) string {
	return unsafe.ReplaceAllString(d.cfg.Prefix+"."+kind, "_")
}

// tiers are the retry topics of a route, shortest delay first.
func (d *Driver) tiers(route string) []string {
	out := make([]string, len(d.cfg.RetryTiers))
	for i, t := range d.cfg.RetryTiers {
		out[i] = d.topic(route) + ".retry-" + t.String()
	}
	return out
}

// tierFor is the retry topic for a delay: the smallest tier at least as long, else the last.
func (d *Driver) tierFor(route string, delay time.Duration) string {
	ts := d.tiers(route)
	for i, t := range d.cfg.RetryTiers {
		if t >= delay {
			return ts[i]
		}
	}
	return ts[len(ts)-1]
}

func (d *Driver) topicsOf(route string) []string {
	return append([]string{d.topic(route)}, d.tiers(route)...)
}
func (d *Driver) dead() string { return d.cfg.Prefix + ".dead" }

// OrdersByKey is true (record key = job key, one running record per key and partition).
func (d *Driver) OrdersByKey() bool { return true }

// MaxDelay is the last retry tier (10 minutes by default).
func (d *Driver) MaxDelay() time.Duration { return d.cfg.RetryTiers[len(d.cfg.RetryTiers)-1] }

func (d *Driver) ensure(ctx context.Context, topics ...string) error {
	if !d.cfg.CreateTopics {
		return nil
	}
	d.createMu.Lock() // one creation at a time: brokers answer concurrent creations of a topic oddly
	defer d.createMu.Unlock()
	d.mu.Lock()
	var missing []string
	for _, t := range topics {
		if !d.topics[t] {
			missing = append(missing, t)
		}
	}
	d.mu.Unlock()
	if len(missing) == 0 {
		return nil
	}
	adm := kadm.NewClient(d.prod)
	res, err := adm.CreateTopics(ctx, d.cfg.Partitions, d.cfg.ReplicationFactor, nil, missing...)
	if err != nil {
		return err
	}
	var failed []string
	for _, r := range res {
		if r.Err != nil && !isExists(r.Err) {
			failed = append(failed, r.Topic)
		}
	}
	if len(failed) > 0 {
		// Perhaps made meanwhile by another process: they count if the broker lists them.
		listed, lerr := adm.ListTopics(ctx, failed...)
		for _, t := range failed {
			if lerr != nil || !listed.Has(t) {
				return fmt.Errorf("kafkaq: create %s: %w", t, res[t].Err)
			}
		}
	}
	d.mu.Lock()
	for _, t := range missing {
		d.topics[t] = true
	}
	d.mu.Unlock()
	return nil
}

func isExists(err error) bool { return errors.Is(err, kerr.TopicAlreadyExists) }

func record(topic string, m jobqueue.Message, attempt int, notBefore time.Time) (*kgo.Record, error) {
	body, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	r := &kgo.Record{Topic: topic, Value: body,
		Headers: []kgo.RecordHeader{{Key: "jobqueue-attempt", Value: []byte(strconv.Itoa(attempt))}}}
	if m.Key != "" {
		r.Key = []byte(m.Key)
	}
	if !notBefore.IsZero() {
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: "jobqueue-not-before", Value: []byte(strconv.FormatInt(notBefore.UnixMilli(), 10))})
	}
	return r, nil
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Send produces m to its kind's topic, or to a retry tier when delayed.
func (d *Driver) Send(ctx context.Context, m jobqueue.Message, delay time.Duration) error {
	if delay <= 0 {
		return d.SendBatch(ctx, []jobqueue.Message{m})
	}
	if err := d.ensure(ctx, d.topicsOf(m.Route())...); err != nil {
		return err
	}
	r, err := record(d.tierFor(m.Route(), delay), m, 1, time.Now().Add(delay))
	if err != nil {
		return err
	}
	return d.prod.ProduceSync(ctx, r).FirstErr()
}

// SendBatch produces all messages and waits for every acknowledgement.
func (d *Driver) SendBatch(ctx context.Context, ms []jobqueue.Message) error {
	rs := make([]*kgo.Record, 0, len(ms))
	for _, m := range ms {
		if err := d.ensure(ctx, d.topicsOf(m.Route())...); err != nil {
			return err
		}
		r, err := record(d.topic(m.Route()), m, 1, time.Time{})
		if err != nil {
			return err
		}
		rs = append(rs, r)
	}
	return d.prod.ProduceSync(ctx, rs...).FirstErr()
}

// DeadLetter produces m to "<prefix>.dead" with the attempts and the error as headers.
func (d *Driver) DeadLetter(ctx context.Context, m jobqueue.Message, attempts int, cause error) error {
	if err := d.ensure(ctx, d.dead()); err != nil {
		return err
	}
	r, err := record(d.dead(), m, attempts, time.Time{})
	if err != nil {
		return err
	}
	if cause != nil {
		e := cause.Error()
		if len(e) > 1000 {
			e = e[:1000]
		}
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: "jobqueue-error", Value: []byte(e)}, kgo.RecordHeader{Key: "jobqueue-kind", Value: []byte(m.Kind)})
	}
	return d.prod.ProduceSync(ctx, r).FirstErr()
}

// Receive waits up to Poll for one job of kind.
func (d *Driver) Receive(ctx context.Context, kind string) (jobqueue.Delivery, error) {
	ds, err := d.ReceiveBatch(ctx, kind, 1)
	if len(ds) == 0 {
		return nil, err
	}
	return ds[0], err
}

// ReceiveBatch waits up to Poll for a job of kind, then takes what else is ready, up to max.
func (d *Driver) ReceiveBatch(ctx context.Context, kind string, max int) ([]jobqueue.Delivery, error) {
	c, err := d.consumer(ctx, kind)
	if err != nil {
		return nil, err
	}
	return c.take(ctx, max, d.cfg.Poll), nil
}

// Close stops the consumers and the producer.
func (d *Driver) Close() error {
	d.mu.Lock()
	d.closed = true
	kinds := d.kinds
	d.mu.Unlock()
	for _, c := range kinds {
		c.close()
	}
	d.prod.Close()
	return nil
}

// consumer reads one kind's topic and retry topic.
type consumer struct {
	d    *Driver
	kind string
	cl   *kgo.Client
	stop context.CancelFunc
	done chan struct{}

	mu      sync.Mutex
	ready   []*delivery            // handed out in this order
	waiting map[string][]*delivery // per partition and key: behind a running record of the same key
	running map[string]bool        // partition and key with a record out
	tracks  map[tp]*track
	wake    chan struct{}
}

type tp struct {
	topic     string
	partition int32
}

// track is the offsets of one partition: received in order, committed up to the first not done.
type track struct {
	offsets []int64
	done    map[int64]bool
	epoch   int32
}

func (d *Driver) consumer(ctx context.Context, kind string) (*consumer, error) {
	d.mu.Lock()
	c, ok := d.kinds[kind]
	closed := d.closed
	d.mu.Unlock()
	if ok {
		return c, nil
	}
	if closed {
		return nil, errors.New("kafkaq: closed")
	}
	if err := d.ensure(ctx, d.topicsOf(kind)...); err != nil {
		return nil, err
	}
	c = &consumer{d: d, kind: kind, waiting: map[string][]*delivery{}, running: map[string]bool{}, tracks: map[tp]*track{},
		wake: make(chan struct{}), done: make(chan struct{})}
	opts := append([]kgo.Opt{kgo.SeedBrokers(d.cfg.Brokers...), kgo.ConsumerGroup(d.topic(kind)),
		kgo.ConsumeTopics(d.topicsOf(kind)...), kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.OnPartitionsRevoked(c.revoked), kgo.OnPartitionsLost(c.revoked)}, d.cfg.Options...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	c.cl = cl
	pctx, cancel := context.WithCancel(context.Background())
	c.stop = cancel
	d.mu.Lock()
	if prev, ok := d.kinds[kind]; ok {
		d.mu.Unlock()
		cancel()
		cl.Close()
		return prev, nil
	}
	d.kinds[kind] = c
	d.mu.Unlock()
	go c.poll(pctx)
	return c, nil
}

func (c *consumer) close() {
	c.stop()
	<-c.done
	c.cl.Close()
}

// revoked forgets partitions this member no longer owns (their unfinished records go to the new owner).
func (c *consumer) revoked(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for topic, parts := range lost {
		for _, p := range parts {
			delete(c.tracks, tp{topic, p})
		}
	}
}

func (c *consumer) signal() {
	close(c.wake)
	c.wake = make(chan struct{})
}

func (c *consumer) poll(ctx context.Context) {
	defer close(c.done)
	tiers := map[string]chan *kgo.Record{}
	for _, t := range c.d.tiers(c.kind) {
		ch := make(chan *kgo.Record, 1000)
		tiers[t] = ch
		go c.retries(ctx, ch) // one per tier: a tier's records wait about as long as each other
	}
	for ctx.Err() == nil {
		fs := c.cl.PollRecords(ctx, 500)
		fs.EachRecord(func(r *kgo.Record) {
			c.mu.Lock()
			t := c.tracks[tp{r.Topic, r.Partition}]
			if t == nil {
				t = &track{done: map[int64]bool{}}
				c.tracks[tp{r.Topic, r.Partition}] = t
			}
			t.offsets = append(t.offsets, r.Offset)
			t.epoch = r.LeaderEpoch
			c.mu.Unlock()
			if ch, ok := tiers[r.Topic]; ok {
				select {
				case ch <- r:
				case <-ctx.Done():
				}
				return
			}
			c.add(r)
		})
	}
}

// retries puts retry records back on the kind's topic when due, in order.
func (c *consumer) retries(ctx context.Context, in <-chan *kgo.Record) {
	for {
		select {
		case <-ctx.Done():
			return
		case r := <-in:
			if nb, err := strconv.ParseInt(header(r, "jobqueue-not-before"), 10, 64); err == nil {
				if wait := time.Until(time.UnixMilli(nb)); wait > 0 {
					t := time.NewTimer(wait)
					select {
					case <-ctx.Done():
						t.Stop()
						return
					case <-t.C:
					}
				}
			}
			back := &kgo.Record{Topic: c.d.topic(c.kind), Key: r.Key, Value: r.Value,
				Headers: []kgo.RecordHeader{{Key: "jobqueue-attempt", Value: []byte(header(r, "jobqueue-attempt"))}}}
			if err := c.d.prod.ProduceSync(ctx, back).FirstErr(); err != nil {
				continue // not committed: read again after a restart
			}
			c.finish(r)
		}
	}
}

func laneOf(r *kgo.Record) string {
	if len(r.Key) == 0 {
		return ""
	}
	return strconv.Itoa(int(r.Partition)) + "\x00" + string(r.Key)
}

// add queues a record, behind a running one with its key.
func (c *consumer) add(r *kgo.Record) {
	var m jobqueue.Message
	if err := json.Unmarshal(r.Value, &m); err != nil || m.Kind == "" {
		c.finish(r) // not ours: skipped
		return
	}
	attempt, _ := strconv.Atoi(header(r, "jobqueue-attempt"))
	x := &delivery{c: c, r: r, m: m, attempt: max(attempt, 1)}
	c.mu.Lock()
	defer c.mu.Unlock()
	if lane := laneOf(r); lane != "" {
		if c.running[lane] {
			c.waiting[lane] = append(c.waiting[lane], x)
			return
		}
		c.running[lane] = true
	}
	c.ready = append(c.ready, x)
	c.signal()
}

// take answers up to max ready deliveries, waiting up to poll for the first.
func (c *consumer) take(ctx context.Context, max int, poll time.Duration) []jobqueue.Delivery {
	t := time.NewTimer(poll)
	defer t.Stop()
	for {
		c.mu.Lock()
		if len(c.ready) > 0 {
			n := min(max, len(c.ready))
			out := make([]jobqueue.Delivery, n)
			for i := 0; i < n; i++ {
				out[i] = c.ready[i]
			}
			c.ready = c.ready[n:]
			c.mu.Unlock()
			return out
		}
		wake := c.wake
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			return nil
		case <-wake:
		}
	}
}

// release lets the next record of r's key go.
func (c *consumer) release(r *kgo.Record) {
	lane := laneOf(r)
	if lane == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if next := c.waiting[lane]; len(next) > 0 {
		c.waiting[lane] = next[1:]
		if len(c.waiting[lane]) == 0 {
			delete(c.waiting, lane)
		}
		c.ready = append(c.ready, next[0])
		c.signal()
		return
	}
	delete(c.running, lane)
}

// finish marks r done and commits its partition up to the first record not done.
func (c *consumer) finish(r *kgo.Record) {
	c.mu.Lock()
	t := c.tracks[tp{r.Topic, r.Partition}]
	if t == nil {
		c.mu.Unlock()
		return // the partition moved to another member
	}
	t.done[r.Offset] = true
	commit := int64(-1)
	for len(t.offsets) > 0 && t.done[t.offsets[0]] {
		commit = t.offsets[0] + 1
		delete(t.done, t.offsets[0])
		t.offsets = t.offsets[1:]
	}
	epoch := t.epoch
	c.mu.Unlock()
	if commit < 0 {
		return
	}
	c.cl.CommitOffsets(context.Background(), map[string]map[int32]kgo.EpochOffset{
		r.Topic: {r.Partition: {Epoch: epoch, Offset: commit}}}, nil)
}

type delivery struct {
	c       *consumer
	r       *kgo.Record
	m       jobqueue.Message
	attempt int
}

func (x *delivery) Message() jobqueue.Message { return x.m }

// Origin is the topic, partition and offset the job was read from.
func (x *delivery) Origin() string {
	return fmt.Sprintf("kafka:%s/%d@%d", x.r.Topic, x.r.Partition, x.r.Offset)
}
func (x *delivery) Attempt() int                 { return x.attempt }
func (x *delivery) Extend(context.Context) error { return nil }

// Ack marks the record done.
func (x *delivery) Ack(context.Context) error {
	x.c.release(x.r)
	x.c.finish(x.r)
	return nil
}

// Retry with a delay produces the job to the retry topic, then marks this record done. Without one (a worker
// stopping), the record goes back to the front of this process's queue, uncommitted, so it keeps its place.
func (x *delivery) Retry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		x.c.mu.Lock()
		x.c.ready = append([]*delivery{{c: x.c, r: x.r, m: x.m, attempt: x.attempt + 1}}, x.c.ready...)
		x.c.signal()
		x.c.mu.Unlock()
		return nil
	}
	r, err := record(x.c.d.tierFor(x.c.kind, delay), x.m, x.attempt+1, time.Now().Add(delay))
	if err != nil {
		return err
	}
	if err := x.c.d.prod.ProduceSync(ctx, r).FirstErr(); err != nil {
		return err
	}
	x.c.release(x.r)
	x.c.finish(x.r)
	return nil
}
