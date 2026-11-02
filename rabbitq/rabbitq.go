// Package rabbitq is the RabbitMQ adapter of jobqueue (also Amazon MQ for RabbitMQ and CloudAMQP), over AMQP 0-9-1.
//
//   - Each kind has a durable quorum queue "<prefix>.<kind>" (quorum queues count deliveries).
//   - Keyed jobs go to one of KeyShards lanes "<prefix>.<kind>.k<n>": quorum queues with a single active consumer,
//     read with a prefetch of 1, so a key's jobs run one at a time in order across all workers.
//   - Delays: no plugin needed. A delayed job waits in "<prefix>.wait.<target>.<seconds>s", a queue whose messages
//     expire after that many seconds and are dead-lettered to the target queue; idle wait queues delete themselves.
//   - Dead letters go to the queue "<prefix>.dead".
//
// Publishing waits for publisher confirms. A running job is not extended: RabbitMQ's consumer_timeout (30 minutes by
// default) bounds a job's run time.
package rabbitq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/ez-technologies/jobqueue"
)

// Config of the driver.
type Config struct {
	// Prefix of the queue names (default "jobs").
	Prefix string
	// Prefetch is how many unkeyed jobs of a kind a worker holds unacknowledged (default 20).
	Prefetch int
	// KeyShards is how many ordered lanes each kind has for keyed jobs (default 4).
	KeyShards int
	// Poll is how long Receive waits (default 2s).
	Poll time.Duration
}

// Driver is the RabbitMQ driver.
type Driver struct {
	conn *amqp.Connection
	cfg  Config

	pubMu sync.Mutex
	pub   *amqp.Channel

	mu        sync.Mutex
	declared  map[string]bool
	consumers map[string]chan *delivery
	chans     []*amqp.Channel
}

var (
	_ jobqueue.Driver        = (*Driver)(nil)
	_ jobqueue.BatchSender   = (*Driver)(nil)
	_ jobqueue.BatchReceiver = (*Driver)(nil)
	_ jobqueue.Orderer       = (*Driver)(nil)
)

// New returns a driver over an AMQP connection (the caller's).
func New(conn *amqp.Connection, cfg Config) (*Driver, error) {
	if cfg.Prefix == "" {
		cfg.Prefix = "jobs"
	}
	if cfg.Prefetch <= 0 {
		cfg.Prefetch = 20
	}
	if cfg.KeyShards <= 0 {
		cfg.KeyShards = 4
	}
	if cfg.Poll <= 0 {
		cfg.Poll = 2 * time.Second
	}
	pub, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := pub.Confirm(false); err != nil {
		return nil, err
	}
	return &Driver{conn: conn, cfg: cfg, pub: pub, declared: map[string]bool{}, consumers: map[string]chan *delivery{}}, nil
}

// OrdersByKey is true: keyed jobs go through single-active-consumer lanes.
func (d *Driver) OrdersByKey() bool { return true }

// MaxDelay is Unlimited (wait queues).
func (d *Driver) MaxDelay() time.Duration { return jobqueue.Unlimited }

func (d *Driver) queue(kind string) string { return d.cfg.Prefix + "." + kind }

func (d *Driver) lane(kind string, n int) string {
	return fmt.Sprintf("%s.%s.k%d", d.cfg.Prefix, kind, n)
}

func (d *Driver) target(m jobqueue.Message) string {
	if m.Key == "" {
		return d.queue(m.Route())
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(m.Key))
	return d.lane(m.Route(), int(h.Sum32()%uint32(d.cfg.KeyShards)))
}

// declare makes a queue once, on the publishing channel.
func (d *Driver) declare(name string, args amqp.Table) error {
	d.mu.Lock()
	ok := d.declared[name]
	d.mu.Unlock()
	if ok {
		return nil
	}
	d.pubMu.Lock()
	_, err := d.pub.QueueDeclare(name, true, false, false, false, args)
	d.pubMu.Unlock()
	if err != nil {
		return fmt.Errorf("rabbitq: declare %s: %w", name, err)
	}
	d.mu.Lock()
	d.declared[name] = true
	d.mu.Unlock()
	return nil
}

func quorum(single bool) amqp.Table {
	t := amqp.Table{"x-queue-type": "quorum"}
	if single {
		t["x-single-active-consumer"] = true
	}
	return t
}

func (d *Driver) declareKind(kind string) error {
	if err := d.declare(d.queue(kind), quorum(false)); err != nil {
		return err
	}
	for n := 0; n < d.cfg.KeyShards; n++ {
		if err := d.declare(d.lane(kind, n), quorum(true)); err != nil {
			return err
		}
	}
	return nil
}

// waitQueue is where a message for target waits delay (rounded up to the second) before going there.
func (d *Driver) waitQueue(target string, delay time.Duration) (string, error) {
	secs := int64((delay + time.Second - 1) / time.Second)
	name := fmt.Sprintf("%s.wait.%s.%ds", d.cfg.Prefix, target, secs)
	return name, d.declare(name, amqp.Table{
		"x-message-ttl":             secs * 1000,
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": target,
		"x-expires":                 secs*1000 + 60_000,
	})
}

type out struct {
	queue string
	m     jobqueue.Message
	hdr   amqp.Table
}

// publish sends messages to queues and waits for the broker's confirms.
func (d *Driver) publish(ctx context.Context, msgs []out) error {
	confirms := make([]*amqp.DeferredConfirmation, 0, len(msgs))
	d.pubMu.Lock()
	for _, o := range msgs {
		body, err := json.Marshal(o.m)
		if err != nil {
			d.pubMu.Unlock()
			return err
		}
		c, err := d.pub.PublishWithDeferredConfirmWithContext(ctx, "", o.queue, true, false, amqp.Publishing{
			ContentType: "application/json", DeliveryMode: amqp.Persistent, Body: body, Headers: o.hdr, MessageId: o.m.ID})
		if err != nil {
			d.pubMu.Unlock()
			return err
		}
		confirms = append(confirms, c)
	}
	d.pubMu.Unlock()
	for _, c := range confirms {
		ok, err := c.WaitContext(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("rabbitq: the broker refused a message")
		}
	}
	return nil
}

func (d *Driver) outFor(m jobqueue.Message, attempt int, delay time.Duration) (out, error) {
	if err := d.declareKind(m.Route()); err != nil {
		return out{}, err
	}
	o := out{queue: d.target(m), m: m, hdr: amqp.Table{"jobqueue-attempt": int32(attempt)}}
	if delay > 0 {
		w, err := d.waitQueue(o.queue, delay)
		if err != nil {
			return out{}, err
		}
		o.queue = w
	}
	return o, nil
}

// Send publishes m, through a wait queue when delayed.
func (d *Driver) Send(ctx context.Context, m jobqueue.Message, delay time.Duration) error {
	o, err := d.outFor(m, 1, delay)
	if err != nil {
		return err
	}
	return d.publish(ctx, []out{o})
}

// SendBatch publishes many and waits for all confirms at once.
func (d *Driver) SendBatch(ctx context.Context, ms []jobqueue.Message) error {
	list := make([]out, 0, len(ms))
	for _, m := range ms {
		o, err := d.outFor(m, 1, 0)
		if err != nil {
			return err
		}
		list = append(list, o)
	}
	return d.publish(ctx, list)
}

// deliveries starts the kind's consumers once and answers the channel they feed.
func (d *Driver) deliveries(kind string) (chan *delivery, error) {
	d.mu.Lock()
	ch, ok := d.consumers[kind]
	d.mu.Unlock()
	if ok {
		return ch, nil
	}
	if err := d.declareKind(kind); err != nil {
		return nil, err
	}
	ch = make(chan *delivery)
	consume := func(queue string, prefetch int) error {
		c, err := d.conn.Channel()
		if err != nil {
			return err
		}
		if err := c.Qos(prefetch, 0, false); err != nil {
			return err
		}
		in, err := c.Consume(queue, "", false, false, false, false, nil)
		if err != nil {
			return err
		}
		d.mu.Lock()
		d.chans = append(d.chans, c)
		d.mu.Unlock()
		go func() {
			for raw := range in {
				var m jobqueue.Message
				if err := json.Unmarshal(raw.Body, &m); err != nil || m.Kind == "" {
					_ = raw.Reject(false) // not ours
					continue
				}
				ch <- &delivery{d: d, raw: raw, m: m, attempt: attemptOf(raw)}
			}
		}()
		return nil
	}
	if err := consume(d.queue(kind), d.cfg.Prefetch); err != nil {
		return nil, err
	}
	for n := 0; n < d.cfg.KeyShards; n++ {
		if err := consume(d.lane(kind, n), 1); err != nil {
			return nil, err
		}
	}
	d.mu.Lock()
	d.consumers[kind] = ch
	d.mu.Unlock()
	return ch, nil
}

func attemptOf(raw amqp.Delivery) int {
	a := 1
	if v, ok := raw.Headers["jobqueue-attempt"]; ok {
		switch n := v.(type) {
		case int32:
			a = int(n)
		case int64:
			a = int(n)
		case string:
			a, _ = strconv.Atoi(n)
		}
	}
	if v, ok := raw.Headers["x-delivery-count"]; ok { // quorum queues: earlier deliveries of this message
		switch n := v.(type) {
		case int64:
			a += int(n)
		case int32:
			a += int(n)
		}
	}
	return a
}

// Receive waits up to Poll for one job of kind.
func (d *Driver) Receive(ctx context.Context, kind string) (jobqueue.Delivery, error) {
	ds, err := d.ReceiveBatch(ctx, kind, 1)
	if len(ds) == 0 {
		return nil, err
	}
	return ds[0], err
}

// ReceiveBatch waits up to Poll for a job of kind, then takes whatever else is ready, up to max.
func (d *Driver) ReceiveBatch(ctx context.Context, kind string, max int) ([]jobqueue.Delivery, error) {
	ch, err := d.deliveries(kind)
	if err != nil {
		return nil, err
	}
	t := time.NewTimer(d.cfg.Poll)
	defer t.Stop()
	var outList []jobqueue.Delivery
	select {
	case x := <-ch:
		outList = append(outList, x)
	case <-t.C:
		return nil, nil
	case <-ctx.Done():
		return nil, nil
	}
	for len(outList) < max {
		select {
		case x := <-ch:
			outList = append(outList, x)
		default:
			return outList, nil
		}
	}
	return outList, nil
}

// DeadLetter publishes m to "<prefix>.dead" with the attempts and the error as headers.
func (d *Driver) DeadLetter(ctx context.Context, m jobqueue.Message, attempts int, cause error) error {
	name := d.cfg.Prefix + ".dead"
	if err := d.declare(name, quorum(false)); err != nil {
		return err
	}
	e := ""
	if cause != nil {
		e = cause.Error()
		if len(e) > 1000 {
			e = e[:1000]
		}
	}
	return d.publish(ctx, []out{{queue: name, m: m, hdr: amqp.Table{"jobqueue-attempt": int32(attempts),
		"jobqueue-error": e, "jobqueue-kind": m.Kind}}})
}

// Close closes the driver's channels (not the connection).
func (d *Driver) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	var errs []error
	for _, c := range d.chans {
		errs = append(errs, c.Close())
	}
	errs = append(errs, d.pub.Close())
	return errors.Join(errs...)
}

type delivery struct {
	d       *Driver
	raw     amqp.Delivery
	m       jobqueue.Message
	attempt int
}

func (x *delivery) Message() jobqueue.Message    { return x.m }
func (x *delivery) Attempt() int                 { return x.attempt }
func (x *delivery) Ack(context.Context) error    { return x.raw.Ack(false) }
func (x *delivery) Extend(context.Context) error { return nil }

// Retry puts it back now (0: at the head of its queue), or through a wait queue with the next attempt.
func (x *delivery) Retry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return x.raw.Nack(false, true)
	}
	o, err := x.d.outFor(x.m, x.attempt+1, delay)
	if err != nil {
		return err
	}
	o.queue, err = x.d.waitQueue(x.raw.RoutingKey, delay)
	if err != nil {
		return err
	}
	if err := x.d.publish(ctx, []out{o}); err != nil {
		return err
	}
	return x.raw.Ack(false)
}
