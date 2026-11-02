// Package pubsubq is the Google Cloud Pub/Sub adapter of jobqueue: a Driver with one topic per kind
// ("<prefix>-<kind>") and one pull subscription shared by the workers ("<prefix>-<kind>-workers"). Failed jobs go to
// the topic "<prefix>-dead", kept by its subscription "<prefix>-dead-keep" (7 days).
//
// Pub/Sub has no delayed delivery: give the Queue a Scheduler for delays and repeats. A retry's back-off is the
// message's ack deadline (at most 10 minutes). Pub/Sub may deliver a message twice: make handlers idempotent.
package pubsubq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"time"

	pubsub "cloud.google.com/go/pubsub/v2/apiv1"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/ez-technologies/jobqueue"
)

// Config of the driver.
type Config struct {
	// Project is the Google Cloud project id (required).
	Project string
	// Prefix of the topic and subscription names (default "jobs").
	Prefix string
	// Poll is how long Receive waits for a message (default 5s).
	Poll time.Duration
	// AckDeadline is how long a received job stays hidden without an Extend (default 60s; 10 to 600).
	AckDeadline time.Duration
	// CreateResources makes missing topics and subscriptions (needs pubsub.topics.create and
	// pubsub.subscriptions.create). When false they must exist.
	CreateResources bool
}

// Driver is the Pub/Sub driver.
type Driver struct {
	pub *pubsub.TopicAdminClient
	sub *pubsub.SubscriptionAdminClient
	cfg Config

	mu    sync.Mutex
	ready map[string]bool
	seen  map[string]int
}

var _ jobqueue.Driver = (*Driver)(nil)

// New connects with the default credentials (or the emulator named by PUBSUB_EMULATOR_HOST) plus opts.
func New(ctx context.Context, cfg Config, opts ...option.ClientOption) (*Driver, error) {
	if cfg.Project == "" {
		return nil, errors.New("pubsubq: Project is required")
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "jobs"
	}
	if cfg.Poll <= 0 {
		cfg.Poll = 5 * time.Second
	}
	if cfg.AckDeadline < 10*time.Second || cfg.AckDeadline > 600*time.Second {
		cfg.AckDeadline = time.Minute
	}
	pub, err := pubsub.NewTopicAdminClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	sub, err := pubsub.NewSubscriptionAdminClient(ctx, opts...)
	if err != nil {
		_ = pub.Close()
		return nil, err
	}
	return &Driver{pub: pub, sub: sub, cfg: cfg, ready: map[string]bool{}}, nil
}

var unsafe = regexp.MustCompile(`[^A-Za-z0-9_.~+%-]`)

func (d *Driver) name(kind string) string { return unsafe.ReplaceAllString(d.cfg.Prefix+"-"+kind, "_") }

func (d *Driver) topic(kind string) string {
	return "projects/" + d.cfg.Project + "/topics/" + d.name(kind)
}

func (d *Driver) subscription(kind string) string {
	return "projects/" + d.cfg.Project + "/subscriptions/" + d.name(kind) + "-workers"
}

func (d *Driver) deadTopic() string {
	return "projects/" + d.cfg.Project + "/topics/" + d.cfg.Prefix + "-dead"
}

func exists(err error) bool { return status.Code(err) == codes.AlreadyExists }

// prepare makes the kind's topic and subscription once (when CreateResources).
func (d *Driver) prepare(ctx context.Context, kind string) error {
	d.mu.Lock()
	ok := d.ready[kind]
	d.mu.Unlock()
	if ok || !d.cfg.CreateResources {
		return nil
	}
	if _, err := d.pub.CreateTopic(ctx, &pubsubpb.Topic{Name: d.topic(kind)}); err != nil && !exists(err) {
		return fmt.Errorf("pubsubq: topic: %w", err)
	}
	_, err := d.sub.CreateSubscription(ctx, &pubsubpb.Subscription{Name: d.subscription(kind), Topic: d.topic(kind),
		AckDeadlineSeconds: int32(d.cfg.AckDeadline / time.Second)})
	if err != nil && !exists(err) {
		return fmt.Errorf("pubsubq: subscription: %w", err)
	}
	d.mu.Lock()
	d.ready[kind] = true
	d.mu.Unlock()
	return nil
}

func (d *Driver) prepareDead(ctx context.Context) error {
	d.mu.Lock()
	ok := d.ready[" dead"]
	d.mu.Unlock()
	if ok || !d.cfg.CreateResources {
		return nil
	}
	if _, err := d.pub.CreateTopic(ctx, &pubsubpb.Topic{Name: d.deadTopic()}); err != nil && !exists(err) {
		return err
	}
	_, err := d.sub.CreateSubscription(ctx, &pubsubpb.Subscription{Topic: d.deadTopic(),
		Name:                     "projects/" + d.cfg.Project + "/subscriptions/" + d.cfg.Prefix + "-dead-keep",
		MessageRetentionDuration: durationpb.New(7 * 24 * time.Hour)})
	if err != nil && !exists(err) {
		return err
	}
	d.mu.Lock()
	d.ready[" dead"] = true
	d.mu.Unlock()
	return nil
}

// MaxDelay is 0: Pub/Sub holds no delay.
func (d *Driver) MaxDelay() time.Duration { return 0 }

// Send publishes m (delay must be 0).
func (d *Driver) Send(ctx context.Context, m jobqueue.Message, delay time.Duration) error {
	if delay > 0 {
		return errors.New("pubsubq: Pub/Sub holds no delay; give the Queue a Scheduler")
	}
	if err := d.prepare(ctx, m.Kind); err != nil {
		return err
	}
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	_, err = d.pub.Publish(ctx, &pubsubpb.PublishRequest{Topic: d.topic(m.Kind),
		Messages: []*pubsubpb.PubsubMessage{{Data: data, Attributes: map[string]string{"job_id": m.ID}}}})
	return err
}

// Receive pulls one message of kind, waiting up to Poll.
func (d *Driver) Receive(ctx context.Context, kind string) (jobqueue.Delivery, error) {
	if err := d.prepare(ctx, kind); err != nil {
		return nil, err
	}
	pctx, cancel := context.WithTimeout(ctx, d.cfg.Poll)
	defer cancel()
	out, err := d.sub.Pull(pctx, &pubsubpb.PullRequest{Subscription: d.subscription(kind), MaxMessages: 1})
	if err != nil {
		if pctx.Err() != nil || status.Code(err) == codes.DeadlineExceeded || status.Code(err) == codes.Canceled {
			return nil, nil
		}
		return nil, err
	}
	if len(out.ReceivedMessages) == 0 {
		return nil, nil
	}
	rm := out.ReceivedMessages[0]
	var m jobqueue.Message
	if err := json.Unmarshal(rm.Message.GetData(), &m); err != nil || m.Kind == "" {
		_ = d.sub.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: d.subscription(kind), AckIds: []string{rm.AckId}})
		return nil, errors.New("pubsubq: dropped a message that is not a jobqueue message")
	}
	attempt := int(rm.GetDeliveryAttempt()) // set when the subscription has a dead-letter policy
	if attempt == 0 {
		attempt = d.countAttempt(m.ID)
	}
	return &delivery{d: d, sub: d.subscription(kind), ackID: rm.AckId, m: m, attempt: attempt}, nil
}

// countAttempt counts deliveries in this process when Pub/Sub does not (no dead-letter policy on the subscription).
// A job retried on another worker starts again at 1 there.
func (d *Driver) countAttempt(id string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen == nil {
		d.seen = map[string]int{}
	}
	if len(d.seen) > 10000 {
		d.seen = map[string]int{}
	}
	d.seen[id]++
	return d.seen[id]
}

// DeadLetter publishes m on "<prefix>-dead" with kind, attempts and error attributes.
func (d *Driver) DeadLetter(ctx context.Context, m jobqueue.Message, attempts int, cause error) error {
	if err := d.prepareDead(ctx); err != nil {
		return err
	}
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	e := ""
	if cause != nil {
		e = cause.Error()
		if len(e) > 1000 {
			e = e[:1000]
		}
	}
	_, err = d.pub.Publish(ctx, &pubsubpb.PublishRequest{Topic: d.deadTopic(), Messages: []*pubsubpb.PubsubMessage{{
		Data: data, Attributes: map[string]string{"kind": m.Kind, "job_id": m.ID, "attempts": strconv.Itoa(attempts), "error": e}}}})
	return err
}

// Close closes the clients.
func (d *Driver) Close() error { return errors.Join(d.pub.Close(), d.sub.Close()) }

type delivery struct {
	d       *Driver
	sub     string
	ackID   string
	m       jobqueue.Message
	attempt int
}

func (x *delivery) Message() jobqueue.Message { return x.m }
func (x *delivery) Attempt() int              { return x.attempt }

func (x *delivery) Ack(ctx context.Context) error {
	err := x.d.sub.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: x.sub, AckIds: []string{x.ackID}})
	if err == nil {
		x.d.mu.Lock()
		delete(x.d.seen, x.m.ID)
		x.d.mu.Unlock()
	}
	return err
}

func (x *delivery) deadline(ctx context.Context, d time.Duration) error {
	s := int32((d + time.Second - 1) / time.Second)
	if s > 600 {
		s = 600
	}
	err := x.d.sub.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{Subscription: x.sub,
		AckIds: []string{x.ackID}, AckDeadlineSeconds: s})
	return err
}

// Retry sets the ack deadline to delay (0 redelivers now; at most 10 minutes).
func (x *delivery) Retry(ctx context.Context, delay time.Duration) error {
	return x.deadline(ctx, delay)
}

// Extend sets the ack deadline to another AckDeadline.
func (x *delivery) Extend(ctx context.Context) error { return x.deadline(ctx, x.d.cfg.AckDeadline) }
