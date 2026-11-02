// Package sqsq is the AWS SQS adapter of jobqueue: a Driver over standard queues, one queue per kind
// ("<prefix>-<kind>"), and one dead-letter queue per kind ("<prefix>-<kind>-dead") wired as the queue's redrive target
// for messages that keep crashing workers.
//
// SQS holds delays up to 15 minutes; give the Queue a Scheduler for longer ones. Standard queues may deliver a message
// twice and do not de-duplicate by id: make handlers idempotent (jobqueue.Once).
package sqsq

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/ez-technologies/jobqueue"
)

// MaxDelay is the longest delay SQS holds.
const MaxDelay = 15 * time.Minute

// Config of the driver.
type Config struct {
	// Prefix of the queue names (default "jobs").
	Prefix string
	// Wait is the long-poll time of Receive (default 5s, at most 20s).
	Wait time.Duration
	// Visibility is how long a received job stays hidden without an Extend (default 60s).
	Visibility time.Duration
	// CreateQueues makes missing queues (needs sqs:CreateQueue). When false the queues must exist.
	CreateQueues bool
	// FIFO uses FIFO queues ("<name>.fifo"): a job's Key is its MessageGroupId, so jobs with one key run one at a
	// time in order across all workers, and a job id is de-duplicated for 5 minutes. FIFO queues hold no per-message
	// delay: delays go through the Scheduler.
	FIFO bool
	// MaxReceives before SQS moves a message to the dead-letter queue by itself (a worker that crashes on it every
	// time; default 50; the Queue's own MaxAttempts normally acts first).
	MaxReceives int
}

// Driver is the SQS driver.
type Driver struct {
	c   *sqs.Client
	cfg Config

	mu   sync.Mutex
	urls map[string]string
}

var (
	_ jobqueue.Driver        = (*Driver)(nil)
	_ jobqueue.BatchSender   = (*Driver)(nil)
	_ jobqueue.BatchReceiver = (*Driver)(nil)
	_ jobqueue.Orderer       = (*Driver)(nil)
)

// OrdersByKey is true for FIFO queues.
func (d *Driver) OrdersByKey() bool { return d.cfg.FIFO }

// New returns a driver over an SQS client (its region, credentials and endpoint are the caller's).
func New(c *sqs.Client, cfg Config) *Driver {
	if cfg.Prefix == "" {
		cfg.Prefix = "jobs"
	}
	if cfg.Wait <= 0 {
		cfg.Wait = 5 * time.Second
	}
	if cfg.Wait > 20*time.Second {
		cfg.Wait = 20 * time.Second
	}
	if cfg.Visibility <= 0 {
		cfg.Visibility = time.Minute
	}
	if cfg.MaxReceives <= 0 {
		cfg.MaxReceives = 50
	}
	return &Driver{c: c, cfg: cfg, urls: map[string]string{}}
}

var unsafe = regexp.MustCompile(`[^A-Za-z0-9_-]`)

// QueueName is the queue of kind (dots become '_'; SQS allows 80 characters).
func (d *Driver) QueueName(kind string) string {
	n := unsafe.ReplaceAllString(d.cfg.Prefix+"-"+kind, "_")
	if len(n) > 69 {
		n = n[:69]
	}
	return n
}

// fifo adds the FIFO suffix and attribute when the driver uses FIFO queues.
func (d *Driver) fifo(name string, attrs map[string]string) (string, map[string]string) {
	if !d.cfg.FIFO {
		return name, attrs
	}
	if attrs == nil {
		attrs = map[string]string{}
	}
	attrs["FifoQueue"] = "true"
	return name + ".fifo", attrs
}

func (d *Driver) url(ctx context.Context, name string, attrs map[string]string) (string, error) {
	d.mu.Lock()
	u, ok := d.urls[name]
	d.mu.Unlock()
	if ok {
		return u, nil
	}
	if d.cfg.CreateQueues {
		out, err := d.c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attrs})
		if err != nil {
			return "", fmt.Errorf("sqsq: create %s: %w", name, err)
		}
		u = aws.ToString(out.QueueUrl)
	} else {
		out, err := d.c.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
		if err != nil {
			return "", fmt.Errorf("sqsq: queue %s: %w", name, err)
		}
		u = aws.ToString(out.QueueUrl)
	}
	d.mu.Lock()
	d.urls[name] = u
	d.mu.Unlock()
	return u, nil
}

func (d *Driver) deadURL(ctx context.Context, kind string) (string, error) {
	name, attrs := d.fifo(d.QueueName(kind)+"-dead", map[string]string{"MessageRetentionPeriod": "1209600"})
	return d.url(ctx, name, attrs)
}

func (d *Driver) queueURL(ctx context.Context, kind string) (string, error) {
	name := d.QueueName(kind)
	var attrs map[string]string
	if d.cfg.CreateQueues {
		dead, err := d.deadURL(ctx, kind)
		if err != nil {
			return "", err
		}
		a, err := d.c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(dead),
			AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
		if err != nil {
			return "", err
		}
		policy, _ := json.Marshal(map[string]string{"deadLetterTargetArn": a.Attributes["QueueArn"],
			"maxReceiveCount": strconv.Itoa(d.cfg.MaxReceives)})
		attrs = map[string]string{"RedrivePolicy": string(policy),
			"VisibilityTimeout": strconv.Itoa(int(d.cfg.Visibility.Seconds()))}
	}
	name, attrs = d.fifo(name, attrs)
	return d.url(ctx, name, attrs)
}

// MaxDelay is 15 minutes (0 for FIFO queues).
func (d *Driver) MaxDelay() time.Duration {
	if d.cfg.FIFO {
		return 0
	}
	return MaxDelay
}

// group and dedup are a FIFO message's group (its key; else its own, i.e. no order) and de-duplication id.
func group(m jobqueue.Message) string {
	if m.Key != "" {
		return clip(m.Key)
	}
	return clip(m.Kind + ":" + m.ID)
}

func dedup(m jobqueue.Message) string {
	sum := sha256.Sum256([]byte(m.Kind + "\x00" + m.ID))
	return hex.EncodeToString(sum[:])
}

func clip(s string) string {
	if len(s) > 128 {
		sum := sha256.Sum256([]byte(s))
		return hex.EncodeToString(sum[:])
	}
	return s
}

func (d *Driver) input(m jobqueue.Message, delay time.Duration) (string, *int32, *string, *string, error) {
	body, err := json.Marshal(m)
	if err != nil {
		return "", nil, nil, nil, err
	}
	if d.cfg.FIFO {
		if delay > 0 {
			return "", nil, nil, nil, errors.New("sqsq: FIFO queues hold no per-message delay; give the Queue a Scheduler")
		}
		return string(body), nil, aws.String(group(m)), aws.String(dedup(m)), nil
	}
	return string(body), aws.Int32(int32((delay + time.Second - 1) / time.Second)), nil, nil, nil
}

// SendBatch sends undelayed messages ten at a time per queue.
func (d *Driver) SendBatch(ctx context.Context, ms []jobqueue.Message) error {
	byKind := map[string][]types.SendMessageBatchRequestEntry{}
	var order []string
	for i, m := range ms {
		body, delay, grp, dd, err := d.input(m, 0)
		if err != nil {
			return err
		}
		e := types.SendMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(i)), MessageBody: aws.String(body),
			MessageGroupId: grp, MessageDeduplicationId: dd}
		if delay != nil {
			e.DelaySeconds = *delay
		}
		if _, ok := byKind[m.Kind]; !ok {
			order = append(order, m.Kind)
		}
		byKind[m.Kind] = append(byKind[m.Kind], e)
	}
	for _, kind := range order {
		u, err := d.queueURL(ctx, kind)
		if err != nil {
			return err
		}
		entries := byKind[kind]
		for len(entries) > 0 {
			n := min(10, len(entries))
			out, err := d.c.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: aws.String(u), Entries: entries[:n]})
			if err != nil {
				return err
			}
			if len(out.Failed) > 0 {
				return fmt.Errorf("sqsq: %d of a batch failed: %s", len(out.Failed), aws.ToString(out.Failed[0].Message))
			}
			entries = entries[n:]
		}
	}
	return nil
}

// Send sends m after delay (rounded up to the second).
func (d *Driver) Send(ctx context.Context, m jobqueue.Message, delay time.Duration) error {
	if delay > MaxDelay {
		return fmt.Errorf("sqsq: delay %s over %s", delay, MaxDelay)
	}
	u, err := d.queueURL(ctx, m.Kind)
	if err != nil {
		return err
	}
	body, del, grp, dd, err := d.input(m, delay)
	if err != nil {
		return err
	}
	in := &sqs.SendMessageInput{QueueUrl: aws.String(u), MessageBody: aws.String(body), MessageGroupId: grp,
		MessageDeduplicationId: dd}
	if del != nil {
		in.DelaySeconds = *del
	}
	_, err = d.c.SendMessage(ctx, in)
	return err
}

// Receive long-polls the kind's queue for one message.
func (d *Driver) Receive(ctx context.Context, kind string) (jobqueue.Delivery, error) {
	ds, err := d.ReceiveBatch(ctx, kind, 1)
	if len(ds) == 0 {
		return nil, err
	}
	return ds[0], err
}

// ReceiveBatch long-polls for up to max (at most 10) messages.
func (d *Driver) ReceiveBatch(ctx context.Context, kind string, max int) ([]jobqueue.Delivery, error) {
	u, err := d.queueURL(ctx, kind)
	if err != nil {
		return nil, err
	}
	out, err := d.c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(u), MaxNumberOfMessages: int32(clamp(max)),
		WaitTimeSeconds:             int32(d.cfg.Wait / time.Second),
		VisibilityTimeout:           int32(d.cfg.Visibility / time.Second),
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount}})
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil
		}
		return nil, err
	}
	var ds []jobqueue.Delivery
	for _, msg := range out.Messages {
		var m jobqueue.Message
		if err := json.Unmarshal([]byte(aws.ToString(msg.Body)), &m); err != nil || m.Kind == "" {
			continue // not ours: left to the redrive policy
		}
		attempt, _ := strconv.Atoi(msg.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
		ds = append(ds, &delivery{d: d, url: u, handle: aws.ToString(msg.ReceiptHandle), m: m, attempt: attempt})
	}
	return ds, nil
}

func clamp(n int) int { return min(10, max(n, 1)) }

// DeadLetter sends m to the kind's dead-letter queue with the attempts and the error as attributes.
func (d *Driver) DeadLetter(ctx context.Context, m jobqueue.Message, attempts int, cause error) error {
	u, err := d.deadURL(ctx, m.Kind)
	if err != nil {
		return err
	}
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	e := "unknown"
	if cause != nil {
		e = cause.Error()
		if len(e) > 1000 {
			e = e[:1000]
		}
	}
	var grp, dd *string
	if d.cfg.FIFO {
		grp, dd = aws.String(group(m)), aws.String(dedup(m))
	}
	_, err = d.c.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(u), MessageBody: aws.String(string(body)),
		MessageGroupId: grp, MessageDeduplicationId: dd,
		MessageAttributes: map[string]types.MessageAttributeValue{
			"attempts": {DataType: aws.String("Number"), StringValue: aws.String(strconv.Itoa(attempts))},
			"error":    {DataType: aws.String("String"), StringValue: aws.String(e)},
		}})
	return err
}

// Close does nothing: the client is the caller's.
func (d *Driver) Close() error { return nil }

type delivery struct {
	d       *Driver
	url     string
	handle  string
	m       jobqueue.Message
	attempt int
}

func (x *delivery) Message() jobqueue.Message { return x.m }
func (x *delivery) Attempt() int              { return x.attempt }

func (x *delivery) Ack(ctx context.Context) error {
	_, err := x.d.c.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(x.url), ReceiptHandle: aws.String(x.handle)})
	return err
}

func (x *delivery) visible(ctx context.Context, after time.Duration) error {
	s := int32((after + time.Second - 1) / time.Second)
	if s > 43200 {
		s = 43200
	}
	_, err := x.d.c.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: aws.String(x.url),
		ReceiptHandle: aws.String(x.handle), VisibilityTimeout: s})
	return err
}

// Retry makes the message visible again after delay (up to 12 hours).
func (x *delivery) Retry(ctx context.Context, delay time.Duration) error {
	return x.visible(ctx, delay)
}

// Extend hides it for another Visibility.
func (x *delivery) Extend(ctx context.Context) error { return x.visible(ctx, x.d.cfg.Visibility) }
