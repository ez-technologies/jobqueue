// Command throughput measures how many empty jobs a driver publishes (in batches) and runs (one worker process, the
// given concurrency and fetch batch) per second, against local servers (scripts/test-env.sh).
//
//	go run ./examples/throughput -driver nats -n 20000
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"

	"github.com/ez-technologies/jobqueue"
	"github.com/ez-technologies/jobqueue/kafkaq"
	"github.com/ez-technologies/jobqueue/memq"
	"github.com/ez-technologies/jobqueue/natsq"
	"github.com/ez-technologies/jobqueue/rabbitq"
	"github.com/ez-technologies/jobqueue/redisq"
	"github.com/ez-technologies/jobqueue/sqsq"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func must[T any](v T, err error) T {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return v
}

func main() {
	driver := flag.String("driver", "memory", "memory, nats, redis, rabbitmq, kafka or sqs")
	n := flag.Int("n", 20000, "jobs")
	batch := flag.Int("batch", 100, "publish and fetch batch size")
	conc := flag.Int("concurrency", 32, "jobs running at once")
	flag.Parse()
	ctx := context.Background()
	prefix := fmt.Sprintf("tp%d", time.Now().Unix())

	var d jobqueue.Driver
	switch *driver {
	case "memory":
		d = memq.New()
	case "nats":
		nc := must(nats.Connect(env("JOBQUEUE_TEST_NATS_URL", "nats://127.0.0.1:4231")))
		js := must(jetstream.New(nc))
		d = must(natsq.New(ctx, js, natsq.Config{Prefix: prefix, Poll: 200 * time.Millisecond}))
	case "redis":
		d = redisq.New(redis.NewClient(&redis.Options{Addr: env("JOBQUEUE_TEST_REDIS_ADDR", "127.0.0.1:6393")}),
			redisq.Config{Prefix: prefix, Poll: 200 * time.Millisecond})
	case "rabbitmq":
		conn := must(amqp.Dial(env("JOBQUEUE_TEST_RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5673/")))
		d = must(rabbitq.New(conn, rabbitq.Config{Prefix: prefix, Prefetch: 200, Poll: 200 * time.Millisecond}))
	case "kafka":
		d = must(kafkaq.New(kafkaq.Config{Brokers: strings.Split(env("JOBQUEUE_TEST_KAFKA_BROKERS", "127.0.0.1:19093"), ","),
			Prefix: prefix, CreateTopics: true, Partitions: 6, Poll: 200 * time.Millisecond}))
	case "sqs":
		c := sqs.New(sqs.Options{Region: "us-east-1", BaseEndpoint: aws.String(env("JOBQUEUE_TEST_SQS_ENDPOINT", "http://127.0.0.1:9331")),
			Credentials: credentials.NewStaticCredentialsProvider("local", "local", "")})
		d = sqsq.New(c, sqsq.Config{Prefix: prefix, Wait: time.Second, CreateQueues: true})
	default:
		fmt.Fprintln(os.Stderr, "unknown driver", *driver)
		os.Exit(2)
	}
	q := jobqueue.New(d)
	kind := "bench.noop"

	start := time.Now()
	for i := 0; i < *n; i += *batch {
		var pubs []jobqueue.Pub
		for j := i; j < min(i+*batch, *n); j++ {
			pubs = append(pubs, jobqueue.Pub{Kind: kind, ID: fmt.Sprint(j), Payload: map[string]int{"n": j}})
		}
		if err := q.PublishBatch(ctx, pubs); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	pub := time.Since(start)

	var done atomic.Int64
	wctx, stop := context.WithCancel(ctx)
	start = time.Now()
	go func() {
		_ = q.Work(wctx, map[string]jobqueue.Handler{kind: func(context.Context, jobqueue.Job) error {
			if done.Add(1) == int64(*n) {
				stop()
			}
			return nil
		}}, jobqueue.WorkOptions{Concurrency: *conc, BatchSize: *batch, Logger: slog.New(slog.DiscardHandler)})
	}()
	<-wctx.Done()
	run := time.Since(start)
	fmt.Printf("%-9s %6d jobs  publish %8.0f/s  run %8.0f/s  (batch %d, concurrency %d)\n", *driver, *n,
		float64(*n)/pub.Seconds(), float64(*n)/run.Seconds(), *batch, *conc)
	_ = d.Close()
}
