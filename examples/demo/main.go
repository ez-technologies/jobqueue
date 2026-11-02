// Command demo shows jobqueue at work: a job that fails once and is retried, a delayed job, a repeating job, a job
// that fails for good and goes to the dead letters, and a graceful stop that releases a running job.
//
//	go run ./examples/demo                              # in memory
//	go run ./examples/demo -nats nats://127.0.0.1:4222  # on a NATS server (2.12 or later, JetStream on)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ez-technologies/jobqueue"
	"github.com/ez-technologies/jobqueue/memq"
	"github.com/ez-technologies/jobqueue/natsq"
)

var start = time.Now()

func say(format string, args ...any) {
	fmt.Printf("%6.1fs  %s\n", time.Since(start).Seconds(), fmt.Sprintf(format, args...))
}

func main() {
	natsURL := flag.String("nats", "", "NATS server URL (empty: in memory)")
	flag.Parse()
	ctx := context.Background()

	var d jobqueue.Driver = memq.New()
	where := "in memory"
	if *natsURL != "" {
		nc, err := nats.Connect(*natsURL)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer nc.Close()
		js, _ := jetstream.New(nc)
		_ = js.DeleteStream(ctx, "DEMO") // a fresh start for the demo
		if d, err = natsq.New(ctx, js, natsq.Config{Prefix: "demo", Poll: 300 * time.Millisecond}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		where = "on NATS JetStream"
	}
	q := jobqueue.New(d)
	say("queue %s", where)

	var emails atomic.Int32
	handlers := map[string]jobqueue.Handler{
		"email.send": func(ctx context.Context, j jobqueue.Job) error {
			var p struct{ To string }
			_ = j.Decode(&p)
			if emails.Add(1) == 1 {
				return errors.New("mail server not answering")
			}
			say("  email.send     sent to %s (attempt %d)", p.To, j.Attempt)
			return nil
		},
		"report.build": func(ctx context.Context, j jobqueue.Job) error {
			say("  report.build   built, as scheduled")
			return nil
		},
		"cache.cleanup": func(ctx context.Context, j jobqueue.Job) error {
			say("  cache.cleanup  run %s", j.ID)
			return nil
		},
		"image.resize": func(ctx context.Context, j jobqueue.Job) error {
			return jobqueue.Permanent(errors.New("not an image"))
		},
		"video.encode": func(ctx context.Context, j jobqueue.Job) error {
			say("  video.encode   started (takes a minute)")
			<-ctx.Done() // the worker is stopping: give up, the job goes back to the queue
			return ctx.Err()
		},
	}

	must := func(err error) {
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	must(q.Publish(ctx, "email.send", "welcome-42", map[string]string{"to": "user@example.org"}))
	must(q.Publish(ctx, "report.build", "monthly", nil, jobqueue.After(3*time.Second)))
	must(q.Publish(ctx, "cache.cleanup", "tidy", nil, jobqueue.Every(2*time.Second)))
	must(q.Publish(ctx, "image.resize", "upload-7", nil))
	must(q.Publish(ctx, "video.encode", "clip-3", nil))
	say("published 5 jobs; report.build in 3 s, cache.cleanup every 2 s")

	work, stop := context.WithCancel(ctx)
	time.AfterFunc(5500*time.Millisecond, func() {
		say("SIGTERM: stop taking jobs, give running ones 1 s")
		stop()
	})
	_ = q.Work(work, handlers, jobqueue.WorkOptions{
		StopGrace: time.Second,
		Backoff:   func(int) time.Duration { return time.Second },
		Logger:    slog.New(slog.DiscardHandler),
		OnDone: func(j jobqueue.Job, outcome string, err error, took time.Duration) {
			switch outcome {
			case "retry":
				say("  %-14s failed: %v; retry in 1 s", j.Kind, err)
			case "dead":
				say("  %-14s failed for good: %v; kept in the dead letters", j.Kind, err)
			case "released":
				say("  %-14s released for another worker", j.Kind)
			}
		},
	})
	say("stopped")
	if m, ok := d.(*memq.Driver); ok {
		for _, dl := range m.DeadLetters() {
			say("dead letter: %s %s after %d attempt(s): %s", dl.Message.Kind, dl.Message.ID, dl.Attempts, dl.Error)
		}
	}
}
