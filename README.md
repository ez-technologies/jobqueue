# jobqueue

**Background jobs for Go that run the same way on a laptop, on AWS and on Google Cloud.**

- One small interface: publish a job (a kind, an id, a JSON payload); workers run the handler for its kind.
- A worker runtime: acknowledge only after the handler finished, retries with back-off, a dead-letter destination,
  delayed and repeating jobs, panics caught, and a **graceful stop on SIGTERM** within a deadline (spot and preemptible
  machines get about 30 seconds).
- One adapter per platform, each in its own package, so your binary links only the SDK it uses:
  **NATS JetStream**, **Kafka** (Apache Kafka, AWS MSK, Confluent, Redpanda), **RabbitMQ** (Amazon MQ, CloudAMQP),
  **Redis / Valkey Streams**, **AWS SQS**, **Google Cloud Pub/Sub**, and **in memory** for tests.
- **No head-of-line blocking**: a failed job is re-published with its retry details and the original acknowledged at
  once; every job has a timeout; slow work runs in **lanes** of its own.
- Unordered by default, for throughput; an optional **ordering key** runs a key's jobs one at a time in publish order;
  a key that keeps failing is **parked** (only that key waits).
- **Skip what is already covered**: a retry hook and a ready-made supersede-by-version check.
- **Batches**: publish in batches, fetch in batches, acknowledge each job on its own; an optional batch handler.
- Delays a broker cannot hold go through a **scheduler loop**; one worker runs it at a time, chosen by a pluggable
  **lease**: PostgreSQL, NATS KV, DynamoDB or Firestore.
- An idempotency helper (`Once` with a done-log) so a job delivered twice does not send two emails.
- One **conformance suite** (`jobqueuetest`) that every adapter passes, run against local emulators.

![The demo on NATS JetStream: a retry, a delayed job, a repeating job, a dead letter and a graceful stop](docs/images/demo-nats.png)

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [Rules for the next developer](#rules-for-the-next-developer) (also [AGENTS.md](AGENTS.md))
- [Configuration](#configuration)
- [Choosing settings](#choosing-settings)
- [How to](#how-to)
  - [Retries, back-off and dead letters](#retries-back-off-and-dead-letters)
  - [Delayed and repeating jobs](#delayed-and-repeating-jobs)
  - [Delays the broker cannot hold: the scheduler](#delays-the-broker-cannot-hold-the-scheduler)
  - [Stop gracefully on SIGTERM (spot machines)](#stop-gracefully-on-sigterm-spot-machines)
  - [Do it once: idempotent handlers](#do-it-once-idempotent-handlers)
  - [Run only some kinds in a worker](#run-only-some-kinds-in-a-worker)
  - [Failures never hold up the rest](#failures-never-hold-up-the-rest)
  - [Lanes for slow work](#lanes-for-slow-work)
  - [Skip a job a newer one covers](#skip-a-job-a-newer-one-covers)
  - [Order jobs by key](#order-jobs-by-key)
  - [Batches](#batches)
  - [Test your handlers](#test-your-handlers)
  - [Write an adapter](#write-an-adapter)
- [Run it on: local / AWS / Google](#run-it-on-local--aws--google)
- [Options](#options)
- [Adapters at a glance](#adapters-at-a-glance)
- [Kafka: what is emulated](#kafka-what-is-emulated)
- [Throughput](#throughput)
- [Run the demo](#run-the-demo)
- [Development](#development)

## Install

Go 1.26 or later:

```sh
go get github.com/ez-technologies/jobqueue@latest
```

Import the root package and the adapters you use:

| Package | What | SDK it pulls in |
|---|---|---|
| `jobqueue` | the queue, the worker runtime, the scheduler, `Once` | none |
| `jobqueue/natsq` | NATS JetStream driver and a NATS KV lease | `nats.go` |
| `jobqueue/memq` | in-memory driver, store, lease and done-log | none |
| `jobqueue/kafkaq` | Kafka driver (Apache Kafka, MSK, Confluent, Redpanda) | `franz-go` |
| `jobqueue/rabbitq` | RabbitMQ driver (AMQP 0-9-1; Amazon MQ, CloudAMQP) | `amqp091-go` |
| `jobqueue/redisq` | Redis / Valkey Streams driver | `go-redis/v9` |
| `jobqueue/sqsq` | AWS SQS driver (standard or FIFO queues) | `aws-sdk-go-v2/service/sqs` |
| `jobqueue/pubsubq` | Google Cloud Pub/Sub driver | `cloud.google.com/go/pubsub/v2` |
| `jobqueue/pgq` | PostgreSQL scheduler store, lease and done-log (`database/sql`) | none (bring a driver) |
| `jobqueue/dynamolease` | DynamoDB lease | `aws-sdk-go-v2/service/dynamodb` |
| `jobqueue/firestorelease` | Firestore lease | `cloud.google.com/go/firestore` |
| `jobqueue/jobqueuetest` | the conformance suite | none |

## Quick start

```go
import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ez-technologies/jobqueue"
	"github.com/ez-technologies/jobqueue/natsq"
)

nc, _ := nats.Connect("nats://127.0.0.1:4222")
js, _ := jetstream.New(nc)
driver, err := natsq.New(ctx, js, natsq.Config{Prefix: "jobs"})
q := jobqueue.New(driver)

// In the app: publish.
err = q.Publish(ctx, "email.send", "welcome-42", map[string]string{"to": "user@example.org"})

// In the worker: handle until SIGTERM.
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
defer stop()
err = q.Work(ctx, map[string]jobqueue.Handler{
	"email.send": func(ctx context.Context, j jobqueue.Job) error {
		var p struct{ To string }
		if err := j.Decode(&p); err != nil {
			return jobqueue.Permanent(err) // bad input: no retries
		}
		return sendEmail(ctx, p.To) // an error retries the job
	},
}, jobqueue.WorkOptions{Concurrency: 4})
```

The same app on SQS or Pub/Sub changes only the driver line; see [Run it on](#run-it-on-local--aws--google).

## Rules for the next developer

Read these before changing a handler or a setting ([AGENTS.md](AGENTS.md) has the same list for coding agents):

1. **Handlers must be idempotent.** Every broker here delivers at least once. Use `Once` with a done-log for effects.
2. **Never assume order unless the jobs carry a key** (and the mode is keyed-ordered).
3. **Never do long work without a lane and a timeout.** A transcription in the lane of the pushes delays the pushes.
4. **Kafka commits lag behind the slowest running job of a partition** (see [Kafka](#kafka-what-is-emulated)).
5. **SQS holds at most 15 minutes of delay; Pub/Sub holds none.** Longer delays need a Scheduler.
6. **Ordered keys can stall.** A key that keeps failing is parked: watch the alerts (`OnParked`, the error log) and
   the dead letters, fix, then `Unpark`.
7. **Batch handlers must return one result per job**, in order. A missing one counts as a failure.
8. **Skipping is final.** Skip a job only when a newer job, or the current state, already covers its effect.
9. **Test a new adapter or store with the conformance suite** (`jobqueuetest`).

## Configuration

One struct per queue, `jobqueue.WorkOptions`, with good defaults (the zero value works):

| Field | Default | Meaning |
|---|---|---|
| `Mode` | keyed-ordered | `KeyedOrdered`: jobs with a key run one at a time per key, in publish order; others unordered. `Unordered`: keys ignored |
| `BatchSize` | 1 | jobs one fetch may take; each is still acknowledged on its own |
| `Concurrency` | 1 | jobs of each kind (route) running at once in this process: the most in flight |
| `Timeout` | 5 min | a job running longer has failed and is re-published (handlers must honour their ctx) |
| `MaxAttempts` | 5 | tries before the dead letters |
| `Backoff`, `Jitter` | 5 s, 30 s, 2 min, 10 min, 30 min; ±20 % | wait before the next attempt |
| `RetryRoute` | `Republish` | `Republish`: a copy with `RetryInfo`, delayed by the back-off (Kafka: retry-topic tiers), the original acknowledged at once. `Nack`: the broker redelivers the same message |
| dead-letter name | per driver | NATS `<STREAM>_DEAD`, SQS `<prefix>-<kind>-dead`, Pub/Sub `<prefix>-dead`, Kafka `<prefix>.dead`, RabbitMQ `<prefix>.dead`, Redis `<prefix>:dead` |
| `KeyPolicy`, `KeyAttempts` | `Park`, 3 | a keyed job that keeps failing: retried in place `KeyAttempts` times while its key waits, then its key is parked. `RetryInPlace`: up to `MaxAttempts`, then dead-lettered and the key goes on |
| `Parked`, `OnParked` | in this process; none | where parked keys are kept (`pgq.ParkStore` shares them); the alert |
| `Lanes`, `OnlyLanes` | none; all | lanes by name (kinds, concurrency, batch, timeout); the lanes this process runs (`"default"` is the rest) |
| `ShouldSkip`, `SkipFirstAttempt` | none; false | asked before a retry (and a first attempt): skip what is covered (`Supersede`) |
| `RetryHandlers` | none | per kind, run the re-published copy instead of the handler |
| `BatchHandlers` | none | per kind, take a whole batch, return a result per job |
| `StopGrace` | 25 s | on SIGTERM, how long running jobs may finish |
| `ExtendEvery` | 20 s | how often a running job is kept hidden from other workers |
| `Logger`, `OnDone` | `slog.Default()`, none | logs; one call per job: `done`, `retry`, `dead`, `parked`, `skipped`, `released` |

Per-backend extras are in the driver's `Config`: Kafka `Partitions`, `RetryTiers` (5 s, 1 min, 10 min); SQS `FIFO`,
`Visibility`; Pub/Sub `AckDeadline`, `Ordering`; NATS and RabbitMQ `KeyShards`; Redis `Visibility` (see [Options](#options)).

## Choosing settings

**Fast notifications** (pushes, chat messages): unordered, many at once, short timeout, skip what is covered.

```go
jobqueue.WorkOptions{Mode: jobqueue.Unordered, Concurrency: 32, BatchSize: 50, Timeout: 30 * time.Second,
	MaxAttempts: 4, ShouldSkip: coveredByNewerPushOrRead}
```

**Slow media jobs** (transcription, video): their own lane and workers, long timeout, few at a time.

```go
jobqueue.WorkOptions{Lanes: map[string]jobqueue.Lane{"media": {Kinds: []string{"transcribe", "video.encode"},
	Concurrency: 1, Timeout: 15 * time.Minute}}, OnlyLanes: []string{"media"}} // on the big machine
// and OnlyLanes: []string{"default"} on the small ones
```

**Strictly ordered updates per entity** (an account's ledger): keyed-ordered, a shared park store, an alert.

```go
q.Publish(ctx, "ledger.apply", evt.ID, evt, jobqueue.Key("account-"+id))
jobqueue.WorkOptions{KeyAttempts: 3, Parked: pgq.NewParkStore(db), OnParked: page}
```

**Very high throughput on Kafka**: many partitions, big batches, unordered unless needed, short timeouts (they bound
the commit lag), retry tiers that match your back-off.

```go
kafkaq.Config{Partitions: 48, RetryTiers: []time.Duration{5 * time.Second, time.Minute}}
jobqueue.WorkOptions{Mode: jobqueue.Unordered, BatchSize: 500, Concurrency: 256, Timeout: 20 * time.Second,
	Backoff: func(int) time.Duration { return 5 * time.Second }}
```

## How to

### Retries, back-off and dead letters

A handler that returns an error is tried again after `Backoff(attempt)` (default 5 s, 30 s, 2 min, 10 min, 30 min).
After `MaxAttempts` (default 5), or at once for an error wrapped in `jobqueue.Permanent`, the job goes to the
driver's dead letters (NATS: stream `<STREAM>_DEAD`; SQS: queue `<prefix>-<kind>-dead`; Pub/Sub: topic
`<prefix>-dead`; memq: `DeadLetters()`), with the attempts and the last error. A panic counts as an error.

```go
q.Work(ctx, handlers, jobqueue.WorkOptions{
	MaxAttempts: 8,
	Backoff:     func(n int) time.Duration { return time.Duration(n*n) * time.Second },
	OnDone: func(j jobqueue.Job, outcome string, err error, took time.Duration) {
		metrics.Count("jobs_"+outcome, j.Kind) // done, retry, dead, released
	},
})
```

### Delayed and repeating jobs

```go
q.Publish(ctx, "reminder.send", "invoice-17", p, jobqueue.After(2*time.Minute))
q.Publish(ctx, "report.build", "monthly-2026-11", nil, jobqueue.At(firstOfMonth))
q.Publish(ctx, "uploads.cleanup", "cleanup", nil, jobqueue.Every(time.Hour)) // again with the same id: replaced
```

Each run of a repeat reaches the handler with its own id, `cleanup@<unix seconds>` (`jobqueue.Occurrence`), so a
done-log keeps runs apart. NATS (2.12 or later) holds delays and repeats on the server, to the second. SQS holds
delays up to 15 minutes. Pub/Sub holds none. Anything a driver cannot hold needs a scheduler.

### Delays the broker cannot hold: the scheduler

```go
db, _ := sql.Open("pgx", os.Getenv("DATABASE_URL"))
_ = pgq.Migrate(ctx, db) // jobqueue_scheduled, jobqueue_leases, jobqueue_done

q := jobqueue.New(sqsDriver, jobqueue.WithScheduler(&jobqueue.Scheduler{
	Store:  pgq.NewStore(db),
	Lease:  pgq.NewLease(db), // or natsq.NewLease, dynamolease.New, firestorelease.New
	Holder: hostname + "-" + strconv.Itoa(os.Getpid()),
}))
```

`Publish` with a delay beyond the driver's `MaxDelay`, or a repeat the driver cannot hold, writes an entry to the
store. Every worker runs the scheduler loop inside `Work`; only the lease holder moves due entries to the queue, and
another worker takes over within the lease's TTL (default 30 s) when it dies.

### Stop gracefully on SIGTERM (spot machines)

`Work` returns when its context ends. It stops taking jobs at once, gives running ones `StopGrace` (default 25 s) to
finish, then cancels their context; whatever did not finish is released at once for another worker (and would be
redelivered anyway after the broker's visibility timeout). Long jobs are kept hidden from other workers by an
`Extend` every `ExtendEvery` (default 20 s).

```go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
defer stop()
_ = q.Work(ctx, handlers, jobqueue.WorkOptions{StopGrace: 25 * time.Second})
```

### Do it once: idempotent handlers

Every broker here delivers at least once. Record each effect in a done-log:

```go
done := pgq.NewDoneLog(db)
for _, user := range recipients {
	_, err := jobqueue.Once(ctx, done, job.Kind+":"+job.ID+":"+user, func(ctx context.Context) error {
		return push.Send(ctx, user, msg)
	})
	if err != nil {
		return err // the failed recipient is forgotten, so the retry sends to them (and only them)
	}
}
```

The key is recorded before `fn` runs: a process that dies inside it loses that one effect rather than doing it twice.
Prune old keys from a daily job: `done.Prune(ctx, 7*24*time.Hour)`.

### Run only some kinds in a worker

Each kind has its own consumer (NATS), queue (SQS) or subscription (Pub/Sub), shared by every worker that handles
it. Start a worker with the handlers it should run: a small machine for `push.*`, a big one for `video.encode`.

### Failures never hold up the rest

A failed job is **not retried in place**. It is published again as a copy carrying `RetryInfo` (attempt, where it
first came from: queue, topic/partition/offset or stream id; first-failure time; last error; next-attempt time) with
the back-off as its delay, and the original is acknowledged (Kafka: its offset marked done) at once. The copy waits in
the broker: a NATS schedule, SQS `DelaySeconds`, a Kafka retry-topic tier, a RabbitMQ wait queue, the Redis delayed
set, or the Scheduler. The jobs behind it run meanwhile. A job that runs past `Timeout` counts as failed.

Keyed jobs are the exception: re-publishing would let the key's next job overtake it (see below).

### Lanes for slow work

```go
q.Publish(ctx, "image.resize", id, p, jobqueue.InLane("slow")) // this one is big
q.Work(ctx, handlers, jobqueue.WorkOptions{
	Lanes:     map[string]jobqueue.Lane{"slow": {Kinds: []string{"transcribe"}, Concurrency: 1, Timeout: 15 * time.Minute}},
	OnlyLanes: []string{"slow"}, // this process runs the slow lane only
})
```

A lane has its own queue (topic, subscription, stream: `<kind>.lane-<name>` for jobs published into it; a kind listed
in `Kinds` keeps its own queue), its own workers and its own concurrency, batch size and timeout, so slow work never
shares a batch or a worker slot with fast work.

### Skip a job a newer one covers

Ask one question: **does a newer job, or the current state, already cover the effect of this one?**

- **Yes: skip it.** A notification about a conversation when a newer notification for the same person and
  conversation was sent, or the person already read past that message (opening the conversation shows everything);
  a summary or a recompute; an entity sync.
- **No: never skip.** Jobs where each one has its own effect that nothing later covers: a payment or a charge, a
  distinct email or letter, a one-time code, an audit record.

```go
versions := pgq.NewVersionStore(db) // or natsq, redisq, dynamolease, firestorelease, NewMemoryVersions
q := jobqueue.New(driver, jobqueue.WithVersions(versions))
q.Publish(ctx, "summary.recompute", id, p, jobqueue.Version("topic-7", eventID)) // the store keeps the newest per kind+entity
q.Work(ctx, handlers, jobqueue.WorkOptions{ShouldSkip: jobqueue.Supersede(versions)})
```

`Supersede` skips a retry whose version is lower than the newest one published for its entity, even while that newer
job is still running. Or pass your own `ShouldSkip` that reads your database. Scope the entity to what the effect
covers: for a push, person and conversation, not the person alone. **The version must grow with every change of its
entity, and a skip is final**: the job is acknowledged, logged with the reason and reported as `skipped`. By default
only retries are checked; `SkipFirstAttempt` checks first attempts too. `RetryHandlers` run a re-published copy with a
handler of its own (it gets the `RetryInfo`).

### Order jobs by key

There is **no global order**: delivery is at least once and unordered by default, which is what lets many workers
go fast. Make handlers idempotent. When a stream of jobs about one thing must not overtake each other, give them a
key:

```go
q.Publish(ctx, "account.apply", "evt-981", ev, jobqueue.Key("account-17"))
```

Jobs of a kind with the same key run one at a time, in publish order. **A failing keyed job blocks only its own key**,
never the batch or other keys: it is retried in place `KeyAttempts` times (3) while its key waits, then, by default,
its key is **parked**: the job and the key's following jobs go to the dead letters, in order, an error is logged and
`OnParked` called, until `Unpark`. With `KeyPolicy: RetryInPlace` it is retried up to `MaxAttempts`, then
dead-lettered and the key goes on (that job is then out of order). Choose deliberately, and watch parked keys. How each
driver keeps the order:

| Driver | Mapping | Across all workers? |
|---|---|---|
| `kafkaq` | the key is the record key (one partition); the driver holds back a record while an earlier one with its key runs | yes |
| `sqsq` with `FIFO: true` | the key is the `MessageGroupId` | yes |
| `rabbitq` | `KeyShards` lanes per kind, each a quorum queue with a single active consumer and a prefetch of 1 | yes |
| `natsq` | `KeyShards` lanes per kind (subjects `<prefix>.key.<n>.<kind>`), each a consumer with `MaxAckPending` 1 | yes |
| `memq` | the driver holds back a key's next job | yes (one process) |
| `pubsubq` with `Ordering: true` | the key is the ordering key; Pub/Sub delivers a key's messages in order | within one worker process (keyed lock) |
| `redisq`, `sqsq` standard | none in the broker | within one worker process only (a keyed lock in the runtime) |

A lane or key that is busy does not slow other keys (except that lanes are shared by the keys hashed to them).
Keyed jobs on `natsq` and `rabbitq` may wait up to one poll when the kind has no unkeyed jobs.

### Batches

```go
// Publish in few round trips: SQS SendMessageBatch (10 per call), a NATS async publish window, Kafka and Pub/Sub
// batches, a Redis pipeline, RabbitMQ with all confirms awaited together.
err := q.PublishBatch(ctx, []jobqueue.Pub{{Kind: "push.send", ID: "p1", Payload: a}, {Kind: "push.send", ID: "p2", Payload: b}})

// Fetch up to 50 at a time, run up to 32 at once; each is acknowledged on its own.
q.Work(ctx, handlers, jobqueue.WorkOptions{BatchSize: 50, Concurrency: 32})

// Or hand a whole batch to one call (send many pushes concurrently, delete many files), with a result per job:
q.Work(ctx, nil, jobqueue.WorkOptions{BatchSize: 100, BatchHandlers: map[string]jobqueue.BatchHandler{
	"files.delete": func(ctx context.Context, jobs []jobqueue.Job) []error {
		errs := make([]error, len(jobs))
		// ... errs[i] = nil when jobs[i] is done
		return errs
	},
}})
```

One bad job in a batch fails only itself: it is retried or dead-lettered alone.

### Test your handlers

```go
d := memq.New()
q := jobqueue.New(d)
_ = q.Publish(ctx, "email.send", "t1", payload)
go q.Work(ctx, handlers, jobqueue.WorkOptions{})
// ... assert; d.DeadLetters() lists what failed for good
```

### Write an adapter

Implement `jobqueue.Driver` (and `Delivery`), optionally `jobqueue.Repeater`, and run the suite:

```go
func TestDriver(t *testing.T) {
	jobqueuetest.Driver(t, func(t *testing.T) jobqueue.Driver { return mydriver.New(...) })
}
```

`jobqueuetest.Lease`, `Store`, `DoneLog` and `SchedulerOver` cover the other interfaces.

## Run it on: local / AWS / Google

| | Local or any host | AWS | Google Cloud |
|---|---|---|---|
| Driver | `natsq` (NATS 2.12+, JetStream on), `kafkaq`, `rabbitq`, `redisq` | `sqsq`, or `kafkaq` on MSK, `rabbitq` on Amazon MQ, `redisq` on ElastiCache | `pubsubq`, or `redisq` on Memorystore, `kafkaq` on Confluent |
| Delays | native, any length | native up to 15 min, longer by the scheduler | by the scheduler |
| Scheduler store | not needed (or `pgq`) | `pgq` (RDS / Aurora PostgreSQL) | `pgq` (Cloud SQL PostgreSQL) |
| Lease | not needed (or `natsq.NewLease`, `pgq`) | `dynamolease` or `pgq` | `firestorelease` or `pgq` |
| Done-log | `pgq` | `pgq` | `pgq` |

### Local (NATS)

```sh
docker run -d --name nats -p 4222:4222 nats:2.15.2-alpine -js
```

```go
driver, err := natsq.New(ctx, js, natsq.Config{Prefix: "jobs", Replicas: 1})
```

The driver creates the streams `JOBS` (work queue, subjects `jobs.run.>` and `jobs.sched.>`, schedules and roll-ups
allowed) and `JOBS_DEAD`. With NATS accounts, the app and workers need publish and subscribe on `jobs.>` and `$JS.API.>`
(or the narrower `$JS.API.STREAM.*.JOBS*`, `$JS.API.CONSUMER.*.JOBS.>`, `$JS.ACK.JOBS.>`), and `$KV.jobqueue_leases.>`
for the KV lease. JetStream data holds only jobs in flight; whether to back it up depends on whether your jobs can be
rebuilt from your database.

### Kafka (Apache Kafka, MSK, Confluent, Redpanda)

```go
driver, err := kafkaq.New(kafkaq.Config{Brokers: []string{"broker-1:9092"}, Prefix: "jobs", CreateTopics: true,
	Options: []kgo.Opt{ /* kgo.DialTLSConfig(...), kgo.SASL(...) for MSK IAM or Confluent */ }})
```

Topics `<prefix>.<kind>` and `<prefix>.<kind>.retry` (`Partitions`, default 6), `<prefix>.dead`; consumer groups
`<prefix>.<kind>`. ACLs: `WRITE` and `DESCRIBE` on those topics, `READ` on the kind and retry topics, `READ` on the
groups, and `CREATE` on the cluster for `CreateTopics` (or create the topics yourself). On MSK with IAM:
`kafka-cluster:Connect`, `DescribeTopic`, `ReadData`, `WriteData`, `AlterGroup`, `DescribeGroup`, and `CreateTopic`.
Delays and repeats need a Scheduler.

### RabbitMQ (Amazon MQ, CloudAMQP)

```go
conn, _ := amqp.Dial("amqps://user:pass@host:5671/vhost")
driver, err := rabbitq.New(conn, rabbitq.Config{Prefix: "jobs", Prefetch: 20, KeyShards: 4})
```

Queues (in the default exchange): `<prefix>.<kind>` and `<prefix>.<kind>.k<n>` (quorum), `<prefix>.wait.*` (classic,
self-deleting), `<prefix>.dead`. User permissions on the vhost: configure, write and read on `^<prefix>\..*`.
Quorum queues need RabbitMQ 3.8 or later (Amazon MQ for RabbitMQ 3.13+ supports them). Jobs that run longer than the
broker's `consumer_timeout` (30 minutes by default) are redelivered.

### Redis / Valkey Streams (your own, ElastiCache, Memorystore)

```go
driver := redisq.New(redis.NewClient(&redis.Options{Addr: addr}), redisq.Config{Prefix: "jobs"})
```

Keys `<prefix>:<kind>` (streams, consumer group `workers`), `<prefix>:delayed` (sorted set), `<prefix>:dead`. Redis
5.0 or later; `XAUTOCLAIM` needs 6.2. With ACLs: `~<prefix>:* +@stream +zadd +zrangebyscore +zrem +evalsha +eval`.
Cluster mode: one kind's keys may live on different shards, which is fine (the delay script touches one stream per
job; give `<prefix>` a hash tag such as `{jobs}` to keep them on one shard if your provider requires it for scripts).

### AWS (SQS, DynamoDB)

```go
cfg, _ := config.LoadDefaultConfig(ctx) // region and credentials from the environment or the instance role
driver := sqsq.New(sqs.NewFromConfig(cfg), sqsq.Config{Prefix: "jobs", CreateQueues: true})
lease := dynamolease.New(dynamodb.NewFromConfig(cfg), "jobqueue_leases")
```

IAM permissions (resource `arn:aws:sqs:<region>:<account>:jobs-*`):

| Who | Actions |
|---|---|
| app and workers | `sqs:SendMessage`, `sqs:GetQueueUrl`, `sqs:GetQueueAttributes` |
| workers | also `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility` |
| with `CreateQueues: true` | also `sqs:CreateQueue`, `sqs:SetQueueAttributes` (or create the queues with your infrastructure code and leave it false) |
| `dynamolease` | `dynamodb:PutItem`, `dynamodb:DeleteItem` on `arn:aws:dynamodb:<region>:<account>:table/jobqueue_leases`; `dynamodb:CreateTable`, `dynamodb:UpdateTimeToLive` only for `CreateTable` |

With `FIFO: true` the queues are `<prefix>-<kind>.fifo` (needs FIFO-capable permissions on `jobs-*.fifo`), a job's Key
is its message group, ids are de-duplicated for 5 minutes, and delays go through the Scheduler.

Queues: `<prefix>-<kind>` (dots become `_`) with a redrive policy to `<prefix>-<kind>-dead` after `MaxReceives`
(50) receives, for messages that crash every worker. The DynamoDB table has the string key `name`; turn on TTL on `ttl`.

### Google Cloud (Pub/Sub, Firestore)

```go
driver, err := pubsubq.New(ctx, pubsubq.Config{Project: "my-project", Prefix: "jobs", CreateResources: true})
fs, _ := firestore.NewClient(ctx, "my-project")
lease := firestorelease.New(fs, "jobqueue_leases")
```

Credentials are the application default credentials (the service account of Cloud Run, GKE or the VM).

| Who | Role or permissions |
|---|---|
| app | `roles/pubsub.publisher` on the topics (`pubsub.topics.publish`) |
| workers | `roles/pubsub.subscriber` on the subscriptions (`pubsub.subscriptions.consume`) and publisher on `<prefix>-dead` |
| with `CreateResources: true` | `pubsub.topics.create`, `pubsub.subscriptions.create`, `pubsub.topics.attachSubscription` (`roles/pubsub.editor`), or create them with your infrastructure code |
| `firestorelease` | `roles/datastore.user` (`datastore.entities.get`, `.create`, `.update`, `.delete`); a TTL policy on the field `ttl` is optional cleanup |

Resources: topic `<prefix>-<kind>` with the pull subscription `<prefix>-<kind>-workers`, and the topic `<prefix>-dead`
with the subscription `<prefix>-dead-keep` (7 days). If you add a dead-letter policy to the workers' subscriptions,
Pub/Sub counts delivery attempts itself; without one, attempts are counted per worker process.

### PostgreSQL (`pgq`)

`pgq.Migrate` needs `CREATE` on the schema once; afterwards `SELECT, INSERT, UPDATE, DELETE` on `jobqueue_scheduled`,
`jobqueue_leases` and `jobqueue_done`. Every statement is a single round trip with no session state, so it works
behind PgBouncer in transaction mode, RDS Proxy and Cloud SQL managed pooling.

## Options

`jobqueue.WorkOptions`:

| Option | Default | Meaning |
|---|---|---|
| `Concurrency` | 1 | jobs of each kind running at once in this process (for a batch handler: batches) |
| `BatchSize` | 1 | jobs one fetch may take (up to the idle handlers); the batch size of a batch handler |
| `BatchHandlers` | none | kinds handled a batch at a time |
| `StopGrace` | 25s | how long running jobs may finish after the context ends |
| `MaxAttempts` | 5 | tries before the dead letters |
| `Backoff` | 5s, 30s, 2m, 10m, 30m | wait before the next attempt |
| `ExtendEvery` | 20s | how often a running job is kept hidden from other workers |
| `Logger` | `slog.Default()` | warnings about failures |
| `OnDone` | none | told about each job: `done`, `retry`, `dead` or `released` |

`jobqueue.Scheduler`: `Store`, `Lease` (required), `Holder` (required, unique per process), `Name`
(`jobqueue-scheduler`), `TTL` (30s), `Poll` (1s).

`natsq.Config`: `KeyShards` (4 ordered lanes per kind), `Prefix` (`jobs`), `Stream` (the prefix in upper case), `Poll` (2s), `AckWait` (60s), `Replicas` (1),
`MaxAge` (7 days), `DeadMaxAge` (30 days), `Duplicates` (10 min: a second publish of the same kind and id is dropped),
`Storage` (file).

`sqsq.Config`: `Prefix` (`jobs`), `Wait` (5s long poll, at most 20s), `Visibility` (60s), `CreateQueues` (false),
`FIFO` (false), `MaxReceives` (50).

`kafkaq.Config`: `Brokers` (required), `Options` (extra `kgo.Opt`), `Prefix` (`jobs`), `CreateTopics` (false),
`Partitions` (6), `ReplicationFactor` (broker default), `Poll` (2s).

`rabbitq.Config`: `Prefix` (`jobs`), `Prefetch` (20), `KeyShards` (4), `Poll` (2s).

`redisq.Config`: `Prefix` (`jobs`), `Group` (`workers`), `Consumer` (host, pid, random), `Poll` (2s), `Visibility`
(60s: a job idle this long is taken over by another worker), `DeadMaxLen` (10000).

`pubsubq.Config`: `Project` (required), `Prefix` (`jobs`), `Poll` (5s), `AckDeadline` (60s), `CreateResources` (false),
`Ordering` (false).

## Adapters at a glance

| Adapter | Delays | Repeats | De-duplicates by id | Tested against |
|---|---|---|---|---|
| `natsq` | any (server schedules, to the second) | native | yes, within `Duplicates` | nats-server 2.15 |
| `sqsq` | up to 15 min | scheduler | no | ElasticMQ |
| `pubsubq` | scheduler | scheduler | no | the gcloud Pub/Sub emulator |
| `kafkaq` | scheduler | scheduler | no | Redpanda 25.2 |
| `rabbitq` | any (wait queues, to the second) | scheduler | no | RabbitMQ 4.1 |
| `redisq` | any (a sorted set) | scheduler | no | Valkey 8.1 |
| `sqsq` FIFO | scheduler | scheduler | yes, 5 minutes | ElasticMQ |
| `memq` | any | native | no | itself |
| `pgq`, `natsq.Lease`, `dynamolease`, `firestorelease` | | | | PostgreSQL 16, nats-server, DynamoDB Local, the Firestore emulator |

`sqsq`, `pubsubq`, `dynamolease` and `firestorelease` pass the suite on the emulators; they have not yet run against
the real services. Treat them as **experimental** until they have. `kafkaq`, `rabbitq` and `redisq` ran against
the real servers (Redpanda, RabbitMQ, Valkey), not yet against MSK, Confluent, Amazon MQ, ElastiCache or
Memorystore.

## Kafka: what is emulated

Kafka keeps an offset per partition, not an acknowledgement per message, and holds nothing for later. `kafkaq`:

- **Acknowledgement**: tracks the offsets it handed out per partition and commits a partition only up to the first
  job not yet done. A crash redelivers every unfinished job (and some finished ones after it): at least once. One
  slow job holds back its partition's commit, not the work after it.
- **Retries** are re-published to a retry tier, `<prefix>.<kind>.retry-<tier>` (`RetryTiers`, default 5 s, 1 min,
  10 min: the smallest tier at least as long as the back-off), with a not-before time; each tier is read in order and
  put back on the kind's topic when due. All records of a tier wait about as long, so no long back-off holds up a
  short one. Longer delays go through the Scheduler. Keyed jobs are retried in place (their key only waits).
- **Commit lag, worst case**: a partition's committed offset can stay behind by the records handed out since its
  oldest unfinished one. That job ends within `Timeout` (then it is re-published and its offset counts as done), so
  the lag is at most `Timeout` plus a re-publish, and after a crash at most that window of jobs runs again. For keyed
  jobs add `KeyAttempts` × (`Timeout` + back-off). Keep timeouts short where throughput matters.
- **Dead letters** go to `<prefix>.dead` with the attempts and the error in headers.
- **Delays and repeats** go through the Scheduler (`MaxDelay` is 0).
- **Order**: a key maps to one partition, a partition to one group member, and the driver holds a key's next record
  until the previous one is done. A rebalance hands unfinished records to the new owner in order.

## Throughput

Empty jobs, one worker process, measured with `go run ./examples/throughput -driver <d> -n 20000` on one development
machine (8 cores) with the servers from `scripts/test-env.sh` in local containers, `BatchSize` 100,
`Concurrency` 32. Publish is `PublishBatch` in batches of 100; run is fetch, handle and acknowledge each job.

| Driver | Publish (jobs/s) | Run (jobs/s) |
|---|---|---|
| `memq` | 612,000 | 809,000 |
| `kafkaq` (Redpanda, 6 partitions) | 56,900 | 252,000 |
| `rabbitq` (quorum queues, confirms) | 14,800 | 35,100 |
| `redisq` (Valkey) | 158,900 | 24,900 |
| `natsq` (file storage, double acks) | 85,700 | 15,800 |
| `sqsq` (ElasticMQ, batches of 10, 2,000 jobs) | 5,400 | 2,900 |

These are local numbers to compare adapters and settings, not a promise for a cloud service: there the network round
trip dominates, and larger batches and more concurrency pay off more.

## Run the demo

```sh
go run ./examples/demo                              # in memory
docker run -d --name nats -p 4222:4222 nats:2.15.2-alpine -js
go run ./examples/demo -nats nats://127.0.0.1:4222
```

![The demo in memory](docs/images/demo-memory.png)

## Development

```sh
eval "$(scripts/test-env.sh up)"   # nats-server, Redpanda, RabbitMQ, Valkey, ElasticMQ, DynamoDB Local, the Pub/Sub and Firestore emulators, PostgreSQL
gofmt -l . && go vet ./... && go test -race ./...
scripts/test-env.sh down
```

Without those variables the adapter tests are skipped (the root and `memq` tests always run). The tests never reach
a cloud account: the AWS clients use placeholder keys against the local endpoints. The screenshots come from
`NODE_PATH=<node_modules with playwright> node scripts/screenshots.mjs nats://127.0.0.1:4231`.

## License

MIT
