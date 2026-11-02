# jobqueue

**Background jobs for Go that run the same way on a laptop, on AWS and on Google Cloud.**

- One small interface: publish a job (a kind, an id, a JSON payload); workers run the handler for its kind.
- A worker runtime: acknowledge only after the handler finished, retries with back-off, a dead-letter destination,
  delayed and repeating jobs, panics caught, and a **graceful stop on SIGTERM** within a deadline (spot and preemptible
  machines get about 30 seconds).
- One adapter per platform, each in its own package, so your binary links only the SDK it uses:
  **NATS JetStream**, **AWS SQS**, **Google Cloud Pub/Sub**, and **in memory** for tests.
- Delays a broker cannot hold go through a **scheduler loop**; one worker runs it at a time, chosen by a pluggable
  **lease**: PostgreSQL, NATS KV, DynamoDB or Firestore.
- An idempotency helper (`Once` with a done-log) so a job delivered twice does not send two emails.
- One **conformance suite** (`jobqueuetest`) that every adapter passes, run against local emulators.

![The demo on NATS JetStream: a retry, a delayed job, a repeating job, a dead letter and a graceful stop](docs/images/demo-nats.png)

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [How to](#how-to)
  - [Retries, back-off and dead letters](#retries-back-off-and-dead-letters)
  - [Delayed and repeating jobs](#delayed-and-repeating-jobs)
  - [Delays the broker cannot hold: the scheduler](#delays-the-broker-cannot-hold-the-scheduler)
  - [Stop gracefully on SIGTERM (spot machines)](#stop-gracefully-on-sigterm-spot-machines)
  - [Do it once: idempotent handlers](#do-it-once-idempotent-handlers)
  - [Run only some kinds in a worker](#run-only-some-kinds-in-a-worker)
  - [Test your handlers](#test-your-handlers)
  - [Write an adapter](#write-an-adapter)
- [Run it on: local / AWS / Google](#run-it-on-local--aws--google)
- [Options](#options)
- [Adapters at a glance](#adapters-at-a-glance)
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
| `jobqueue/sqsq` | AWS SQS driver | `aws-sdk-go-v2/service/sqs` |
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
| Driver | `natsq` (NATS 2.12+, JetStream on) | `sqsq` (standard queues) | `pubsubq` |
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
| `Concurrency` | 1 | jobs of each kind running at once in this process |
| `StopGrace` | 25s | how long running jobs may finish after the context ends |
| `MaxAttempts` | 5 | tries before the dead letters |
| `Backoff` | 5s, 30s, 2m, 10m, 30m | wait before the next attempt |
| `ExtendEvery` | 20s | how often a running job is kept hidden from other workers |
| `Logger` | `slog.Default()` | warnings about failures |
| `OnDone` | none | told about each job: `done`, `retry`, `dead` or `released` |

`jobqueue.Scheduler`: `Store`, `Lease` (required), `Holder` (required, unique per process), `Name`
(`jobqueue-scheduler`), `TTL` (30s), `Poll` (1s).

`natsq.Config`: `Prefix` (`jobs`), `Stream` (the prefix in upper case), `Poll` (2s), `AckWait` (60s), `Replicas` (1),
`MaxAge` (7 days), `DeadMaxAge` (30 days), `Duplicates` (10 min: a second publish of the same kind and id is dropped),
`Storage` (file).

`sqsq.Config`: `Prefix` (`jobs`), `Wait` (5s long poll, at most 20s), `Visibility` (60s), `CreateQueues` (false),
`MaxReceives` (50).

`pubsubq.Config`: `Project` (required), `Prefix` (`jobs`), `Poll` (5s), `AckDeadline` (60s), `CreateResources` (false).

## Adapters at a glance

| Adapter | Delays | Repeats | De-duplicates by id | Tested against |
|---|---|---|---|---|
| `natsq` | any (server schedules, to the second) | native | yes, within `Duplicates` | nats-server 2.15 |
| `sqsq` | up to 15 min | scheduler | no | ElasticMQ |
| `pubsubq` | scheduler | scheduler | no | the gcloud Pub/Sub emulator |
| `memq` | any | native | no | itself |
| `pgq`, `natsq.Lease`, `dynamolease`, `firestorelease` | | | | PostgreSQL 16, nats-server, DynamoDB Local, the Firestore emulator |

`sqsq`, `pubsubq`, `dynamolease` and `firestorelease` pass the suite on the emulators; they have not yet run against
the real services. Treat them as **experimental** until they have.

## Run the demo

```sh
go run ./examples/demo                              # in memory
docker run -d --name nats -p 4222:4222 nats:2.15.2-alpine -js
go run ./examples/demo -nats nats://127.0.0.1:4222
```

![The demo in memory](docs/images/demo-memory.png)

## Development

```sh
eval "$(scripts/test-env.sh up)"   # nats-server, ElasticMQ, DynamoDB Local, the Pub/Sub and Firestore emulators, PostgreSQL
gofmt -l . && go vet ./... && go test -race ./...
scripts/test-env.sh down
```

Without those variables the adapter tests are skipped (the root and `memq` tests always run). The tests never reach
a cloud account: the AWS clients use placeholder keys against the local endpoints. The screenshots come from
`NODE_PATH=<node_modules with playwright> node scripts/screenshots.mjs nats://127.0.0.1:4231`.

## License

MIT
