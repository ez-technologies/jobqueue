# AGENTS.md: rules for anyone (human or agent) changing code that uses jobqueue

jobqueue runs background jobs on NATS, Kafka, RabbitMQ, Redis Streams, SQS, Pub/Sub or in memory. The README has the
details; these are the rules and the traps.

## Rules
1. **Handlers must be idempotent.** Delivery is at least once on every broker. Record each effect with `Once` and a
   done-log (`pgq.DoneLog`), keyed by job id and recipient.
2. **Never assume order** unless the jobs carry a `Key` and the queue's `Mode` is keyed-ordered (the default).
3. **Never do long work without a lane and a timeout.** Give slow kinds a `Lane` with its own `Timeout` and workers;
   every job has a `Timeout` (default 5 minutes) and a handler must stop when its ctx ends.
4. **A failure is re-published, not retried in place** (unless keyed). Do not add sleeps or loops inside a handler to
   retry: return the error.
5. **Kafka**: a partition's commit lags behind its oldest unfinished job (at most about `Timeout`). Keep timeouts
   short; never block a handler on another Kafka job.
6. **Delays**: SQS holds at most 15 minutes, Pub/Sub none, Kafka up to its last retry tier. Longer ones need a
   `Scheduler` (`pgq` store and a lease), or `Publish` returns `ErrNoScheduler`.
7. **Ordered keys can stall**: a key that keeps failing is parked (`KeyPolicy`). Watch `OnParked` and the dead
   letters; unpark only after fixing the cause, then replay the key's dead letters in order.
8. **Batch handlers must return one result per job**, in the order given.
9. **Skip only what is covered.** Ask: does a newer job, or the current state, already cover this job's effect? Yes
   (a newer notification for the same person and conversation, a read position past the message, a recompute, an
   entity sync): skip. No (a payment, a distinct email or letter, a one-time code, an audit record): never skip. Scope
   the supersede key to what the effect covers; versions must grow with every change; a skip is final.
10. **Test adapters and stores with `jobqueuetest`** (Driver, Flow, Lease, Store, DoneLog, Versions, Parking,
    SchedulerOver) on local emulators only (`scripts/test-env.sh`); never against a cloud account.

## Traps
- A NATS publish with the same kind and id within 10 minutes is dropped (de-duplication): give each job its own id.
- A handler that ignores its ctx keeps running after its timeout, while its job is retried elsewhere.
- `OnlyLanes` that leave a lane with no process: its jobs wait forever. Every lane needs a worker somewhere.
- Changing `Mode`, lanes or key shards moves jobs to other queues: drain the old ones first.
- The library is generic: keep application names, data and secrets out of it.

## Checks before a pull request
`gofmt -l .` (empty), `go vet ./...`, `eval "$(scripts/test-env.sh up)" && go test -race ./...`.
