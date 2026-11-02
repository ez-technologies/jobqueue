package pgq_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/ez-technologies/jobqueue"
	"github.com/ez-technologies/jobqueue/jobqueuetest"
	"github.com/ez-technologies/jobqueue/memq"
	"github.com/ez-technologies/jobqueue/pgq"
)

func db(t *testing.T) *sql.DB {
	t.Helper()
	url := os.Getenv("JOBQUEUE_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("JOBQUEUE_TEST_POSTGRES_URL is not set (eval \"$(scripts/test-env.sh up)\")")
	}
	d, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if err := pgq.Migrate(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestStore(t *testing.T) {
	jobqueuetest.Store(t, func(t *testing.T) jobqueue.Store { return pgq.NewStore(db(t)) })
}

func TestLease(t *testing.T) {
	jobqueuetest.Lease(t, func(t *testing.T) jobqueue.Lease { return pgq.NewLease(db(t)) })
}

func TestDoneLog(t *testing.T) {
	jobqueuetest.DoneLog(t, func(t *testing.T) jobqueue.DoneLog { return pgq.NewDoneLog(db(t)) })
}

func TestSchedulerOver(t *testing.T) {
	d := db(t)
	jobqueuetest.SchedulerOver(t, memq.New(), pgq.NewStore(d), pgq.NewLease(d))
}

func TestPruneAndMigrateTwice(t *testing.T) {
	d := db(t)
	ctx := context.Background()
	if err := pgq.Migrate(ctx, d); err != nil {
		t.Fatal("migrate is idempotent:", err)
	}
	l := pgq.NewDoneLog(d)
	key := jobqueuetest.Kind("prune")
	if _, err := l.Claim(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Exec(`UPDATE jobqueue_done SET done_at = now() - interval '2 days' WHERE key = $1`, key); err != nil {
		t.Fatal(err)
	}
	if n, err := l.Prune(ctx, 24*time.Hour); err != nil || n < 1 {
		t.Fatalf("prune: %d %v", n, err)
	}
	if ok, _ := l.Claim(ctx, key); !ok {
		t.Fatal("a pruned key can be claimed again")
	}
}
