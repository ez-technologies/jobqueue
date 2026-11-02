package rabbitq_test

import (
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/ez-technologies/jobqueue"
	"github.com/ez-technologies/jobqueue/jobqueuetest"
	"github.com/ez-technologies/jobqueue/memq"
	"github.com/ez-technologies/jobqueue/rabbitq"
)

func newDriver(t *testing.T) *rabbitq.Driver {
	t.Helper()
	url := os.Getenv("JOBQUEUE_TEST_RABBITMQ_URL")
	if url == "" {
		t.Skip("JOBQUEUE_TEST_RABBITMQ_URL is not set (eval \"$(scripts/test-env.sh up)\")")
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	d, err := rabbitq.New(conn, rabbitq.Config{Prefix: "jqtest", Poll: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func TestDriver(t *testing.T) {
	jobqueuetest.Driver(t, func(t *testing.T) jobqueue.Driver { return newDriver(t) })
}

func TestSchedulerOver(t *testing.T) {
	jobqueuetest.SchedulerOver(t, newDriver(t), memq.NewStore(), memq.NewLease())
}
