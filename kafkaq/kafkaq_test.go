package kafkaq_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ez-technologies/jobqueue"
	"github.com/ez-technologies/jobqueue/jobqueuetest"
	"github.com/ez-technologies/jobqueue/kafkaq"
	"github.com/ez-technologies/jobqueue/memq"
)

func newDriver(t *testing.T) *kafkaq.Driver {
	t.Helper()
	brokers := os.Getenv("JOBQUEUE_TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("JOBQUEUE_TEST_KAFKA_BROKERS is not set (eval \"$(scripts/test-env.sh up)\")")
	}
	d, err := kafkaq.New(kafkaq.Config{Brokers: strings.Split(brokers, ","), Prefix: "jqtest", CreateTopics: true,
		Partitions: 3, Poll: 500 * time.Millisecond})
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
