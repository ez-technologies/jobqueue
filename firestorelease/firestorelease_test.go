package firestorelease_test

import (
	"context"
	"os"
	"testing"

	"cloud.google.com/go/firestore"

	"github.com/ez-technologies/jobqueue"
	"github.com/ez-technologies/jobqueue/firestorelease"
	"github.com/ez-technologies/jobqueue/jobqueuetest"
)

// TestLease runs against the Firestore emulator (JOBQUEUE_TEST_FIRESTORE_EMULATOR); never Google Cloud.
func TestLease(t *testing.T) {
	host := os.Getenv("JOBQUEUE_TEST_FIRESTORE_EMULATOR")
	if host == "" {
		t.Skip("JOBQUEUE_TEST_FIRESTORE_EMULATOR is not set (eval \"$(scripts/test-env.sh up)\")")
	}
	t.Setenv("FIRESTORE_EMULATOR_HOST", host)
	c, err := firestore.NewClient(context.Background(), "jobqueue-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	jobqueuetest.Lease(t, func(t *testing.T) jobqueue.Lease { return firestorelease.New(c, "jqtest_leases") })
}
