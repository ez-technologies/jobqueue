package memq_test

import (
	"testing"

	"github.com/ez-technologies/jobqueue"
	"github.com/ez-technologies/jobqueue/jobqueuetest"
	"github.com/ez-technologies/jobqueue/memq"
)

func TestDriver(t *testing.T) {
	jobqueuetest.Driver(t, func(t *testing.T) jobqueue.Driver { return memq.New() })
}

func TestLease(t *testing.T) {
	jobqueuetest.Lease(t, func(t *testing.T) jobqueue.Lease { return memq.NewLease() })
}

func TestStore(t *testing.T) {
	jobqueuetest.Store(t, func(t *testing.T) jobqueue.Store { return memq.NewStore() })
}

func TestDoneLog(t *testing.T) {
	jobqueuetest.DoneLog(t, func(t *testing.T) jobqueue.DoneLog { return memq.NewDoneLog() })
}

func TestSchedulerOver(t *testing.T) {
	jobqueuetest.SchedulerOver(t, memq.New(), memq.NewStore(), memq.NewLease())
}

func TestVersions(t *testing.T) {
	jobqueuetest.Versions(t, func(t *testing.T) jobqueue.VersionStore { return jobqueue.NewMemoryVersions() })
}

func TestParking(t *testing.T) {
	jobqueuetest.Parking(t, func(t *testing.T) jobqueue.ParkStore { return jobqueue.NewMemoryParkStore() })
}
