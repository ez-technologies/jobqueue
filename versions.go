package jobqueue

import (
	"context"
	"fmt"
	"sync"
)

// VersionStore keeps the newest version seen per key (a kind and an entity): pgq, natsq, redisq, dynamolease,
// firestorelease, or NewMemoryVersions.
type VersionStore interface {
	// Observe raises key's version to v (never lowers it), atomically.
	Observe(ctx context.Context, key string, v int64) error
	// Latest is key's newest version; ok is false when none was seen.
	Latest(ctx context.Context, key string) (v int64, ok bool, err error)
}

// SkipFunc decides whether a job is no longer worth running; the reason is logged. A skip is final: the job is
// acknowledged and never runs.
type SkipFunc func(ctx context.Context, job Job) (skip bool, reason string)

// RetryHandler runs a job that came back from the retry path (it failed before); info says how. Without one, the
// kind's Handler runs again.
type RetryHandler func(ctx context.Context, job Job, info RetryInfo) error

// Supersede is the ready-made SkipFunc for state-sync jobs: skip a job whose Version is lower than the newest version
// published for its Entity (WithVersions on the publishing Queue, the same store here). Jobs without an Entity always
// run. Use it only for last-write-wins work ("recompute the summary of topic 7"), never where every job matters.
func Supersede(store VersionStore) SkipFunc {
	return func(ctx context.Context, job Job) (bool, string) {
		if job.Entity == "" {
			return false, ""
		}
		latest, ok, err := store.Latest(ctx, job.Kind+"\x00"+job.Entity)
		if err != nil || !ok || latest <= job.Version {
			return false, ""
		}
		return true, fmt.Sprintf("superseded: version %d of %s is newer than %d", latest, job.Entity, job.Version)
	}
}

// memVersions is a VersionStore in this process.
type memVersions struct {
	mu sync.Mutex
	v  map[string]int64
}

// NewMemoryVersions keeps versions in this process (tests, one process).
func NewMemoryVersions() VersionStore { return &memVersions{v: map[string]int64{}} }

func (m *memVersions) Observe(_ context.Context, key string, v int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cur, ok := m.v[key]; !ok || v > cur {
		m.v[key] = v
	}
	return nil
}

func (m *memVersions) Latest(_ context.Context, key string) (int64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.v[key]
	return v, ok, nil
}
