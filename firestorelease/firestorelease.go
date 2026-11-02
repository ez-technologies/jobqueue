// Package firestorelease is a jobqueue Lease on Google Cloud Firestore: one document per lease in a collection
// (default "jobqueue_leases"), changed in a transaction. Set a TTL policy on the "ttl" field to have old documents
// removed (cleanup only: expiry is checked in every transaction).
package firestorelease

import (
	"context"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ez-technologies/jobqueue"
)

// Lease keeps leases in a collection.
type Lease struct {
	c   *firestore.Client
	col string
	now func() time.Time
}

var _ jobqueue.Lease = (*Lease)(nil)

// New returns leases in collection (default "jobqueue_leases").
func New(c *firestore.Client, collection string) *Lease {
	if collection == "" {
		collection = "jobqueue_leases"
	}
	return &Lease{c: c, col: collection, now: time.Now}
}

type doc struct {
	Holder  string    `firestore:"holder"`
	Expires time.Time `firestore:"expires"`
	TTL     time.Time `firestore:"ttl"`
}

// Acquire takes the lease if it is free, expired or holder's.
func (l *Lease) Acquire(ctx context.Context, name, holder string, ttl time.Duration) (bool, error) {
	ref := l.c.Collection(l.col).Doc(name)
	got := false
	err := l.c.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		got = false
		snap, err := tx.Get(ref)
		now := l.now()
		if err != nil && status.Code(err) != codes.NotFound {
			return err
		}
		if err == nil && snap.Exists() {
			var cur doc
			if err := snap.DataTo(&cur); err != nil {
				return err
			}
			if cur.Holder != holder && cur.Expires.After(now) {
				return nil
			}
		}
		got = true
		return tx.Set(ref, doc{Holder: holder, Expires: now.Add(ttl), TTL: now.Add(ttl + time.Hour)})
	})
	if err != nil {
		return false, err
	}
	return got, nil
}

// Release deletes the lease if holder has it.
func (l *Lease) Release(ctx context.Context, name, holder string) error {
	ref := l.c.Collection(l.col).Doc(name)
	return l.c.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		snap, err := tx.Get(ref)
		if status.Code(err) == codes.NotFound {
			return nil
		}
		if err != nil {
			return err
		}
		var cur doc
		if err := snap.DataTo(&cur); err != nil || cur.Holder != holder {
			return err
		}
		return tx.Delete(ref)
	})
}
