package natsq

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/ez-technologies/jobqueue"
)

// Lease keeps leases in a JetStream key-value bucket, changed only by compare-and-set on the key's revision. The
// expiry is written by the holder's clock: keep clocks in sync (NTP), and the TTL well above the skew.
type Lease struct {
	kv  jetstream.KeyValue
	now func() time.Time
}

var _ jobqueue.Lease = (*Lease)(nil)

// NewLease makes (or opens) the bucket (default "jobqueue_leases").
func NewLease(ctx context.Context, js jetstream.JetStream, bucket string) (*Lease, error) {
	if bucket == "" {
		bucket = "jobqueue_leases"
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: bucket, History: 1})
	if err != nil {
		return nil, err
	}
	return &Lease{kv: kv, now: time.Now}, nil
}

type leaseValue struct {
	Holder  string    `json:"holder"`
	Expires time.Time `json:"expires"`
}

func key(name string) string { return token(name) }

// Acquire takes or renews the lease.
func (l *Lease) Acquire(ctx context.Context, name, holder string, ttl time.Duration) (bool, error) {
	val, _ := json.Marshal(leaseValue{Holder: holder, Expires: l.now().Add(ttl)})
	entry, err := l.kv.Get(ctx, key(name))
	if errors.Is(err, jetstream.ErrKeyNotFound) || (err == nil && entry.Operation() != jetstream.KeyValuePut) {
		if _, err := l.kv.Create(ctx, key(name), val); err != nil {
			if errors.Is(err, jetstream.ErrKeyExists) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	if err != nil {
		return false, err
	}
	var cur leaseValue
	_ = json.Unmarshal(entry.Value(), &cur)
	if cur.Holder != holder && cur.Expires.After(l.now()) {
		return false, nil
	}
	if _, err := l.kv.Update(ctx, key(name), val, entry.Revision()); err != nil {
		if isConflict(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Release deletes the lease if holder has it.
func (l *Lease) Release(ctx context.Context, name, holder string) error {
	entry, err := l.kv.Get(ctx, key(name))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var cur leaseValue
	_ = json.Unmarshal(entry.Value(), &cur)
	if cur.Holder != holder {
		return nil
	}
	err = l.kv.Delete(ctx, key(name), jetstream.LastRevision(entry.Revision()))
	if isConflict(err) {
		return nil
	}
	return err
}

func isConflict(err error) bool {
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence
}
