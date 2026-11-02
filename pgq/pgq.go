// Package pgq keeps jobqueue's scheduler entries, leases and done-log in PostgreSQL, through database/sql (any driver:
// pgx's stdlib, lib/pq). Call Migrate once at start; it creates three tables if they are missing:
// jobqueue_scheduled, jobqueue_leases and jobqueue_done. Times are the database's (now()), so app clocks may differ.
//
// Every statement is a single round trip with no session state, so it works behind PgBouncer in transaction mode.
package pgq

import (
	"context"
	"database/sql"
	"time"

	"github.com/ez-technologies/jobqueue"
)

// Schema is what Migrate runs (idempotent). Put it in your own migrations instead if you prefer.
const Schema = `
CREATE TABLE IF NOT EXISTS jobqueue_scheduled (
	id       text PRIMARY KEY,
	kind     text NOT NULL,
	payload  bytea,
	due_at   timestamptz NOT NULL,
	every_ms bigint NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS jobqueue_scheduled_due ON jobqueue_scheduled (due_at);
CREATE TABLE IF NOT EXISTS jobqueue_leases (
	name       text PRIMARY KEY,
	holder     text NOT NULL,
	expires_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS jobqueue_done (
	key     text PRIMARY KEY,
	done_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS jobqueue_done_at ON jobqueue_done (done_at);`

// Migrate creates the tables.
func Migrate(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, Schema)
	return err
}

// Store is the scheduler's store.
type Store struct{ db *sql.DB }

// NewStore returns a store over db.
func NewStore(db *sql.DB) *Store { return &Store{db: db} }

var _ jobqueue.Store = (*Store)(nil)

func (s *Store) Put(ctx context.Context, e jobqueue.Entry) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO jobqueue_scheduled (id, kind, payload, due_at, every_ms)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (id) DO UPDATE SET kind = EXCLUDED.kind, payload = EXCLUDED.payload, due_at = EXCLUDED.due_at,
			every_ms = EXCLUDED.every_ms`,
		e.ID, e.Kind, []byte(e.Payload), e.Due.UTC(), e.Every.Milliseconds())
	return err
}

func (s *Store) Due(ctx context.Context, now time.Time, limit int) ([]jobqueue.Entry, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, kind, payload, due_at, every_ms FROM jobqueue_scheduled
		WHERE due_at <= $1 ORDER BY due_at, id LIMIT $2`, now.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []jobqueue.Entry
	for rows.Next() {
		var e jobqueue.Entry
		var payload []byte
		var every int64
		if err := rows.Scan(&e.ID, &e.Kind, &payload, &e.Due, &every); err != nil {
			return nil, err
		}
		if len(payload) > 0 {
			e.Payload = payload
		}
		e.Every = time.Duration(every) * time.Millisecond
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) Done(ctx context.Context, e jobqueue.Entry, next *time.Time) error {
	var err error
	if next == nil {
		_, err = s.db.ExecContext(ctx, `DELETE FROM jobqueue_scheduled WHERE id = $1 AND due_at = $2`, e.ID, e.Due.UTC())
	} else {
		_, err = s.db.ExecContext(ctx, `UPDATE jobqueue_scheduled SET due_at = $3 WHERE id = $1 AND due_at = $2`,
			e.ID, e.Due.UTC(), next.UTC())
	}
	return err
}

// Lease is a lease row per name, taken with one conditional upsert.
type Lease struct{ db *sql.DB }

// NewLease returns leases over db.
func NewLease(db *sql.DB) *Lease { return &Lease{db: db} }

var _ jobqueue.Lease = (*Lease)(nil)

func (l *Lease) Acquire(ctx context.Context, name, holder string, ttl time.Duration) (bool, error) {
	var got string
	err := l.db.QueryRowContext(ctx, `INSERT INTO jobqueue_leases (name, holder, expires_at)
		VALUES ($1, $2, now() + $3 * interval '1 millisecond')
		ON CONFLICT (name) DO UPDATE SET holder = EXCLUDED.holder, expires_at = EXCLUDED.expires_at
			WHERE jobqueue_leases.holder = EXCLUDED.holder OR jobqueue_leases.expires_at < now()
		RETURNING holder`, name, holder, ttl.Milliseconds()).Scan(&got)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return got == holder, nil
}

func (l *Lease) Release(ctx context.Context, name, holder string) error {
	_, err := l.db.ExecContext(ctx, `DELETE FROM jobqueue_leases WHERE name = $1 AND holder = $2`, name, holder)
	return err
}

// DoneLog is the done-log table.
type DoneLog struct{ db *sql.DB }

// NewDoneLog returns a done-log over db.
func NewDoneLog(db *sql.DB) *DoneLog { return &DoneLog{db: db} }

var _ jobqueue.DoneLog = (*DoneLog)(nil)

func (l *DoneLog) Claim(ctx context.Context, key string) (bool, error) {
	res, err := l.db.ExecContext(ctx, `INSERT INTO jobqueue_done (key) VALUES ($1) ON CONFLICT DO NOTHING`, key)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func (l *DoneLog) Forget(ctx context.Context, key string) error {
	_, err := l.db.ExecContext(ctx, `DELETE FROM jobqueue_done WHERE key = $1`, key)
	return err
}

// Prune deletes done-log keys older than age (run it from a daily job); it answers how many went.
func (l *DoneLog) Prune(ctx context.Context, age time.Duration) (int64, error) {
	res, err := l.db.ExecContext(ctx, `DELETE FROM jobqueue_done WHERE done_at < now() - $1 * interval '1 millisecond'`,
		age.Milliseconds())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
