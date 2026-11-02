package jobqueue

import (
	"context"
	"errors"
)

// DoneLog remembers which effects were done, so a job delivered twice does not repeat them (pgq.DoneLog,
// memq.DoneLog).
type DoneLog interface {
	// Claim records key; it answers false when key was recorded before.
	Claim(ctx context.Context, key string) (bool, error)
	// Forget removes key, so the effect may be tried again.
	Forget(ctx context.Context, key string) error
}

// Once runs fn unless key was done before. When fn fails the key is forgotten, so a retry runs it again. It answers
// whether fn ran.
//
// The key is recorded before fn runs: a process that dies inside fn loses that effect rather than doing it twice.
// Use a key per effect, e.g. job id + recipient.
func Once(ctx context.Context, log DoneLog, key string, fn func(ctx context.Context) error) (bool, error) {
	ok, err := log.Claim(ctx, key)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	if err := fn(ctx); err != nil {
		if ferr := log.Forget(context.WithoutCancel(ctx), key); ferr != nil {
			return true, errors.Join(err, ferr)
		}
		return true, err
	}
	return true, nil
}
