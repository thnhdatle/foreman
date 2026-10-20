// SPDX-License-Identifier: Apache-2.0

package foreman

import "context"

// Middleware wraps inserts and work, for example for tracing, metrics,
// logging or adding metadata. Middleware runs in Config.Middleware order:
// the first one is the outermost.
type Middleware interface {
	// InsertMany wraps every insert (single inserts are batches of one).
	InsertMany(ctx context.Context, params []InsertParams, next func(ctx context.Context) ([]*InsertResult, error)) (
		[]*InsertResult, error)
	// Work wraps each attempt of a job.
	Work(ctx context.Context, job *JobRow, next func(ctx context.Context) error) error
}

// MiddlewareDefaults implements Middleware as a pass-through; embed it to
// implement only one of the hooks.
type MiddlewareDefaults struct{}

// InsertMany calls next.
func (MiddlewareDefaults) InsertMany(
	ctx context.Context, _ []InsertParams, next func(ctx context.Context) ([]*InsertResult, error),
) ([]*InsertResult, error) {
	return next(ctx)
}

// Work calls next.
func (MiddlewareDefaults) Work(ctx context.Context, _ *JobRow, next func(ctx context.Context) error) error {
	return next(ctx)
}
