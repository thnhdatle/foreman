// SPDX-License-Identifier: Apache-2.0

package foreman

import "context"

// Middleware wraps inserts and work.
type Middleware interface {
	InsertMany(ctx context.Context, params []InsertParams, next func(ctx context.Context) ([]*InsertResult, error)) (
		[]*InsertResult, error)
	Work(ctx context.Context, job *JobRow, next func(ctx context.Context) error) error
}

// MiddlewareDefaults is a pass-through Middleware to embed.
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
