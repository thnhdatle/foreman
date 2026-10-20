// SPDX-License-Identifier: Apache-2.0

package tracing

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// InstrumentationName identifies Foreman's tracer.
const InstrumentationName = "github.com/thnhdatle/foreman"

// Span attribute keys.
const (
	AttrJobID   = attribute.Key("foreman.job.id")
	AttrKind    = attribute.Key("foreman.job.kind")
	AttrQueue   = attribute.Key("foreman.queue")
	AttrAttempt = attribute.Key("foreman.job.attempt")
	AttrCount   = attribute.Key("foreman.job.count")
)

// Tracer creates Foreman spans; a nil provider disables tracing.
type Tracer struct {
	t trace.Tracer
}

// New returns a tracer from tp (no-op when tp is nil).
func New(tp trace.TracerProvider) *Tracer {
	if tp == nil {
		tp = noop.NewTracerProvider()
	}
	return &Tracer{t: tp.Tracer(InstrumentationName)}
}

// StartWork starts the span "foreman.work <kind>" around one attempt.
func (t *Tracer) StartWork(
	ctx context.Context, id int64, kind, queue string, attempt int,
) (context.Context, trace.Span) {
	return t.t.Start(ctx, "foreman.work "+kind, trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(
		AttrJobID.Int64(id), AttrKind.String(kind), AttrQueue.String(queue), AttrAttempt.Int(attempt),
	))
}

// StartInsert starts the span "foreman.insert <kind>" around an insert of n jobs.
func (t *Tracer) StartInsert(ctx context.Context, kind string, n int) (context.Context, trace.Span) {
	return t.t.Start(ctx, "foreman.insert "+kind, trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(
		AttrKind.String(kind), AttrCount.Int(n),
	))
}

// End records err on span (if any) and ends it.
func End(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
