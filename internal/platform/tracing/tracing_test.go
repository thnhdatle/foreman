// SPDX-License-Identifier: Apache-2.0

package tracing_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/thnhdatle/foreman/internal/platform/tracing"
)

func TestTracer_WorkAndInsertSpans(t *testing.T) {
	t.Parallel()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	tr := tracing.New(tp)
	_, span := tr.StartWork(context.Background(), 42, "email", "default", 3)
	tracing.End(span, errors.New("boom"))
	_, span = tr.StartInsert(context.Background(), "email", 5)
	tracing.End(span, nil)

	spans := rec.Ended()
	require.Len(t, spans, 2)
	assert.Equal(t, "foreman.work email", spans[0].Name())
	assert.Equal(t, codes.Error, spans[0].Status().Code)
	attrs := map[string]any{}
	for _, a := range spans[0].Attributes() {
		attrs[string(a.Key)] = a.Value.AsInterface()
	}
	assert.Equal(t, map[string]any{
		"foreman.job.id": int64(42), "foreman.job.kind": "email", "foreman.queue": "default", "foreman.job.attempt": int64(3),
	}, attrs)
	assert.Equal(t, "foreman.insert email", spans[1].Name())
	assert.Equal(t, codes.Unset, spans[1].Status().Code)
}

func TestTracer_NilProviderIsNoop(t *testing.T) {
	t.Parallel()
	_, span := tracing.New(nil).StartWork(context.Background(), 1, "k", "q", 1)
	assert.False(t, span.SpanContext().IsValid())
	tracing.End(span, nil)
}
