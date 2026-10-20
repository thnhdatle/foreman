// SPDX-License-Identifier: Apache-2.0

// Package tracing creates OpenTelemetry spans for job work and inserts:
// "foreman.work <kind>" with job id, kind, queue and attempt attributes.
//
//	tr := tracing.New(tp)
//	ctx, span := tr.StartWork(ctx, job.ID, job.Kind, job.Queue, job.Attempt)
//	defer tracing.End(span, err)
package tracing
