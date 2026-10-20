// SPDX-License-Identifier: Apache-2.0

package job_test

import (
	"testing"
	"time"

	"github.com/thnhdatle/foreman/internal/domain/job"
)

func BenchmarkMapResult_Retry(b *testing.B) {
	r := row(2, 25)
	o := job.Outcome{Err: errBoom}
	b.ReportAllocs()
	for b.Loop() {
		_ = job.MapResult(r, o, now, func() time.Time { return retryAt })
	}
}
