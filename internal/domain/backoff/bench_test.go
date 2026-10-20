// SPDX-License-Identifier: Apache-2.0

package backoff_test

import (
	"testing"

	"github.com/thnhdatle/foreman/internal/domain/backoff"
)

func BenchmarkExponential_Delay(b *testing.B) {
	e := backoff.NewExponential()
	b.ReportAllocs()
	for b.Loop() {
		_ = e.Delay(7)
	}
}
