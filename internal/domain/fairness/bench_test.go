// SPDX-License-Identifier: Apache-2.0

package fairness_test

import (
	"fmt"
	"testing"

	"github.com/thnhdatle/foreman/internal/domain/fairness"
)

func benchmarkAllocate(b *testing.B, keys int, fair bool) {
	b.Helper()
	cands := make([]fairness.Candidate, keys)
	for i := range cands {
		cands[i] = cand(fmt.Sprintf("k%d", i), 1+i%4, i%60, int64(i), 5)
	}
	tr := fairness.NewTracker()
	b.ReportAllocs()
	for b.Loop() {
		for _, a := range fairness.Allocate(50, cands, fair, tr) {
			tr.Served(a.Key)
		}
	}
}

func BenchmarkAllocate_Fair1000Keys(b *testing.B)   { benchmarkAllocate(b, 1000, true) }
func BenchmarkAllocate_Unfair1000Keys(b *testing.B) { benchmarkAllocate(b, 1000, false) }
func BenchmarkAllocate_Fair2Keys(b *testing.B)      { benchmarkAllocate(b, 2, true) }
