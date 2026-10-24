// SPDX-License-Identifier: Apache-2.0

package schedule_test

import (
	"testing"
	"time"

	"github.com/thnhdatle/foreman/internal/domain/schedule"
)

func FuzzParseCron(f *testing.F) {
	for _, s := range []string{"* * * * *", "*/5 * * * * *", "@hourly", "0 9 * * MON-FRI", "61 * * * *", "@every 1h", ""} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, expr string) {
		s, err := schedule.ParseCron(expr, time.UTC)
		if err != nil {
			return
		}
		at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		for i := 0; i < 3; i++ {
			next := s.Next(at)
			if next.IsZero() {
				return
			}
			if !next.After(at) {
				t.Fatalf("%q: next %s not after %s", expr, next, at)
			}
			at = next
		}
	})
}
