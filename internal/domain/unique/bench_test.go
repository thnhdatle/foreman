// SPDX-License-Identifier: Apache-2.0

package unique_test

import (
	"testing"
	"time"

	"github.com/thnhdatle/foreman/internal/domain/unique"
)

func BenchmarkIdentity_ByArgFields(b *testing.B) {
	args := []byte(`{"user_id":9007199254740993,"tmpl":"welcome","extra":{"a":[1,2,3]},"note":"x"}`)
	opts := unique.Options{ByArgFields: []string{"user_id", "tmpl"}, ByQueue: true, ByPeriod: time.Hour}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := unique.Identity("email", "default", args, opts, now); err != nil {
			b.Fatal(err)
		}
	}
}
