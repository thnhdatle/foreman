// SPDX-License-Identifier: Apache-2.0

package unique_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/thnhdatle/foreman/internal/domain/unique"
)

// FuzzIdentity: for any args, Identity never panics and returns valid JSON.
func FuzzIdentity(f *testing.F) {
	for _, s := range []string{`{"a":1}`, `{"a":9007199254740993,"b":"x"}`, `[1,2]`, `null`, `{bad`, `{"a":{"b":[1.5]}}`} {
		f.Add([]byte(s), true, "a")
	}
	f.Fuzz(func(t *testing.T, args []byte, byArgs bool, field string) {
		opts := unique.Options{ByArgs: byArgs, ByQueue: true, ByPeriod: time.Hour}
		if !byArgs && field != "" {
			opts.ByArgFields = []string{field}
		}
		id, err := unique.Identity("k", "q", args, opts, time.Unix(1700000000, 0))
		if err == nil && !json.Valid(id) {
			t.Fatalf("invalid identity JSON %q for args %q", id, args)
		}
	})
}
