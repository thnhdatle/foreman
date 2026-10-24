// SPDX-License-Identifier: Apache-2.0

package config_test

import (
	"testing"

	"github.com/thnhdatle/foreman/internal/config"
)

func FuzzParseDuration(f *testing.F) {
	for _, s := range []string{"30s", "14d", "1d12h", "0d", "-1d", "d", "5m", "1d-5m", "999999999999d"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		d, err := config.ParseDuration(s)
		if err == nil && d < 0 && !hasMinus(s) {
			t.Fatalf("%q parsed to negative %s", s, d)
		}
	})
}

func hasMinus(s string) bool {
	for _, r := range s {
		if r == '-' {
			return true
		}
	}
	return false
}
