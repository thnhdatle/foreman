// SPDX-License-Identifier: Apache-2.0

package foreman

import "testing"

// FuzzDecodeCursor: list cursors from clients never panic and round-trip.
func FuzzDecodeCursor(f *testing.F) {
	for _, s := range []string{"", encodeCursor(1), encodeCursor(9007199254740993), "aWQ6", "!!", "aWQ6LTE"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		id, err := decodeCursor(s)
		if err != nil || id == 0 {
			return
		}
		if again, err := decodeCursor(encodeCursor(id)); err != nil || again != id {
			t.Fatalf("cursor %q -> %d does not round-trip", s, id)
		}
	})
}
