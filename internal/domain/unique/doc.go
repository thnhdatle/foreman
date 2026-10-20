// SPDX-License-Identifier: Apache-2.0

// Package unique validates unique-job options, selects the identifying
// argument fields and computes the period bucket. The canonical form and the
// hash are computed once, in SQL, by foreman_canonical_json, so every
// language that enqueues through SQL gets the same key; Go never hashes.
//
//	id, err := unique.Identity("email", "default", args, unique.Options{ByArgs: true}, now)
//	mask := unique.Mask(opts.States()) // "11111000"
package unique
