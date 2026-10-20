// SPDX-License-Identifier: Apache-2.0

// Package clock provides the injectable time source used by app and adapter
// code: [Real] for production and [Fake] for tests and
// simulations driven by a manually advanced clock.
//
//	fc := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
//	ch := fc.After(time.Second)
//	fc.Advance(time.Second) // ch now has a value
package clock
