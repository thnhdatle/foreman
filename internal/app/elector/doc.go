// SPDX-License-Identifier: Apache-2.0

// Package elector elects one leader per cluster with a lease row whose times
// come from the client clock. The leader runs the maintenance
// services; it renews every TTL/3 and stops its services as soon as a
// renewal fails.
//
//	e, err := elector.New(elector.Config{ClientID: id, Store: store, Clock: clk, Services: runMaintenance})
//	go e.Run(ctx)
package elector
