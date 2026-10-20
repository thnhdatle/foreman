// SPDX-License-Identifier: Apache-2.0

// Package notifier multiplexes a single Postgres LISTEN connection to the
// producers, the cancel handler and the elector, and reconnects with
// jittered backoff when the connection is lost.
//
//	n, err := notifier.New(notifier.Config{Listener: l, Clock: clk, OnConnect: wakeAll})
//	n.Subscribe("foreman_insert", func(queue string) { wake(queue) })
//	go n.Run(ctx)
package notifier
