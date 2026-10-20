// SPDX-License-Identifier: Apache-2.0

// Package producer runs the fetch loop of one queue: it wakes on NOTIFY, on
// finished jobs and on a poll interval, fetches up to its free capacity
// (honouring pause, per-key limits and fairness) and runs each job on its own
// goroutine with a cancellable job context.
//
//	p, err := producer.New(producer.Config{Queue: "default", ClientID: id, Store: store, Clock: clk, Execute: run})
//	go p.Run(ctx)
//	p.Wake()
package producer
