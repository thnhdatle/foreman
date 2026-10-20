// SPDX-License-Identifier: Apache-2.0

// Package completer batches job state updates and heartbeats. Results are
// written by size (100) or interval (50 ms) in single statements, retried
// with backoff on database errors, never dropped, and flushed on shutdown.
//
//	c, err := completer.New(completer.Config{ClientID: id, Store: store, Clock: clk})
//	go c.Run(ctx)
//	c.Add(completion)
package completer
