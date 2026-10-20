// SPDX-License-Identifier: Apache-2.0

// Package metrics defines the Prometheus collectors of a Foreman client,
// the shared histogram buckets, and label capping for values
// derived from user data.
//
//	m, err := metrics.New(prometheus.DefaultRegisterer)
//	m.Inserted.WithLabelValues(m.Queue(q), m.Kind(k)).Inc()
package metrics
