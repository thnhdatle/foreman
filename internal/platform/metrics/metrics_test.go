// SPDX-License-Identifier: Apache-2.0

package metrics_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman/internal/platform/metrics"
)

func TestNew_SharesCollectorsAcrossClients(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	a, err := metrics.New(reg)
	require.NoError(t, err)
	b, err := metrics.New(reg)
	require.NoError(t, err, "a second client on the same registerer")
	a.Inserted.WithLabelValues("q", "k").Inc()
	b.Inserted.WithLabelValues("q", "k").Inc()
	assert.InDelta(t, 2, testutil.ToFloat64(a.Inserted.WithLabelValues("q", "k")), 0)
	families, err := reg.Gather()
	require.NoError(t, err)
	names := map[string]bool{}
	for _, f := range families {
		names[f.GetName()] = true
	}
	assert.True(t, names["foreman_jobs_inserted_total"])
	assert.True(t, names["foreman_build_info"])

	private, err := metrics.New(nil)
	require.NoError(t, err)
	private.Leader.Set(1)
	assert.InDelta(t, 1, testutil.ToFloat64(private.Leader), 0)
}

func TestNew_ConflictingCollectorFails(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(prometheus.NewGauge(prometheus.GaugeOpts{Name: "foreman_rescued_total", Help: "x"})))
	_, err := metrics.New(reg)
	require.Error(t, err)
}

func TestCapper(t *testing.T) {
	t.Parallel()
	c := metrics.NewCapper(2)
	assert.Equal(t, "a", c.Value("a"))
	assert.Equal(t, "b", c.Value("b"))
	assert.Equal(t, metrics.Other, c.Value("c"))
	assert.Equal(t, "a", c.Value("a"))
	m, err := metrics.New(nil)
	require.NoError(t, err)
	for i := 0; i < metrics.DefaultLabelCap; i++ {
		assert.Equal(t, fmt.Sprint(i), m.Queue(fmt.Sprint(i)))
	}
	assert.Equal(t, metrics.Other, m.Queue("overflow"))
	assert.Equal(t, "k", m.Kind("k"))
	assert.Equal(t, "n", m.Name("n"))
}

func TestObserveMaintenance(t *testing.T) {
	t.Parallel()
	m, err := metrics.New(nil)
	require.NoError(t, err)
	at := time.Unix(1700000000, 500_000_000)
	m.ObserveMaintenance("cleaner", 7, at)
	assert.InDelta(t, 1700000000.5, testutil.ToFloat64(m.MaintenanceRun.WithLabelValues("cleaner")), 1e-3)
	assert.InDelta(t, 7, testutil.ToFloat64(m.MaintenanceRows.WithLabelValues("cleaner")), 0)
}
