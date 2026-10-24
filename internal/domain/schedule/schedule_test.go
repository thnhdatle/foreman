// SPDX-License-Identifier: Apache-2.0

package schedule_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/foreman/internal/domain/schedule"
)

func utc(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

func ticks(t *testing.T, s schedule.Schedule, from time.Time, n int) []string {
	t.Helper()
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		from = s.Next(from)
		out = append(out, from.Format(time.RFC3339))
	}
	return out
}

func mustLoc(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	require.NoError(t, err)
	return loc
}

func TestParseCron_Forms(t *testing.T) {
	t.Parallel()
	tests := []struct {
		expr string
		from string
		want []string
	}{
		{"* * * * *", "2026-01-01T00:00:30Z", []string{"2026-01-01T00:01:00Z", "2026-01-01T00:02:00Z"}},
		{"*/15 * * * * *", "2026-01-01T00:00:00Z", []string{"2026-01-01T00:00:15Z", "2026-01-01T00:00:30Z"}},
		{"@hourly", "2026-01-01T00:00:00Z", []string{"2026-01-01T01:00:00Z", "2026-01-01T02:00:00Z"}},
		{"0 9 * * MON", "2026-01-01T00:00:00Z", []string{"2026-01-05T09:00:00Z", "2026-01-12T09:00:00Z"}},
	}
	for _, tc := range tests {
		t.Run(tc.expr, func(t *testing.T) {
			t.Parallel()
			s, err := schedule.ParseCron(tc.expr, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.want, ticks(t, s, utc(tc.from), len(tc.want)))
		})
	}
}

func TestParseCron_Errors(t *testing.T) {
	t.Parallel()
	for _, expr := range []string{"", "not cron", "61 * * * *", "@every 1h", "* * * * * * *"} {
		_, err := schedule.ParseCron(expr, time.UTC)
		require.ErrorIsf(t, err, schedule.ErrInvalid, "expr %q", expr)
	}
}

func TestParseCron_NeverFiresAgain(t *testing.T) {
	t.Parallel()
	s, err := schedule.ParseCron("0 0 30 2 *", time.UTC)
	require.NoError(t, err)
	assert.True(t, s.Next(utc("2026-01-01T00:00:00Z")).IsZero())
	due, skipped := schedule.DueTicks(s, utc("2026-01-01T00:00:00Z"), utc("2027-01-01T00:00:00Z"), time.Hour, 10)
	assert.Empty(t, due)
	assert.Zero(t, skipped)
}

func TestParseCron_DST(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		tz   string
		expr string
		from string
		want []string
	}{
		{
			name: "seoul daily", tz: "Asia/Seoul", expr: "30 2 * * *", from: "2026-03-28T12:00:00Z",
			want: []string{"2026-03-28T17:30:00Z", "2026-03-29T17:30:00Z", "2026-03-30T17:30:00Z"},
		},
		{
			name: "new york spring forward daily", tz: "America/New_York", expr: "30 2 * * *",
			from: "2026-03-07T12:00:00Z",
			want: []string{"2026-03-09T06:30:00Z", "2026-03-10T06:30:00Z"},
		},
		{
			name: "new york spring forward hourly", tz: "America/New_York", expr: "0 * * * *",
			from: "2026-03-08T05:00:00Z",
			want: []string{"2026-03-08T06:00:00Z", "2026-03-08T07:00:00Z", "2026-03-08T08:00:00Z"},
		},
		{
			name: "new york fall back daily", tz: "America/New_York", expr: "30 1 * * *",
			from: "2026-10-31T12:00:00Z",
			want: []string{"2026-11-01T05:30:00Z", "2026-11-01T06:30:00Z", "2026-11-02T06:30:00Z"},
		},
		{
			name: "new york fall back hourly", tz: "America/New_York", expr: "0 * * * *",
			from: "2026-11-01T04:00:00Z",
			want: []string{"2026-11-01T05:00:00Z", "2026-11-01T06:00:00Z", "2026-11-01T07:00:00Z"},
		},
		{
			name: "berlin spring forward daily", tz: "Europe/Berlin", expr: "30 2 * * *",
			from: "2026-03-28T12:00:00Z",
			want: []string{"2026-03-30T00:30:00Z", "2026-03-31T00:30:00Z"},
		},
		{
			name: "berlin fall back daily", tz: "Europe/Berlin", expr: "30 2 * * *",
			from: "2026-10-24T12:00:00Z",
			want: []string{"2026-10-25T00:30:00Z", "2026-10-25T01:30:00Z", "2026-10-26T01:30:00Z"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, err := schedule.ParseCron(tc.expr, mustLoc(t, tc.tz))
			require.NoError(t, err)
			got := ticks(t, s, utc(tc.from), len(tc.want))
			assert.Equal(t, tc.want, got)
			for i := 1; i < len(got); i++ {
				assert.Greater(t, got[i], got[i-1], "ticks strictly increase")
			}
		})
	}
}

func TestEvery(t *testing.T) {
	t.Parallel()
	_, err := schedule.Every(0)
	require.ErrorIs(t, err, schedule.ErrInvalid)
	_, err = schedule.Every(-time.Second)
	require.ErrorIs(t, err, schedule.ErrInvalid)

	s, err := schedule.Every(15 * time.Minute)
	require.NoError(t, err)
	assert.Equal(t, []string{"2026-01-01T00:15:00Z", "2026-01-01T00:30:00Z"},
		ticks(t, s, utc("2026-01-01T00:07:12Z"), 2))
	assert.Equal(t, "2026-01-01T00:30:00Z", s.Next(utc("2026-01-01T00:15:00Z")).Format(time.RFC3339),
		"strictly after a tick")
	neg := time.Unix(-90, 0)
	m, err := schedule.Every(time.Minute)
	require.NoError(t, err)
	assert.Equal(t, time.Unix(-60, 0).UTC(), m.Next(neg))
	assert.Equal(t, time.Unix(0, 0).UTC(), m.Next(time.Unix(-60, 0)))
}

func TestEvery_Property_AlignedAndStrictlyAfter(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		d := time.Duration(rapid.Int64Range(1, int64(48*time.Hour)).Draw(rt, "d"))
		at := time.Unix(0, rapid.Int64Range(-1<<60, 1<<60).Draw(rt, "t"))
		s, err := schedule.Every(d)
		if err != nil {
			rt.Fatal(err)
		}
		n := s.Next(at)
		if !n.After(at) || n.Sub(at) > d || n.UnixNano()%int64(d) != 0 {
			rt.Fatalf("Every(%s).Next(%d) = %d", d, at.UnixNano(), n.UnixNano())
		}
	})
}

func TestDueTicks(t *testing.T) {
	t.Parallel()
	minute, err := schedule.ParseCron("* * * * *", nil)
	require.NoError(t, err)
	base := utc("2026-01-01T10:00:00Z")
	tests := []struct {
		name        string
		last, now   time.Time
		window      time.Duration
		limit       int
		wantDue     []string
		wantSkipped int
	}{
		{name: "nothing due", last: base, now: base.Add(59 * time.Second), window: 5 * time.Minute, limit: 10},
		{
			name: "tick exactly at now", last: base, now: base.Add(time.Minute), window: 0, limit: 10,
			wantDue: []string{"2026-01-01T10:01:00Z"},
		},
		{
			name: "gap within window", last: base, now: base.Add(3*time.Minute + 30*time.Second), window: 5 * time.Minute,
			limit: 10, wantDue: []string{"2026-01-01T10:01:00Z", "2026-01-01T10:02:00Z", "2026-01-01T10:03:00Z"},
		},
		{
			name: "gap beyond window skips oldest", last: base, now: base.Add(10 * time.Minute), window: 5 * time.Minute,
			limit: 10, wantSkipped: 4,
			wantDue: []string{
				"2026-01-01T10:05:00Z", "2026-01-01T10:06:00Z", "2026-01-01T10:07:00Z",
				"2026-01-01T10:08:00Z", "2026-01-01T10:09:00Z", "2026-01-01T10:10:00Z",
			},
		},
		{
			name: "limit", last: base, now: base.Add(10 * time.Minute), window: time.Hour, limit: 2,
			wantDue: []string{"2026-01-01T10:01:00Z", "2026-01-01T10:02:00Z"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			due, skipped := schedule.DueTicks(minute, tc.last, tc.now, tc.window, tc.limit)
			got := make([]string, 0, len(due))
			for _, d := range due {
				got = append(got, d.Format(time.RFC3339))
			}
			if len(tc.wantDue) == 0 {
				assert.Empty(t, got)
			} else {
				assert.Equal(t, tc.wantDue, got)
			}
			assert.Equal(t, tc.wantSkipped, skipped)
		})
	}
}

func TestDueTicks_LongGapSaturatesSkipCount(t *testing.T) {
	t.Parallel()
	sec, err := schedule.Every(time.Second)
	require.NoError(t, err)
	last := utc("2026-01-01T00:00:00Z")
	now := last.Add(48 * time.Hour)
	due, skipped := schedule.DueTicks(sec, last, now, 3*time.Second, 100)
	assert.Equal(t, 100_000, skipped)
	require.Len(t, due, 4)
	assert.Equal(t, now.Add(-3*time.Second), due[0])
	assert.Equal(t, now, due[3])
}

type stuck struct{}

func (stuck) Next(t time.Time) time.Time { return t }

func TestDueTicks_NonAdvancingScheduleTerminates(t *testing.T) {
	t.Parallel()
	base := utc("2026-01-01T00:00:00Z")
	due, skipped := schedule.DueTicks(stuck{}, base, base.Add(time.Hour), time.Minute, 10)
	assert.Empty(t, due)
	assert.Equal(t, 1, skipped)
	due, _ = schedule.DueTicks(stuck{}, base.Add(time.Hour), base.Add(time.Hour), time.Minute, 10)
	assert.Len(t, due, 1)
}
