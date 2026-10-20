// SPDX-License-Identifier: Apache-2.0

package unique_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/domain/unique"
)

var now = time.Unix(1760602400, 500).UTC()

func TestOptions_Validate(t *testing.T) {
	t.Parallel()
	pending := []job.State{job.StateAvailable, job.StateScheduled, job.StateRunning, job.StateRetryable}
	tests := []struct {
		name   string
		opts   unique.Options
		errSub []string
	}{
		{name: "zero value", opts: unique.Options{}},
		{name: "by args", opts: unique.Options{ByArgs: true, ByQueue: true, ByPeriod: time.Hour}},
		{name: "by fields", opts: unique.Options{ByArgFields: []string{"a"}}},
		{name: "explicit pending states", opts: unique.Options{ByState: pending}},
		{name: "pending plus discarded", opts: unique.Options{ByState: append(append([]job.State{}, pending...), job.StateDiscarded)}},
		{name: "both args selectors", opts: unique.Options{ByArgs: true, ByArgFields: []string{"a"}}, errSub: []string{"mutually exclusive"}},
		{name: "empty field", opts: unique.Options{ByArgFields: []string{"a", ""}}, errSub: []string{"empty field name"}},
		{name: "sub-second period", opts: unique.Options{ByPeriod: 500 * time.Millisecond}, errSub: []string{"whole number of seconds"}},
		{name: "fractional period", opts: unique.Options{ByPeriod: 1500 * time.Millisecond}, errSub: []string{"whole number"}},
		{name: "negative period", opts: unique.Options{ByPeriod: -time.Second}, errSub: []string{"whole number"}},
		{name: "one second period", opts: unique.Options{ByPeriod: time.Second}},
		{name: "unknown state", opts: unique.Options{ByState: append(append([]job.State{}, pending...), "zombie")}, errSub: []string{"zombie"}},
		{name: "missing running", opts: unique.Options{ByState: []job.State{job.StateAvailable, job.StateScheduled, job.StateRetryable}}, errSub: []string{"include running"}},
		{
			name: "all problems at once",
			opts: unique.Options{ByArgs: true, ByArgFields: []string{""}, ByPeriod: time.Millisecond, ByState: []job.State{job.StateCompleted}},
			errSub: []string{
				"mutually exclusive", "empty field name", "whole number", "include available",
				"include scheduled", "include running", "include retryable",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.opts.Validate()
			if len(tc.errSub) == 0 {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, unique.ErrInvalidOptions)
			for _, s := range tc.errSub {
				assert.Contains(t, err.Error(), s)
			}
		})
	}
}

func TestMaskAndStates(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "11111000", unique.Mask(unique.Options{}.States()))
	assert.Equal(t, "11110000", unique.Mask([]job.State{
		job.StateAvailable, job.StateScheduled,
		job.StateRunning, job.StateRetryable,
	}))
	assert.Equal(t, "11110010", unique.Mask([]job.State{
		job.StateAvailable, job.StateScheduled,
		job.StateRunning, job.StateRetryable, job.StateDiscarded,
	}))
	assert.Equal(t, "00000000", unique.Mask([]job.State{"bogus"}))
	custom := []job.State{job.StateRetryable, job.StateAvailable, job.StateScheduled, job.StateRunning}
	assert.Equal(t, custom, unique.Options{ByState: custom}.States())
	assert.Equal(t, []job.State{job.StateAvailable, job.StateCancelled}, unique.ParseMask("10000100"))
	assert.Equal(t, []job.State{job.StateAvailable}, unique.ParseMask("1"))
	assert.Empty(t, unique.ParseMask(""))
}

func TestMask_Property_RoundTrip(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		var states []job.State
		for _, s := range job.AllStates {
			if rapid.Bool().Draw(rt, string(s)) {
				states = append(states, s)
			}
		}
		got := unique.ParseMask(unique.Mask(states))
		if len(got) != len(states) {
			rt.Fatalf("round trip %v -> %v", states, got)
		}
		for i := range got {
			if got[i] != states[i] {
				rt.Fatalf("round trip %v -> %v", states, got)
			}
		}
	})
}

func TestPeriodBucket(t *testing.T) {
	t.Parallel()
	assert.Equal(t, int64(1760601600), unique.PeriodBucket(now, time.Hour))
	assert.Equal(t, int64(1760602400), unique.PeriodBucket(now, time.Second))
	assert.Equal(t, int64(1760572800), unique.PeriodBucket(now, 24*time.Hour))
	assert.Equal(t, now.Unix(), unique.PeriodBucket(now, 0))
	before := time.Unix(-1, 0)
	assert.Equal(t, int64(-60), unique.PeriodBucket(before, time.Minute), "floor for negative times")
	assert.Equal(t, int64(-60), unique.PeriodBucket(time.Unix(-60, 0), time.Minute))
	assert.Equal(t, int64(0), unique.PeriodBucket(time.Unix(59, 0), time.Minute))
}

func TestPeriodBucket_Property_Bounds(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		secs := rapid.Int64Range(-1<<40, 1<<40).Draw(rt, "secs")
		period := time.Duration(rapid.Int64Range(1, 1<<20).Draw(rt, "period")) * time.Second
		b := unique.PeriodBucket(time.Unix(secs, 0), period)
		p := int64(period / time.Second)
		if b > secs || secs-b >= p || b%p != 0 {
			rt.Fatalf("bucket %d for %d with period %d", b, secs, p)
		}
	})
}

func TestIdentity(t *testing.T) {
	t.Parallel()
	args := []byte(`{"user_id":9007199254740993,"tmpl":"welcome","extra":[1,2]}`)
	tests := []struct {
		name string
		opts unique.Options
		want string
	}{
		{name: "kind only", opts: unique.Options{}, want: `{"kind":"email","args":{}}`},
		{name: "by args keeps raw bytes", opts: unique.Options{ByArgs: true}, want: `{"kind":"email","args":` + string(args) + `}`},
		{
			name: "by fields sorted and deduplicated, absent omitted",
			opts: unique.Options{ByArgFields: []string{"user_id", "missing", "tmpl", "user_id"}},
			want: `{"kind":"email","args":{"tmpl":"welcome","user_id":9007199254740993}}`,
		},
		{name: "by queue", opts: unique.Options{ByQueue: true}, want: `{"kind":"email","args":{},"queue":"mail\"q"}`},
		{name: "by period", opts: unique.Options{ByPeriod: time.Hour}, want: `{"kind":"email","args":{},"period":1760601600}`},
		{
			name: "everything",
			opts: unique.Options{ByArgFields: []string{"tmpl"}, ByQueue: true, ByPeriod: time.Minute},
			want: `{"kind":"email","args":{"tmpl":"welcome"},"queue":"mail\"q","period":1760602380}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := unique.Identity("email", `mail"q`, args, tc.opts, now)
			require.NoError(t, err)
			assert.Equal(t, tc.want, string(got))
			assert.True(t, json.Valid(got))
		})
	}
}

func TestIdentity_Errors(t *testing.T) {
	t.Parallel()
	_, err := unique.Identity("k", "q", []byte(`{bad`), unique.Options{ByArgs: true}, now)
	require.ErrorContains(t, err, "not valid JSON")
	_, err = unique.Identity("k", "q", []byte(`[1]`), unique.Options{ByArgFields: []string{"a"}}, now)
	require.ErrorContains(t, err, "JSON object")
	_, err = unique.Identity("k", "q", []byte(`null`), unique.Options{ByArgFields: []string{"a"}}, now)
	require.ErrorContains(t, err, "JSON object")
	got, err := unique.Identity("k", "q", []byte(`{bad`), unique.Options{}, now)
	require.NoError(t, err, "args are ignored when not selected")
	assert.JSONEq(t, `{"kind":"k","args":{}}`, string(got))
}
