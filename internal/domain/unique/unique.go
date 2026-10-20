// SPDX-License-Identifier: Apache-2.0

package unique

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/thnhdatle/foreman/internal/domain/job"
)

// Errors of unique jobs.
var (
	// ErrInvalidOptions is wrapped by every validation error of Options.
	ErrInvalidOptions = errors.New("unique: invalid options")
	// ErrArgsNotCanonical reports a non-integral number in the unique
	// identity; foreman_canonical_json rejects it (SQLSTATE 22023).
	ErrArgsNotCanonical = errors.New("foreman: unique args not canonical")
)

// Options selects what makes a job unique.
type Options struct {
	ByArgs      bool
	ByArgFields []string
	ByPeriod    time.Duration
	ByQueue     bool
	ByState     []job.State
}

// DefaultStates are the states covered when ByState is empty.
var DefaultStates = []job.State{
	job.StateAvailable, job.StateScheduled, job.StateRunning, job.StateRetryable, job.StateCompleted,
}

// Validate reports every problem with o at once.
func (o Options) Validate() error {
	var errs []error
	if o.ByArgs && len(o.ByArgFields) > 0 {
		errs = append(errs, errors.New("ByArgs and ByArgFields are mutually exclusive"))
	}
	for _, f := range o.ByArgFields {
		if f == "" {
			errs = append(errs, errors.New("ByArgFields contains an empty field name"))
			break
		}
	}
	if o.ByPeriod < 0 || (o.ByPeriod > 0 && (o.ByPeriod < time.Second || o.ByPeriod%time.Second != 0)) {
		errs = append(errs, fmt.Errorf("ByPeriod %s must be a whole number of seconds >= 1s", o.ByPeriod))
	}
	for _, s := range o.ByState {
		if !s.Valid() {
			errs = append(errs, fmt.Errorf("ByState contains unknown state %q", s))
		}
	}
	if len(o.ByState) > 0 {
		// Transitions between pending states are applied by maintenance in
		// bulk; excluding one of them could make such an update violate the
		// unique index, so all four are required.
		for _, p := range job.PendingStates {
			if !slices.Contains(o.ByState, p) {
				errs = append(errs, fmt.Errorf("ByState must include %s", p))
			}
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrInvalidOptions, errors.Join(errs...))
}

// States returns ByState, or DefaultStates when it is empty.
func (o Options) States() []job.State {
	if len(o.ByState) == 0 {
		return DefaultStates
	}
	return o.ByState
}

// Mask returns the bit(8) literal for states; position i is job.AllStates[i].
func Mask(states []job.State) string {
	b := []byte("00000000")
	for _, s := range states {
		if i := s.Bit(); i >= 0 {
			b[i] = '1'
		}
	}
	return string(b)
}

// ParseMask is the inverse of Mask; unknown or unset bits are ignored.
func ParseMask(mask string) []job.State {
	var out []job.State
	for i, s := range job.AllStates {
		if i < len(mask) && mask[i] == '1' {
			out = append(out, s)
		}
	}
	return out
}

// PeriodBucket returns the start of the period containing now, in integer
// Unix seconds: floor(now / period) * period.
func PeriodBucket(now time.Time, period time.Duration) int64 {
	p := int64(period / time.Second)
	if p <= 0 {
		return now.Unix()
	}
	s := now.Unix()
	q := s / p
	if s%p != 0 && s < 0 {
		q--
	}
	return q * p
}

// Identity builds the identity object whose canonical form Postgres hashes
// into unique_key: {"kind", "args"} plus "queue" when ByQueue and
// "period" when ByPeriod. args must be a JSON value; with ByArgFields it must
// be an object. Raw argument bytes are copied verbatim so numbers keep full
// precision.
func Identity(kind, queue string, args []byte, o Options, now time.Time) ([]byte, error) {
	selected, err := selectArgs(args, o)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.WriteString(`{"kind":`)
	buf.Write(mustString(kind))
	buf.WriteString(`,"args":`)
	buf.Write(selected)
	if o.ByQueue {
		buf.WriteString(`,"queue":`)
		buf.Write(mustString(queue))
	}
	if o.ByPeriod > 0 {
		buf.WriteString(`,"period":`)
		buf.WriteString(strconv.FormatInt(PeriodBucket(now, o.ByPeriod), 10))
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func selectArgs(args []byte, o Options) ([]byte, error) {
	switch {
	case o.ByArgs:
		if !json.Valid(args) {
			return nil, errors.New("unique: args are not valid JSON")
		}
		return args, nil
	case len(o.ByArgFields) > 0:
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(args, &fields); err != nil || fields == nil {
			return nil, errors.New("unique: ByArgFields requires args to be a JSON object")
		}
		names := slices.Clone(o.ByArgFields)
		slices.Sort(names)
		names = slices.Compact(names)
		var buf bytes.Buffer
		buf.WriteByte('{')
		n := 0
		for _, name := range names {
			v, ok := fields[name]
			if !ok {
				continue
			}
			if n > 0 {
				buf.WriteByte(',')
			}
			buf.Write(mustString(name))
			buf.WriteByte(':')
			buf.Write(v)
			n++
		}
		buf.WriteByte('}')
		return buf.Bytes(), nil
	default:
		return []byte("{}"), nil
	}
}

func mustString(s string) []byte {
	b, err := json.Marshal(s)
	if err != nil { // strings always marshal
		panic(err)
	}
	return b
}
