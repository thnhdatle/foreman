// SPDX-License-Identifier: Apache-2.0

package foreman

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Generic type safety: each program under testdata/compile
// misuses the API and must be rejected by the type checker.
func TestCompile_MisuseDoesNotCompile(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"worker_args_mismatch": "does not implement foreman.Worker[EmailArgs]",
		"insert_non_jobargs":   "does not implement foreman.JobArgs",
		"job_of_non_jobargs":   "string does not satisfy foreman.JobArgs",
		"decode_non_jobargs":   "does not satisfy foreman.JobArgs",
		"completetx_not_a_job": "does not match *foreman.Job[T]",
	}
	for dir, want := range cases {
		t.Run(dir, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			out, err := exec.CommandContext(ctx, "go", "vet", "./testdata/compile/"+dir).CombinedOutput()
			require.Error(t, err, "misuse must not compile:\n%s", out)
			assert.Contains(t, string(out), want)
		})
	}
}
