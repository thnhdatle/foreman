// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FMN-13: the README quickstart, run verbatim on a fresh clone, starts
// Postgres, runs examples/basic and completes a job in under 5 minutes.
func TestAcceptance_FMN13_Quickstart(t *testing.T) {
	th := loadThresholds(t, "FMN-13")
	limit := time.Duration(th.Int(t, "duration_max_s")) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 2*limit)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", filepath.Join(repoDir(), "scripts", "quickstart-test.sh"))
	cmd.Env = append(os.Environ(), "QUICKSTART_KEEP_STACK=1")
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	start := time.Now()
	err := cmd.Run()
	elapsed := time.Since(start)
	t.Log(out.String())
	completed := strings.Contains(out.String(), "job completed")
	success := err == nil && completed
	measured := map[string]any{"duration_s": elapsed.Seconds(), "success": success, "job_completed": completed}
	pass := success == th.Bool(t, "success") && elapsed < limit
	writeResult(t, "FMN-13", measured, pass, nil)
	require.NoError(t, err)
	assert.True(t, completed, "the example job completed")
	assert.Less(t, elapsed, limit)
}
