// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

// probe checks one `requires` capability and returns a
// human-readable reason when it is unavailable.
func probe(ctx context.Context, capability string) (ok bool, reason string) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	switch capability {
	case "docker":
		return runProbe(ctx, "docker", "info", "--format", "{{.ServerVersion}}")
	case "mac":
		if runtime.GOOS == "darwin" && runtime.GOARCH == "arm64" {
			return true, ""
		}
		return false, fmt.Sprintf("host is %s/%s, not the reference Mac", runtime.GOOS, runtime.GOARCH)
	default:
		return false, fmt.Sprintf("unknown capability %q", capability)
	}
}

func runProbe(ctx context.Context, name string, args ...string) (ok bool, reason string) {
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput() //nolint:gosec // G204: fixed probe commands
	if err != nil {
		return false, fmt.Sprintf("`%s %s` failed: %v: %s", name, strings.Join(args, " "), err,
			strings.TrimSpace(string(out)))
	}
	return true, ""
}
