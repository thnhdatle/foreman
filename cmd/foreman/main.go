// SPDX-License-Identifier: Apache-2.0

// Command foreman is the Foreman admin CLI.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/thnhdatle/foreman/internal/cli"
)

// Set by the linker (-X main.version=...).
var (
	version   = "dev"
	commit    = "unknown"
	buildDate = "unknown"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := cli.Run(ctx, os.Args[1:], cli.IO{Stdout: os.Stdout, Stderr: os.Stderr, Getenv: os.Getenv},
		cli.BuildInfo{Version: version, Commit: commit, BuildDate: buildDate})
	stop()
	os.Exit(code)
}
