// SPDX-License-Identifier: Apache-2.0

// Package cli implements the foreman admin CLI: migrate, jobs, queues,
// leader, periodic, stats, config and version commands. Commands
// run in process so tests can compare their output with golden files.
//
//	code := cli.Run(ctx, os.Args[1:], cli.IO{Stdout: os.Stdout, Stderr: os.Stderr, Getenv: os.Getenv}, build)
package cli
