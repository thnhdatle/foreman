// SPDX-License-Identifier: Apache-2.0

// Command runner runs the acceptance suite behind `make acceptance`:
// it builds the project from scratch, brings up the compose stack, runs every
// acceptance criterion with no test cache, and writes acceptance/report.md.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const (
	resultPass    = "PASS"
	resultFail    = "FAIL"
	resultSkipped = "Skipped (environment)"
	resultBlocked = "Blocked (environment)"
	envMac        = "mac"
	envCI         = "ci"
)

type options struct {
	only      map[string]bool
	version   string
	criteria  string
	dir       string
	keepUp    bool
	skipBuild bool
	timeout   time.Duration
}

type row struct {
	c         criterion
	result    string
	measured  map[string]any
	threshold map[string]any
	note      string
	env       string
}

func main() {
	os.Exit(run())
}

func parseFlags() options {
	var only string
	var o options
	flag.StringVar(&only, "only", "", "comma-separated criterion ids to run (iteration only)")
	flag.StringVar(&o.version, "version", "dev", "version shown in the report heading")
	flag.StringVar(&o.criteria, "criteria", "acceptance/criteria.yaml", "criteria file")
	flag.StringVar(&o.dir, "dir", "acceptance", "acceptance directory")
	flag.BoolVar(&o.keepUp, "keep-up", false, "leave the compose stack running")
	flag.BoolVar(&o.skipBuild, "skip-build", false, "do not rebuild before running (iteration only)")
	flag.DurationVar(&o.timeout, "timeout", 3*time.Hour, "per-criterion timeout")
	flag.Parse()
	o.only = map[string]bool{}
	for _, id := range strings.Split(only, ",") {
		if id = strings.TrimSpace(id); id != "" {
			o.only[id] = true
		}
	}
	return o
}

func run() int {
	o := parseFlags()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cf, err := loadCriteria(o.criteria)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	env := envMac
	if os.Getenv("CI") != "" {
		env = envCI
	}
	logDir := filepath.Join(o.dir, "logs")
	if err := os.MkdirAll(logDir, 0o750); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	problems := checkCriteria(cf, o.dir)
	for _, p := range problems {
		fmt.Fprintln(os.Stderr, "criteria check:", p)
	}

	dockerOK, dockerReason := probe(ctx, "docker")
	if dockerOK {
		if err := prepare(ctx, o); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		if !o.keepUp {
			defer composeDown()
		}
	}

	rows := make([]row, 0, len(cf.Criteria))
	for _, c := range cf.Criteria {
		if len(o.only) > 0 && !o.only[c.ID] {
			continue
		}
		r := runCriterion(ctx, c, env, logDir, o, probeCache{"docker": {dockerOK, dockerReason}})
		fmt.Printf("%-7s %-22s %s\n", c.ID, r.result, r.note)
		rows = append(rows, r)
	}

	if err := writeReport(filepath.Join(o.dir, "report.md"), o.version, env, rows, problems); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return summarize(rows, problems)
}

func summarize(rows []row, problems []string) int {
	fails := 0
	for _, r := range rows {
		if r.result == resultFail {
			fails++
		}
	}
	fmt.Printf("acceptance: %d criteria, %d FAIL, criteria check problems %d\n", len(rows), fails, len(problems))
	if fails > 0 || len(problems) > 0 {
		return 1
	}
	return 0
}

type probeResult struct {
	ok     bool
	reason string
}

type probeCache map[string]probeResult

func prepare(ctx context.Context, o options) error {
	if !o.skipBuild {
		// Rebuild the project's packages from scratch (no build cache reuse).
		if err := runCmd(ctx, os.Stdout, "go", "build", "-a", "./..."); err != nil {
			return fmt.Errorf("build: %w", err)
		}
	}
	err := runCmd(ctx, os.Stdout, "docker", "compose", "-f", "deploy/docker-compose.yml", "up", "-d", "--wait")
	if err != nil {
		return fmt.Errorf("compose up: %w", err)
	}
	return nil
}

func composeDown() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	_ = runCmd(ctx, os.Stdout, "docker", "compose", "-f", "deploy/docker-compose.yml", "down", "-v", "--remove-orphans")
}

func runCmd(ctx context.Context, out io.Writer, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...) //nolint:gosec // G204: fixed tool names from this program
	cmd.Stdout = out
	cmd.Stderr = out
	return cmd.Run() //nolint:wrapcheck // callers add context
}

func runCriterion(ctx context.Context, c criterion, env, logDir string, o options, cache probeCache) row {
	r := row{c: c, env: env}
	runEnv := env
	for _, req := range c.Requires {
		pr, ok := cache[req]
		if !ok {
			pr.ok, pr.reason = probe(ctx, req)
			cache[req] = pr
		}
		if !pr.ok {
			r.result = resultSkipped
			r.note = fmt.Sprintf("requires %s — probe: %s", req, pr.reason)
			return r
		}
	}
	// Performance thresholds are calibrated for the reference machine; on any
	// other host the CI thresholds apply.
	if c.Kind == "performance" && env == envMac {
		if ok, reason := probe(ctx, "mac"); !ok {
			runEnv = envCI
			r.note = "Mac thresholds blocked (environment): " + reason + "; CI thresholds applied"
		}
	}
	r.threshold = c.Pass[runEnv]
	r.env = runEnv

	_ = os.Remove(filepath.Join(logDir, c.ID+".json"))
	logFile, err := os.Create(filepath.Join(logDir, c.ID+".txt")) //nolint:gosec // G304: fixed log directory
	if err != nil {
		r.result, r.note = resultFail, err.Error()
		return r
	}
	defer func() { _ = logFile.Close() }()

	args := []string{"test", "-tags", "acceptance", "-count=1", "-json", "-timeout", o.timeout.String()}
	if strings.HasPrefix(c.Test, "Benchmark") {
		args = append(args, "-run", "^$", "-bench", "^"+c.Test+"$", "-benchtime", "1x")
	} else {
		args = append(args, "-run", "^"+c.Test+"$")
	}
	args = append(args, "./acceptance/...")
	cmd := exec.CommandContext(ctx, "go", args...) //nolint:gosec // G204: arguments come from criteria.yaml
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Env = append(os.Environ(), "FOREMAN_ACC_ENV="+runEnv, "FOREMAN_ACC_LOG_DIR="+mustAbs(logDir))
	runErr := cmd.Run()

	res, readErr := readResult(filepath.Join(logDir, c.ID+".json"))
	switch {
	case readErr != nil:
		r.result = resultFail
		r.note = strings.TrimSpace(r.note + " no result file: " + readErr.Error())
	case runErr != nil || !res.Pass:
		r.result = resultFail
		r.measured = res.Measured
		if runErr != nil {
			r.note = strings.TrimSpace(r.note + " test exited: " + runErr.Error())
		}
	default:
		r.result = resultPass
		r.measured = res.Measured
	}
	if res != nil && res.Blocked != "" {
		r.result = resultBlocked
		r.note = strings.TrimSpace(r.note + " " + res.Blocked)
	}
	return r
}

type testResult struct {
	ID       string         `json:"id"`
	Measured map[string]any `json:"measured"`
	Pass     bool           `json:"pass"`
	Details  any            `json:"details"`
	Blocked  string         `json:"blocked,omitempty"`
}

func readResult(path string) (*testResult, error) {
	data, err := os.ReadFile(path) //nolint:gosec // G304: fixed log directory
	if err != nil {
		return nil, fmt.Errorf("read result: %w", err)
	}
	var res testResult
	if err := json.Unmarshal(data, &res); err != nil {
		return nil, fmt.Errorf("parse result: %w", err)
	}
	if res.ID == "" {
		return nil, errors.New("result has no id")
	}
	return &res, nil
}

func mustAbs(p string) string {
	abs, err := filepath.Abs(p)
	if err != nil {
		return p
	}
	return abs
}
