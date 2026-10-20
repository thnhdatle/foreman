// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman/acceptance/internal/accjobs"
	"github.com/thnhdatle/foreman/migrate"
)

const (
	defaultDBURL     = "postgres://foreman:foreman@localhost:55432/foreman?sslmode=disable"
	defaultProxyURL  = "postgres://foreman:foreman@localhost:55433/foreman?sslmode=disable"
	defaultToxiproxy = "http://localhost:58474"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func dbURL() string        { return envOr("FOREMAN_ACC_DATABASE_URL", defaultDBURL) }
func proxyURL() string     { return envOr("FOREMAN_ACC_PROXY_DATABASE_URL", defaultProxyURL) }
func toxiproxyURL() string { return envOr("FOREMAN_ACC_TOXIPROXY_URL", defaultToxiproxy) }

// newPool opens a pool closed at the end of the test.
func newPool(tb testing.TB, url string, maxConns int32) *pgxpool.Pool {
	tb.Helper()
	cfg, err := pgxpool.ParseConfig(url)
	require.NoError(tb, err)
	cfg.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(tb, err)
	tb.Cleanup(pool.Close)
	return pool
}

// freshSchema recreates schema with the Foreman migrations and the
// executions table.
func freshSchema(tb testing.TB, pool *pgxpool.Pool, schema string) {
	tb.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %q CASCADE`, schema))
	require.NoError(tb, err)
	require.NoError(tb, migrate.Up(ctx, pool, migrate.WithSchema(schema)))
	require.NoError(tb, accjobs.CreateExecutions(ctx, pool, schema))
}

// testLogger writes warnings and errors to the test log.
func testLogger(tb testing.TB) *slog.Logger {
	tb.Helper()
	return slog.New(slog.NewTextHandler(&tbWriter{tb: tb}, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

type tbWriter struct {
	mu sync.Mutex
	tb testing.TB
}

func (w *tbWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tb.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

func countWhere(tb testing.TB, pool *pgxpool.Pool, schema, where string, args ...any) int64 {
	tb.Helper()
	var n int64
	err := pool.QueryRow(context.Background(),
		fmt.Sprintf(`SELECT count(*) FROM %q.foreman_jobs WHERE %s`, schema, where), args...).Scan(&n)
	require.NoError(tb, err)
	return n
}

// percentile returns the p-th percentile (0..100) of ds by nearest rank.
func percentile(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := slices.Clone(ds)
	slices.Sort(s)
	rank := int(p/100*float64(len(s))+0.999999) - 1
	return s[max(0, min(rank, len(s)-1))]
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

// waitFor polls cond every interval until it is true or timeout passes.
func waitFor(timeout, interval time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		<-tick.C
	}
}

var (
	workerOnce sync.Once
	workerBin  string
	workerErr  error
)

// buildWorker compiles acceptance/cmd/worker once per test binary.
func buildWorker(tb testing.TB) string {
	tb.Helper()
	workerOnce.Do(func() {
		dir, err := os.MkdirTemp("", "foreman-acc-worker")
		if err != nil {
			workerErr = err
			return
		}
		workerBin = filepath.Join(dir, "worker")
		out, err := exec.CommandContext(context.Background(), "go", "build", "-o", workerBin, "./cmd/worker").CombinedOutput()
		if err != nil {
			workerErr = fmt.Errorf("build worker: %w: %s", err, out)
		}
	})
	require.NoError(tb, workerErr)
	return workerBin
}

// proc is a running worker process.
type proc struct {
	cmd    *exec.Cmd
	id     string
	stderr *bytes.Buffer
	done   chan struct{}
}

// startWorker starts a worker process, waits for its "ready" line and kills
// it at the end of the test.
func startWorker(tb testing.TB, args ...string) *proc {
	tb.Helper()
	p, err := spawnWorker(buildWorker(tb), args...)
	require.NoError(tb, err)
	tb.Cleanup(func() { p.kill() })
	return p
}

// spawnWorker starts a worker process and waits for its "ready" line; it is
// safe to call from any goroutine. The caller must kill the process.
func spawnWorker(bin string, args ...string) (*proc, error) {
	cmd := exec.CommandContext(context.Background(), bin, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	p := &proc{cmd: cmd, stderr: &bytes.Buffer{}, done: make(chan struct{})}
	cmd.Stderr = &lockedWriter{w: p.stderr}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start worker: %w", err)
	}
	ready := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if line := sc.Text(); strings.HasPrefix(line, "ready ") {
				select {
				case ready <- strings.TrimPrefix(line, "ready "):
				default:
				}
			}
		}
		_, _ = io.Copy(io.Discard, stdout)
	}()
	go func() {
		_ = cmd.Wait()
		close(p.done)
	}()
	select {
	case p.id = <-ready:
		return p, nil
	case <-p.done:
		return nil, fmt.Errorf("worker exited before ready: %s", p.stderr.String())
	case <-time.After(60 * time.Second):
		p.kill()
		return nil, fmt.Errorf("worker not ready after 60s: %s", p.stderr.String())
	}
}

func (p *proc) signal(sig syscall.Signal) { _ = p.cmd.Process.Signal(sig) }

func (p *proc) kill() {
	select {
	case <-p.done:
	default:
		_ = p.cmd.Process.Kill()
		<-p.done
	}
}

func (p *proc) stop(timeout time.Duration) {
	p.signal(syscall.SIGTERM)
	select {
	case <-p.done:
	case <-time.After(timeout):
		p.kill()
	}
}

func (p *proc) wait(timeout time.Duration) bool {
	select {
	case <-p.done:
		return true
	case <-time.After(timeout):
		return false
	}
}

type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(b) //nolint:wrapcheck // test helper
}

// toxiproxy enables or disables the postgres proxy.
func toxiproxySetEnabled(tb testing.TB, enabled bool) {
	tb.Helper()
	body := strings.NewReader(fmt.Sprintf(`{"enabled": %t}`, enabled))
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, toxiproxyURL()+"/proxies/postgres", body)
	require.NoError(tb, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(tb, err)
	defer func() { _ = resp.Body.Close() }()
	require.Less(tb, resp.StatusCode, 300, "toxiproxy status %d", resp.StatusCode)
}

// scrapeMetric fetches a counter value from a worker's /metrics endpoint.
func scrapeMetric(tb testing.TB, addr, name string) float64 {
	tb.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+addr+"/metrics", http.NoBody)
	require.NoError(tb, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(tb, err)
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	require.NoError(tb, err)
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, name+" ") {
			var v float64
			_, err := fmt.Sscanf(strings.TrimPrefix(line, name+" "), "%g", &v)
			require.NoError(tb, err)
			return v
		}
	}
	return 0
}
