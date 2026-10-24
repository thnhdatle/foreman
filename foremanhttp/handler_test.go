// SPDX-License-Identifier: Apache-2.0

package foremanhttp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/thnhdatle/foreman"
)

func TestRoutesMatchOpenAPI(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("../api/openapi.yaml")
	require.NoError(t, err)
	var doc struct {
		Paths map[string]map[string]any `yaml:"paths"`
	}
	require.NoError(t, yaml.Unmarshal(data, &doc))
	var spec []string
	for path, ops := range doc.Paths {
		for method := range ops {
			spec = append(spec, strings.ToUpper(method)+" "+path)
		}
	}
	code := make([]string, 0, len(routes))
	for _, r := range routes {
		code = append(code, r.method+" "+r.path)
	}
	slices.Sort(spec)
	slices.Sort(code)
	assert.Equal(t, spec, code)
}

func newHandler(t *testing.T) http.Handler {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://nobody@127.0.0.1:1/none?connect_timeout=1")
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	c, err := foreman.NewClient(pool, foreman.Config{})
	require.NoError(t, err)
	return Handler(c)
}

func TestHandler_ValidationAndEnvelope(t *testing.T) {
	t.Parallel()
	h := newHandler(t)
	tests := []struct {
		method, target string
		status         int
		code           string
	}{
		{http.MethodGet, "/v1/jobs?limit=0", http.StatusBadRequest, "invalid_argument"},
		{http.MethodGet, "/v1/jobs?limit=abc", http.StatusBadRequest, "invalid_argument"},
		{http.MethodGet, "/v1/jobs/abc", http.StatusBadRequest, "invalid_argument"},
		{http.MethodPost, "/v1/jobs/-1/retry", http.StatusBadRequest, "invalid_argument"},
		{http.MethodPost, "/v1/queues/bad%20name/pause", http.StatusBadRequest, "invalid_argument"},
		{http.MethodGet, "/v1/nothing", http.StatusNotFound, "not_found"},
		{http.MethodGet, "/v1/jobs/1", http.StatusInternalServerError, "internal"},
	}
	for _, tc := range tests {
		t.Run(tc.method+" "+tc.target, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequestWithContext(context.Background(), tc.method, tc.target, http.NoBody)
			req.Header.Set(RequestIDHeader, "req-123")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			assert.Equal(t, tc.status, rec.Code)
			assert.Equal(t, "req-123", rec.Header().Get(RequestIDHeader))
			assert.Contains(t, rec.Body.String(), `"code":"`+tc.code+`"`)
			assert.Contains(t, rec.Body.String(), `"request_id":"req-123"`)
		})
	}
}

func TestHandler_GeneratesRequestIDAndRecovers(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/nothing", http.NoBody)
	req.Header.Set(RequestIDHeader, "bad id with spaces")
	newHandler(t).ServeHTTP(rec, req)
	assert.Len(t, rec.Header().Get(RequestIDHeader), 26, "invalid ids are replaced by a ULID")

	rec = httptest.NewRecorder()
	recoverer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") })).
		ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", http.NoBody))
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), `"code":"internal"`)
}

func TestMulti(t *testing.T) {
	t.Parallel()
	assert.Equal(t, []string{"a", "b", "c"}, multi([]string{"a, b", "", "c"}))
	assert.Empty(t, multi(nil))
}
