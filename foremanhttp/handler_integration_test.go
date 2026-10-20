// SPDX-License-Identifier: Apache-2.0

//go:build integration

package foremanhttp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman"
	"github.com/thnhdatle/foreman/internal/testutil/pgtest"
)

var db *pgtest.Database

func TestMain(m *testing.M) { os.Exit(pgtest.Main(m, &db)) }

type reportArgs struct {
	Name string `json:"name"`
}

func (reportArgs) Kind() string { return "report" }

func call(t *testing.T, h http.Handler, method, target string, out any) int {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), method, target, http.NoBody))
	if out != nil {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), out), rec.Body.String())
	}
	return rec.Code
}

func TestHandler_EndToEnd(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := db.Pool(t)
	c, err := foreman.NewClient(pool, foreman.Config{Schema: db.Schema(t, pool)})
	require.NoError(t, err)
	prefixed := http.NewServeMux()
	prefixed.Handle("/admin/", http.StripPrefix("/admin", Handler(c)))
	for i := 0; i < 3; i++ {
		_, err := c.Insert(ctx, reportArgs{Name: "r"}, &foreman.InsertOpts{Queue: "reports", Tags: []string{"t"}})
		require.NoError(t, err)
	}

	var page struct {
		Data       []jobJSON `json:"data"`
		NextCursor string    `json:"next_cursor"`
	}
	require.Equal(t, http.StatusOK, call(t, prefixed, http.MethodGet, "/admin/v1/jobs?limit=2&state=available&kind=report", &page))
	require.Len(t, page.Data, 2)
	require.NotEmpty(t, page.NextCursor)
	assert.Equal(t, "reports", page.Data[0].Queue)
	assert.JSONEq(t, `{"name":"r"}`, string(page.Data[0].Args))
	var rest struct {
		Data []jobJSON `json:"data"`
	}
	require.Equal(t, http.StatusOK, call(t, prefixed, http.MethodGet, "/admin/v1/jobs?cursor="+page.NextCursor, &rest))
	assert.Len(t, rest.Data, 1)
	id := page.Data[0].ID

	var j jobJSON
	require.Equal(t, http.StatusOK, call(t, prefixed, http.MethodPost, "/admin/v1/jobs/"+itoa(id)+"/cancel", &j))
	assert.Equal(t, "cancelled", j.State)
	require.Equal(t, http.StatusOK, call(t, prefixed, http.MethodPost, "/admin/v1/jobs/"+itoa(id)+"/retry", &j))
	assert.Equal(t, "available", j.State)
	require.Equal(t, http.StatusOK, call(t, prefixed, http.MethodPost, "/admin/v1/jobs/"+itoa(id)+"/discard", &j))
	assert.Equal(t, "discarded", j.State)
	require.Equal(t, http.StatusOK, call(t, prefixed, http.MethodGet, "/admin/v1/jobs/"+itoa(id), &j))
	assert.Equal(t, "discarded", j.State)
	var e errorBody
	assert.Equal(t, http.StatusConflict, call(t, prefixed, http.MethodPost, "/admin/v1/jobs/"+itoa(id)+"/cancel", &e))
	assert.Equal(t, "invalid_transition", e.Error.Code)
	assert.Equal(t, http.StatusNotFound, call(t, prefixed, http.MethodGet, "/admin/v1/jobs/999999", &e))
	assert.Equal(t, "not_found", e.Error.Code)

	var qs struct {
		Data []queueJSON `json:"data"`
	}
	var st queueStateJSON
	require.Equal(t, http.StatusOK, call(t, prefixed, http.MethodPost, "/admin/v1/queues/reports/pause", &st))
	assert.True(t, st.Paused)
	require.Equal(t, http.StatusOK, call(t, prefixed, http.MethodGet, "/admin/v1/queues", &qs))
	require.Len(t, qs.Data, 1)
	assert.True(t, qs.Data[0].Paused)
	assert.EqualValues(t, 2, qs.Data[0].Available)
	require.Equal(t, http.StatusOK, call(t, prefixed, http.MethodPost, "/admin/v1/queues/reports/resume", &st))
	assert.False(t, st.Paused)
	assert.Equal(t, http.StatusNotFound, call(t, prefixed, http.MethodGet, "/admin/v1/leader", &e))

	_, err = pool.Exec(ctx, `INSERT INTO "`+c.Schema()+`".foreman_leader VALUES ('default', 'node-1', now(), now() + interval '15 seconds')`)
	require.NoError(t, err)
	var l leaderJSON
	require.Equal(t, http.StatusOK, call(t, prefixed, http.MethodGet, "/admin/v1/leader", &l))
	assert.Equal(t, "node-1", l.LeaderID)
}

func itoa(id int64) string { return strconv.FormatInt(id, 10) }
