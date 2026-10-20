// SPDX-License-Identifier: Apache-2.0

package foremanhttp

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/thnhdatle/foreman"
)

// route is one endpoint; routes is the source for the OpenAPI drift test.
type route struct {
	method, path string
}

var routes = []route{
	{http.MethodGet, "/v1/jobs"},
	{http.MethodGet, "/v1/jobs/{id}"},
	{http.MethodPost, "/v1/jobs/{id}/retry"},
	{http.MethodPost, "/v1/jobs/{id}/cancel"},
	{http.MethodPost, "/v1/jobs/{id}/discard"},
	{http.MethodGet, "/v1/queues"},
	{http.MethodPost, "/v1/queues/{name}/pause"},
	{http.MethodPost, "/v1/queues/{name}/resume"},
	{http.MethodGet, "/v1/leader"},
}

// Handler returns the admin JSON API for client. Mount it under any prefix
// with http.StripPrefix. It has no authentication: the embedding
// application must protect it (for example with its own middleware).
func Handler(client *foreman.Client) http.Handler {
	h := &handler{client: client}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/jobs", h.listJobs)
	mux.HandleFunc("GET /v1/jobs/{id}", h.getJob)
	mux.HandleFunc("POST /v1/jobs/{id}/retry", h.jobAction(client.JobRetry))
	mux.HandleFunc("POST /v1/jobs/{id}/cancel", h.jobAction(client.JobCancel))
	mux.HandleFunc("POST /v1/jobs/{id}/discard", h.jobAction(client.JobDiscard))
	mux.HandleFunc("GET /v1/queues", h.listQueues)
	mux.HandleFunc("POST /v1/queues/{name}/pause", h.queueAction(true))
	mux.HandleFunc("POST /v1/queues/{name}/resume", h.queueAction(false))
	mux.HandleFunc("GET /v1/leader", h.leader)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, r, http.StatusNotFound, "not_found", "no such endpoint")
	})
	return recoverer(requestID(mux))
}

type handler struct {
	client *foreman.Client
}

// RequestIDHeader carries the request id.
const RequestIDHeader = "X-Request-Id"

var requestIDRe = regexp.MustCompile(`^[A-Za-z0-9._\-]{1,128}$`)

type ridKey struct{}

func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestIDHeader)
		if !requestIDRe.MatchString(id) {
			id = ulid.Make().String()
		}
		w.Header().Set(RequestIDHeader, id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ridKey{}, id)))
	})
}

func recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				writeError(w, r, http.StatusInternalServerError, "internal", "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	rid, _ := r.Context().Value(ridKey{}).(string)
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: msg, RequestID: rid}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeClientError maps Foreman errors to the error envelope (errors.md).
func writeClientError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, foreman.ErrNotFound):
		writeError(w, r, http.StatusNotFound, "not_found", "job not found")
	case errors.Is(err, foreman.ErrInvalidArgument):
		writeError(w, r, http.StatusBadRequest, "invalid_argument", err.Error())
	case errors.Is(err, foreman.ErrInvalidTransition):
		writeError(w, r, http.StatusConflict, "invalid_transition", err.Error())
	case errors.Is(err, foreman.ErrUniqueConflict):
		writeError(w, r, http.StatusConflict, "unique_conflict", err.Error())
	default:
		writeError(w, r, http.StatusInternalServerError, "internal", "internal error")
	}
}

func (h *handler) listJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	p := &foreman.JobListParams{Kinds: multi(q["kind"]), Queues: multi(q["queue"]), Cursor: q.Get("cursor")}
	for _, s := range multi(q["state"]) {
		p.States = append(p.States, foreman.State(s))
	}
	if l := q.Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 1 || n > foreman.MaxListLimit {
			writeError(w, r, http.StatusBadRequest, "invalid_argument",
				fmt.Sprintf("limit must be an integer between 1 and %d", foreman.MaxListLimit))
			return
		}
		p.Limit = n
	}
	res, err := h.client.JobList(r.Context(), p)
	if err != nil {
		writeClientError(w, r, err)
		return
	}
	out := listBody[jobJSON]{Data: make([]jobJSON, len(res.Jobs)), NextCursor: res.NextCursor}
	for i, j := range res.Jobs {
		out.Data[i] = toJSON(j)
	}
	writeJSON(w, http.StatusOK, out)
}

// multi splits repeated and comma-separated query values.
func multi(vals []string) []string {
	var out []string
	for _, v := range vals {
		for _, p := range strings.Split(v, ",") {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
	}
	return out
}

func jobID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, r, http.StatusBadRequest, "invalid_argument", "job id must be a positive integer")
		return 0, false
	}
	return id, true
}

func (h *handler) getJob(w http.ResponseWriter, r *http.Request) {
	id, ok := jobID(w, r)
	if !ok {
		return
	}
	j, err := h.client.JobGet(r.Context(), id)
	if err != nil {
		writeClientError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toJSON(j))
}

func (h *handler) jobAction(fn func(context.Context, int64) (*foreman.JobRow, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := jobID(w, r)
		if !ok {
			return
		}
		j, err := fn(r.Context(), id)
		if err != nil {
			writeClientError(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, toJSON(j))
	}
}

func (h *handler) listQueues(w http.ResponseWriter, r *http.Request) {
	qs, err := h.client.QueueList(r.Context())
	if err != nil {
		writeClientError(w, r, err)
		return
	}
	out := listBody[queueJSON]{Data: make([]queueJSON, len(qs))}
	for i, q := range qs {
		out.Data[i] = queueJSON{
			Name: q.Name, Paused: q.Paused, PausedAt: q.PausedAt, LastSeenAt: q.LastSeenAt,
			Available: q.Available, Scheduled: q.Scheduled, Running: q.Running, Retryable: q.Retryable,
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *handler) queueAction(pause bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		var err error
		if pause {
			err = h.client.QueuePause(r.Context(), name)
		} else {
			err = h.client.QueueResume(r.Context(), name)
		}
		if err != nil {
			writeClientError(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, queueStateJSON{Queue: name, Paused: pause})
	}
}

func (h *handler) leader(w http.ResponseWriter, r *http.Request) {
	l, err := h.client.LeaderGet(r.Context())
	if errors.Is(err, foreman.ErrNotFound) {
		writeError(w, r, http.StatusNotFound, "not_found", "no client holds the leader lease")
		return
	}
	if err != nil {
		writeClientError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, leaderJSON{LeaderID: l.LeaderID, ElectedAt: l.ElectedAt, ExpiresAt: l.ExpiresAt})
}

type listBody[T any] struct {
	Data       []T    `json:"data"`
	NextCursor string `json:"next_cursor,omitempty"`
}

type jobJSON struct {
	ID                int64           `json:"id"`
	Kind              string          `json:"kind"`
	Queue             string          `json:"queue"`
	State             string          `json:"state"`
	Args              json.RawMessage `json:"args"`
	Priority          int             `json:"priority"`
	Attempt           int             `json:"attempt"`
	MaxAttempts       int             `json:"max_attempts"`
	ScheduledAt       time.Time       `json:"scheduled_at"`
	CreatedAt         time.Time       `json:"created_at"`
	AttemptedAt       *time.Time      `json:"attempted_at,omitempty"`
	FinalizedAt       *time.Time      `json:"finalized_at,omitempty"`
	HeartbeatAt       *time.Time      `json:"heartbeat_at,omitempty"`
	CancelRequestedAt *time.Time      `json:"cancel_requested_at,omitempty"`
	AttemptedBy       []string        `json:"attempted_by"`
	Errors            []errorJSON     `json:"errors"`
	Tags              []string        `json:"tags"`
	Metadata          json.RawMessage `json:"metadata"`
	UniqueKey         string          `json:"unique_key,omitempty"`
	ConcurrencyKey    string          `json:"concurrency_key,omitempty"`
	TimeoutMS         int64           `json:"timeout_ms,omitempty"`
}

type errorJSON struct {
	At      time.Time `json:"at"`
	Attempt int       `json:"attempt"`
	Error   string    `json:"error"`
	Trace   string    `json:"trace,omitempty"`
}

type queueJSON struct {
	Name       string     `json:"name"`
	Paused     bool       `json:"paused"`
	PausedAt   *time.Time `json:"paused_at,omitempty"`
	LastSeenAt *time.Time `json:"last_seen_at,omitempty"`
	Available  int64      `json:"available"`
	Scheduled  int64      `json:"scheduled"`
	Running    int64      `json:"running"`
	Retryable  int64      `json:"retryable"`
}

type queueStateJSON struct {
	Queue  string `json:"queue"`
	Paused bool   `json:"paused"`
}

type leaderJSON struct {
	LeaderID  string    `json:"leader_id"`
	ElectedAt time.Time `json:"elected_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

func toJSON(j *foreman.JobRow) jobJSON {
	out := jobJSON{
		ID: j.ID, Kind: j.Kind, Queue: j.Queue, State: string(j.State), Args: j.Args, Priority: j.Priority,
		Attempt: j.Attempt, MaxAttempts: j.MaxAttempts, ScheduledAt: j.ScheduledAt, CreatedAt: j.CreatedAt,
		AttemptedAt: j.AttemptedAt, FinalizedAt: j.FinalizedAt, HeartbeatAt: j.HeartbeatAt,
		CancelRequestedAt: j.CancelRequestedAt, AttemptedBy: nonNil(j.AttemptedBy), Tags: nonNil(j.Tags),
		Metadata: j.Metadata, ConcurrencyKey: j.ConcurrencyKey, TimeoutMS: j.Timeout.Milliseconds(),
		Errors: make([]errorJSON, len(j.Errors)),
	}
	for i, e := range j.Errors {
		out.Errors[i] = errorJSON{At: e.At, Attempt: e.Attempt, Error: e.Error, Trace: e.Trace}
	}
	if len(j.UniqueKey) > 0 {
		out.UniqueKey = hex.EncodeToString(j.UniqueKey)
	}
	return out
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
