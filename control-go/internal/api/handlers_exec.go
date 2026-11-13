package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/aegisrun/aegisrun/control-go/internal/orchestrator"
	"github.com/aegisrun/aegisrun/control-go/internal/store"
)

const maxExecuteBodyBytes = 4 * 1024 * 1024

// ExecHandler serves execution submission, status query, and SSE streaming routes.
type ExecHandler struct {
	engine *orchestrator.Engine
	broker *store.StreamBroker
}

// NewExecHandler constructs an ExecHandler bound to engine and broker.
func NewExecHandler(engine *orchestrator.Engine, broker *store.StreamBroker) *ExecHandler {
	return &ExecHandler{engine: engine, broker: broker}
}

// HandleExecute handles `POST /v1/execute` in sync, stream (SSE), or async mode.
func (h *ExecHandler) HandleExecute(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxExecuteBodyBytes)
	var req orchestrator.ExecuteRequest
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON payload: %v", err))
		return
	}

	if strings.TrimSpace(req.AgentSessionID) == "" {
		if sessionHeader := strings.TrimSpace(r.Header.Get("X-Agent-Session-ID")); sessionHeader != "" {
			req.AgentSessionID = sessionHeader
		}
	}
	if strings.TrimSpace(req.Mode) == "" {
		if modeHeader := strings.TrimSpace(r.Header.Get("X-Execution-Mode")); modeHeader != "" {
			req.Mode = modeHeader
		} else if strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
			req.Mode = "stream"
		}
	}

	tenantID := TenantFromContext(r.Context())
	if tenantID == "" {
		tenantID = "default"
	}

	if strings.EqualFold(req.Mode, "stream") {
		h.handleExecuteStream(w, r, tenantID, req)
		return
	}

	outcome, err := h.engine.SubmitExecution(r.Context(), tenantID, req)
	if err != nil {
		mapOrchestratorError(w, err)
		return
	}
	if outcome.Replayed {
		w.Header().Set("X-Aegis-Idempotent-Replay", "true")
	}

	if strings.EqualFold(req.Mode, "async") {
		writeJSON(w, http.StatusAccepted, outcome.Response)
		return
	}

	if outcome.Response != nil && outcome.Response.ApprovalRequired {
		writeJSON(w, http.StatusAccepted, outcome.Response)
		return
	}
	writeJSON(w, http.StatusOK, outcome.Response)
}

func (h *ExecHandler) handleExecuteStream(w http.ResponseWriter, r *http.Request, tenantID string, req orchestrator.ExecuteRequest) {
	req.Mode = "stream"
	outcome, err := h.engine.SubmitExecution(r.Context(), tenantID, req)
	if err != nil {
		mapOrchestratorError(w, err)
		return
	}

	if outcome.Replayed {
		w.Header().Set("X-Aegis-Idempotent-Replay", "true")
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSONError(w, http.StatusInternalServerError, "streaming unsupported by response writer")
		return
	}

	ch, subErr := h.broker.Subscribe(r.Context(), outcome.ExecutionID)
	if subErr != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("subscribing to execution stream: %v", subErr))
		return
	}

	setSSEHeaders(w)
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	if outcome.Replayed && outcome.Response != nil {
		// If history expired while idempotency cache persisted, synthesize terminal SSE event when stream ends cleanly.
		writeSSEStream(r.Context(), w, flusher, ch, outcome.Response)
		return
	}
	writeSSEStream(r.Context(), w, flusher, ch, nil)
}

// HandleGetExecution serves `GET /v1/executions/{execution_id}` with tenant ownership check.
func (h *ExecHandler) HandleGetExecution(w http.ResponseWriter, r *http.Request) {
	execID := r.PathValue("execution_id")
	if execID == "" {
		writeJSONError(w, http.StatusBadRequest, "missing execution_id path parameter")
		return
	}

	tenantID := TenantFromContext(r.Context())
	rec, err := h.engine.GetExecutionRecord(r.Context(), tenantID, execID)
	if err != nil {
		mapOrchestratorError(w, err)
		return
	}

	resp := map[string]any{
		"execution_id":      rec.ID,
		"tenant_id":         rec.TenantID,
		"agent_session_id":  rec.AgentSessionID,
		"tool_name":         rec.ToolName,
		"runtime":           rec.Runtime,
		"status":            rec.Status,
		"requires_approval": rec.RequiresApproval,
		"approved_by":       rec.ApprovedBy,
		"worker_node_id":    rec.WorkerNodeID,
		"exit_code":         rec.ExitCode,
		"setup_duration_us": rec.SetupDurationUs,
		"exec_duration_us":  rec.ExecDurationUs,
		"peak_memory_bytes": rec.PeakMemoryBytes,
		"stdout":            string(rec.StdoutTail),
		"stderr":            string(rec.StderrTail),
		"created_at":        rec.CreatedAt,
		"completed_at":      rec.CompletedAt,
	}
	writeJSON(w, http.StatusOK, resp)
}

// HandleStreamExecution serves `GET /v1/executions/{execution_id}/stream` via SSE replay + live subscription.
func (h *ExecHandler) HandleStreamExecution(w http.ResponseWriter, r *http.Request) {
	execID := r.PathValue("execution_id")
	if execID == "" {
		writeJSONError(w, http.StatusBadRequest, "missing execution_id path parameter")
		return
	}

	tenantID := TenantFromContext(r.Context())
	if _, err := h.engine.GetExecutionRecord(r.Context(), tenantID, execID); err != nil {
		mapOrchestratorError(w, err)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSONError(w, http.StatusInternalServerError, "streaming unsupported by response writer")
		return
	}

	ch, err := h.broker.Subscribe(r.Context(), execID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("subscribing to execution stream: %v", err))
		return
	}

	setSSEHeaders(w)
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	writeSSEStream(r.Context(), w, flusher, ch, nil)
}

func setSSEHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
}

func writeSSEStream(
	ctx context.Context,
	w http.ResponseWriter,
	flusher http.Flusher,
	ch <-chan *store.StreamEvent,
	fallbackReplay *store.ExecutionResponse,
) {
	var sentEvents int
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-ch:
			if !ok {
				if sentEvents == 0 && fallbackReplay != nil {
					raw, _ := json.Marshal(fallbackReplay)
					_, _ = fmt.Fprintf(w, "id: 1\nevent: completed\ndata: %s\n\n", raw)
					flusher.Flush()
				}
				return
			}
			sentEvents++
			raw, err := json.Marshal(evt)
			if err != nil {
				continue
			}
			eventName := evt.EventType
			if eventName == "" {
				eventName = "message"
			}
			_, _ = fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", strconv.FormatUint(evt.SequenceNum, 10), eventName, raw)
			flusher.Flush()
			if evt.Terminal {
				return
			}
		}
	}
}

func mapOrchestratorError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, orchestrator.ErrDraining):
		writeJSONError(w, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, orchestrator.ErrInvalidRequest):
		writeJSONError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, orchestrator.ErrForbiddenTenant):
		writeJSONError(w, http.StatusForbidden, err.Error())
	case errors.Is(err, store.ErrExecutionNotFound):
		writeJSONError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, store.ErrApprovalNotFound):
		writeJSONError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, store.ErrApprovalAlreadyDecided):
		writeJSONError(w, http.StatusConflict, err.Error())
	case errors.Is(err, store.ErrApprovalExpired):
		writeJSONError(w, http.StatusGone, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		writeJSONError(w, http.StatusGatewayTimeout, "execution wait timed out")
	default:
		writeJSONError(w, http.StatusInternalServerError, err.Error())
	}
}

