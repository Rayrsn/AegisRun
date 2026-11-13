package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/aegisrun/aegisrun/control-go/internal/orchestrator"
)

// HITLHandler serves approval decision webhooks and operator review callbacks.
type HITLHandler struct {
	engine *orchestrator.Engine
}

// NewHITLHandler constructs an HITLHandler backed by engine.
func NewHITLHandler(engine *orchestrator.Engine) *HITLHandler {
	return &HITLHandler{engine: engine}
}

type decideApprovalRequest struct {
	Decision  string `json:"decision"`
	DecidedBy string `json:"decided_by,omitempty"`
}

// HandleDecideApproval handles `POST /v1/approvals/{approval_token}/decide`.
func (h *HITLHandler) HandleDecideApproval(w http.ResponseWriter, r *http.Request) {
	token := r.PathValue("approval_token")
	if token == "" {
		writeJSONError(w, http.StatusBadRequest, "missing approval_token path parameter")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 64*1024)
	var req decideApprovalRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON payload: %v", err))
		return
	}

	res, err := h.engine.DecideApproval(r.Context(), token, req.Decision, req.DecidedBy)
	if err != nil {
		mapOrchestratorError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, res)
}

