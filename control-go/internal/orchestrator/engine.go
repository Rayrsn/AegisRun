package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aegisrun/aegisrun/control-go/internal/hitl"
	"github.com/aegisrun/aegisrun/control-go/internal/idempotency"
	"github.com/aegisrun/aegisrun/control-go/internal/store"
	"github.com/aegisrun/aegisrun/control-go/internal/telemetry"
	"github.com/aegisrun/aegisrun/control-go/pkg/pb"
	"github.com/google/uuid"
)

var (
	ErrDraining          = errors.New("control plane is draining and not accepting new executions")
	ErrInvalidRequest    = errors.New("invalid execution request")
	ErrForbiddenTenant   = errors.New("execution belongs to a different tenant")
	ErrExecutionInFlight = errors.New("execution is still in flight")
)

const (
	defaultWallTimeoutMs  uint32 = 5000
	defaultMemoryMaxBytes uint64 = 128 * 1024 * 1024
	defaultCpuQuotaUs     uint32 = 50000
	defaultPidsMax        uint32 = 64
	defaultTmpfsSizeBytes uint64 = 32 * 1024 * 1024
	maxSyncResponseOutput        = 4 * 1024 * 1024
)

// ResourceLimitsReq represents optional sandbox resource limit overrides on `/v1/execute`.
type ResourceLimitsReq struct {
	WallTimeoutMs  uint32 `json:"wall_timeout_ms,omitempty"`
	MemoryMaxBytes uint64 `json:"memory_max_bytes,omitempty"`
	CpuQuotaUs     uint32 `json:"cpu_quota_us,omitempty"`
	PidsMax        uint32 `json:"pids_max,omitempty"`
	TmpfsSizeBytes uint64 `json:"tmpfs_size_bytes,omitempty"`
}

// ExecuteRequest is the HTTP JSON payload accepted by `POST /v1/execute`.
type ExecuteRequest struct {
	AgentSessionID     string            `json:"agent_session_id"`
	ToolName           string            `json:"tool_name"`
	Runtime            string            `json:"runtime"`
	EntrypointCode     string            `json:"entrypoint_code"`
	Args               json.RawMessage   `json:"args,omitempty"`
	StdinPayload       string            `json:"stdin_payload,omitempty"`
	EnvVars            map[string]string `json:"env_vars,omitempty"`
	Limits             ResourceLimitsReq `json:"limits,omitempty"`
	EnableLoopbackOnly *bool             `json:"enable_loopback_only,omitempty"`
	Mode               string            `json:"mode,omitempty"`
	RequiresApproval   bool              `json:"requires_approval,omitempty"`
	WebhookURL         string            `json:"webhook_url,omitempty"`
	ApprovalTTLSeconds int               `json:"approval_ttl_seconds,omitempty"`
}

// ExecuteOutcome captures the synchronous, cached, async, or stream handle produced by SubmitExecution.
type ExecuteOutcome struct {
	ExecutionID string
	Status      string
	Replayed    bool
	Piggybacked bool
	Response    *store.ExecutionResponse
}

// ApprovalDecisionResult is returned after `/v1/approvals/{token}/decide` updates state.
type ApprovalDecisionResult struct {
	ExecutionID string `json:"execution_id"`
	Decision    string `json:"decision"`
	Status      string `json:"status"`
}

// EngineConfig configures Engine timeouts and HITL defaults.
type EngineConfig struct {
	DefaultApprovalTTL time.Duration
}

// Engine coordinates idempotency locking, HITL gating, failover scheduling, streaming, and audit persistence.
type Engine struct {
	store     store.Store
	broker    *store.StreamBroker
	locker    *idempotency.Locker
	pool      *WorkerPool
	scheduler *Scheduler
	notifier  *hitl.Notifier
	metrics   *telemetry.Metrics
	logger    *slog.Logger
	cfg       EngineConfig

	draining atomic.Bool
	rootCtx  context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewEngine creates an Engine managing background sandbox streams until Shutdown.
func NewEngine(
	st store.Store,
	broker *store.StreamBroker,
	locker *idempotency.Locker,
	pool *WorkerPool,
	scheduler *Scheduler,
	notifier *hitl.Notifier,
	metrics *telemetry.Metrics,
	cfg EngineConfig,
	logger *slog.Logger,
) *Engine {
	if cfg.DefaultApprovalTTL <= 0 {
		cfg.DefaultApprovalTTL = 5 * time.Minute
	}
	if logger == nil {
		logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Engine{
		store:     st,
		broker:    broker,
		locker:    locker,
		pool:      pool,
		scheduler: scheduler,
		notifier:  notifier,
		metrics:   metrics,
		logger:    logger,
		cfg:       cfg,
		rootCtx:   ctx,
		cancel:    cancel,
	}
}

// BeginDrain causes new execution submissions to be rejected with ErrDraining.
func (e *Engine) BeginDrain() {
	e.draining.Store(true)
}

// IsDraining reports whether the engine is shutting down.
func (e *Engine) IsDraining() bool {
	return e.draining.Load()
}

// Shutdown drains active background executions and closes subsystems cleanly.
func (e *Engine) Shutdown(ctx context.Context) error {
	e.BeginDrain()

	waitCh := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(waitCh)
	}()

	var err error
	select {
	case <-waitCh:
	case <-ctx.Done():
		err = ctx.Err()
		e.cancel()
		select {
		case <-waitCh:
		case <-time.After(2 * time.Second):
		}
	}

	if e.notifier != nil {
		if nErr := e.notifier.Shutdown(ctx); nErr != nil && err == nil {
			err = nErr
		}
	}
	if e.pool != nil {
		if pErr := e.pool.Close(); pErr != nil && err == nil {
			err = pErr
		}
	}
	if e.broker != nil {
		if bErr := e.broker.Shutdown(ctx); bErr != nil && err == nil {
			err = bErr
		}
	}
	return err
}

// HandleExpiredApprovals finalizes expired HITL idempotency records after the reaper transitions PostgreSQL rows.
func (e *Engine) HandleExpiredApprovals(ctx context.Context, execIDs []string) {
	for _, execID := range execIDs {
		rec, err := e.store.GetExecution(ctx, execID)
		if err != nil {
			continue
		}
		resp := &store.ExecutionResponse{
			ExecutionID:  execID,
			Status:       "REJECTED",
			Runtime:      rec.Runtime,
			ToolName:     rec.ToolName,
			ExitCode:     -1,
			ErrorMessage: "approval request expired",
		}
		_ = e.locker.Complete(ctx, rec.IdempotencyKey, execID, resp)
		e.metrics.RecordExecution("REJECTED", rec.Runtime, rec.TenantID, 0, 0, 0)
	}
}

// NormalizeRuntime parses a runtime string into canonical API naming and protobuf enum.
func NormalizeRuntime(raw string) (string, pb.RuntimeType, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "python3.12", "python_3_12", "python":
		return "python3.12", pb.RuntimeType_PYTHON_3_12, nil
	case "node20", "node_20", "node":
		return "node20", pb.RuntimeType_NODE_20, nil
	case "bash5", "bash_5", "bash", "sh":
		return "bash5", pb.RuntimeType_BASH_5, nil
	default:
		return "", pb.RuntimeType_RUNTIME_TYPE_UNSPECIFIED, fmt.Errorf("%w: unsupported runtime %q", ErrInvalidRequest, raw)
	}
}

func resolveLimits(in ResourceLimitsReq) store.ResourceLimitsSpec {
	out := store.ResourceLimitsSpec{
		WallTimeoutMs:  defaultWallTimeoutMs,
		MemoryMaxBytes: defaultMemoryMaxBytes,
		CpuQuotaUs:     defaultCpuQuotaUs,
		PidsMax:        defaultPidsMax,
		TmpfsSizeBytes: defaultTmpfsSizeBytes,
	}
	if in.WallTimeoutMs > 0 {
		out.WallTimeoutMs = in.WallTimeoutMs
	}
	if in.MemoryMaxBytes > 0 {
		out.MemoryMaxBytes = in.MemoryMaxBytes
	}
	if in.CpuQuotaUs > 0 {
		out.CpuQuotaUs = in.CpuQuotaUs
	}
	if in.PidsMax > 0 {
		out.PidsMax = in.PidsMax
	}
	if in.TmpfsSizeBytes > 0 {
		out.TmpfsSizeBytes = in.TmpfsSizeBytes
	}
	return out
}

func prepareIdempotencyArgs(args json.RawMessage, stdin string) (json.RawMessage, []byte, error) {
	trimmedArgs := bytes.TrimSpace(args)
	if len(trimmedArgs) > 0 && string(trimmedArgs) != "null" {
		canonArgs, err := idempotency.CanonicalJSON(trimmedArgs)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: invalid JSON args: %v", ErrInvalidRequest, err)
		}
		if stdin == "" {
			return canonArgs, canonArgs, nil
		}
		combined, err := json.Marshal(struct {
			Args  json.RawMessage `json:"args"`
			Stdin string          `json:"stdin"`
		}{
			Args:  json.RawMessage(canonArgs),
			Stdin: stdin,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("marshaling combined idempotency args: %w", err)
		}
		return combined, []byte(stdin), nil
	}

	if stdin != "" {
		encoded, err := json.Marshal(stdin)
		if err != nil {
			return nil, nil, fmt.Errorf("marshaling stdin for idempotency key: %w", err)
		}
		return encoded, []byte(stdin), nil
	}
	return json.RawMessage("null"), nil, nil
}

// SubmitExecution validates, deduplicates, gates (if HITL), dispatches, and returns either sync/cached results or a stream handle.
func (e *Engine) SubmitExecution(ctx context.Context, tenantID string, req ExecuteRequest) (*ExecuteOutcome, error) {
	if e.IsDraining() {
		return nil, ErrDraining
	}
	if strings.TrimSpace(req.AgentSessionID) == "" || strings.TrimSpace(req.ToolName) == "" || strings.TrimSpace(req.EntrypointCode) == "" {
		return nil, fmt.Errorf("%w: agent_session_id, tool_name, and entrypoint_code are required", ErrInvalidRequest)
	}
	normRuntime, _, err := NormalizeRuntime(req.Runtime)
	if err != nil {
		return nil, err
	}
	mode := strings.ToLower(strings.TrimSpace(req.Mode))
	if mode == "" {
		mode = "sync"
	}
	if mode != "sync" && mode != "stream" && mode != "async" {
		return nil, fmt.Errorf("%w: unsupported mode %q", ErrInvalidRequest, mode)
	}

	idempArgs, stdinBytes, err := prepareIdempotencyArgs(req.Args, req.StdinPayload)
	if err != nil {
		return nil, err
	}

	idempKey, codeSHA256, err := idempotency.ComputeKey(tenantID, req.AgentSessionID, req.ToolName, req.EntrypointCode, idempArgs)
	if err != nil {
		return nil, fmt.Errorf("computing idempotency key: %w", err)
	}

	candidateID := uuid.NewString()
	acq, err := e.locker.Acquire(ctx, idempKey, candidateID)
	if err != nil {
		e.metrics.RecordIdempotency("error")
		return nil, fmt.Errorf("acquiring idempotency lock: %w", err)
	}

	switch acq.Outcome {
	case idempotency.OutcomeCompleted:
		e.metrics.RecordIdempotency("cached_replay")
		e.metrics.RecordIdempotentReplay(normRuntime, tenantID)
		resp := acq.Record.CachedResponse
		if resp == nil {
			resp = &store.ExecutionResponse{
				ExecutionID: acq.Record.ExecutionID,
				Status:      "COMPLETED",
			}
		}
		return &ExecuteOutcome{
			ExecutionID: acq.Record.ExecutionID,
			Status:      resp.Status,
			Replayed:    true,
			Response:    resp,
		}, nil

	case idempotency.OutcomeInFlight:
		e.metrics.RecordIdempotency("piggyback")
		execID := acq.Record.ExecutionID
		if mode == "stream" {
			return &ExecuteOutcome{
				ExecutionID: execID,
				Status:      "IN_FLIGHT",
				Piggybacked: true,
			}, nil
		}
		if mode == "async" {
			return &ExecuteOutcome{
				ExecutionID: execID,
				Status:      "IN_FLIGHT",
				Piggybacked: true,
				Response: &store.ExecutionResponse{
					ExecutionID: execID,
					Status:      "IN_FLIGHT",
					Runtime:     normRuntime,
					ToolName:    req.ToolName,
				},
			}, nil
		}
		resp, waitErr := e.waitForExecution(ctx, execID, normRuntime, req.ToolName)
		if waitErr != nil {
			return nil, waitErr
		}
		return &ExecuteOutcome{
			ExecutionID: execID,
			Status:      resp.Status,
			Piggybacked: true,
			Response:    resp,
		}, nil
	}

	// OutcomeAcquired: caller is the leader for candidateID.
	e.metrics.RecordIdempotency("fresh")
	execID := candidateID
	initialStatus := "QUEUED"
	if req.RequiresApproval {
		initialStatus = "AWAITING_APPROVAL"
	}

	loopbackOnly := true
	if req.EnableLoopbackOnly != nil {
		loopbackOnly = *req.EnableLoopbackOnly
	}
	limits := resolveLimits(req.Limits)

	if err := e.store.CreateExecution(ctx, &store.ExecutionRecord{
		ID:               execID,
		TenantID:         tenantID,
		AgentSessionID:   req.AgentSessionID,
		ToolName:         req.ToolName,
		IdempotencyKey:   idempKey,
		Runtime:          normRuntime,
		CodeSHA256:       codeSHA256,
		Status:           initialStatus,
		RequiresApproval: req.RequiresApproval,
	}); err != nil {
		_ = e.locker.Release(context.Background(), idempKey, execID)
		_ = e.broker.Publish(context.Background(), &store.StreamEvent{
			ExecutionID:  execID,
			SequenceNum:  1,
			TimestampNs:  uint64(time.Now().UnixNano()),
			EventType:    "failed",
			ErrorMessage: err.Error(),
			Terminal:     true,
		})
		return nil, fmt.Errorf("creating execution record: %w", err)
	}

	payload := &store.PendingExecutionPayload{
		ExecutionID:        execID,
		TenantID:           tenantID,
		AgentSessionID:     req.AgentSessionID,
		ToolName:           req.ToolName,
		IdempotencyKey:     idempKey,
		Runtime:            normRuntime,
		EntrypointCode:     req.EntrypointCode,
		StdinPayload:       stdinBytes,
		EnvVars:            req.EnvVars,
		Limits:             limits,
		EnableLoopbackOnly: loopbackOnly,
		WebhookURL:         req.WebhookURL,
		BaseSeq:            1,
	}

	// Subscribe BEFORE launching execution when running in sync mode to guarantee zero missed events.
	var syncCh <-chan *store.StreamEvent
	if mode == "sync" {
		syncCh, err = e.broker.Subscribe(ctx, execID)
		if err != nil {
			_ = e.locker.Release(context.Background(), idempKey, execID)
			return nil, fmt.Errorf("subscribing for synchronous execution %s: %w", execID, err)
		}
	}

	if req.RequiresApproval {
		if err := e.initiateHITLPause(ctx, payload, req.ApprovalTTLSeconds); err != nil {
			_ = e.locker.Release(context.Background(), idempKey, execID)
			_ = e.broker.Publish(context.Background(), &store.StreamEvent{
				ExecutionID:  execID,
				SequenceNum:  1,
				TimestampNs:  uint64(time.Now().UnixNano()),
				EventType:    "failed",
				ErrorMessage: err.Error(),
				Terminal:     true,
			})
			return nil, err
		}
		if mode == "async" || mode == "stream" {
			return &ExecuteOutcome{
				ExecutionID: execID,
				Status:      "AWAITING_APPROVAL",
				Response: &store.ExecutionResponse{
					ExecutionID:      execID,
					Status:           "AWAITING_APPROVAL",
					Runtime:          normRuntime,
					ToolName:         req.ToolName,
					ApprovalRequired: true,
				},
			}, nil
		}
		resp, waitErr := collectResponseFromChannel(ctx, syncCh, execID, normRuntime, req.ToolName)
		if waitErr != nil {
			return nil, waitErr
		}
		return &ExecuteOutcome{
			ExecutionID: execID,
			Status:      resp.Status,
			Response:    resp,
		}, nil
	}

	e.launchExecution(payload, 1)

	if mode == "async" {
		return &ExecuteOutcome{
			ExecutionID: execID,
			Status:      "QUEUED",
			Response: &store.ExecutionResponse{
				ExecutionID: execID,
				Status:      "QUEUED",
				Runtime:     normRuntime,
				ToolName:    req.ToolName,
			},
		}, nil
	}
	if mode == "stream" {
		return &ExecuteOutcome{
			ExecutionID: execID,
			Status:      "QUEUED",
		}, nil
	}

	resp, waitErr := collectResponseFromChannel(ctx, syncCh, execID, normRuntime, req.ToolName)
	if waitErr != nil {
		return nil, waitErr
	}
	return &ExecuteOutcome{
		ExecutionID: execID,
		Status:      resp.Status,
		Response:    resp,
	}, nil
}

func (e *Engine) initiateHITLPause(ctx context.Context, payload *store.PendingExecutionPayload, ttlSeconds int) error {
	approvalTTL := e.cfg.DefaultApprovalTTL
	if ttlSeconds > 0 {
		approvalTTL = time.Duration(ttlSeconds) * time.Second
	}
	expiresAt := time.Now().UTC().Add(approvalTTL)

	rawToken, tokenHash, err := hitl.GenerateToken()
	if err != nil {
		return err
	}
	if err := e.store.CreateApproval(ctx, &store.ApprovalRecord{
		ExecutionID:       payload.ExecutionID,
		ApprovalTokenHash: tokenHash,
		WebhookURL:        payload.WebhookURL,
		ExpiresAt:         expiresAt,
	}); err != nil {
		return err
	}
	if err := e.broker.SavePendingPayload(ctx, payload, approvalTTL); err != nil {
		return err
	}
	// Keep idempotency lock active across approval window cleanly.
	_ = e.locker.Renew(ctx, payload.IdempotencyKey, payload.ExecutionID, approvalTTL+time.Minute)

	e.metrics.RecordHITL("required")
	if err := e.broker.Publish(ctx, &store.StreamEvent{
		ExecutionID: payload.ExecutionID,
		SequenceNum: 1,
		TimestampNs: uint64(time.Now().UnixNano()),
		EventType:   "approval_required",
		WebhookURL:  payload.WebhookURL,
		ExpiresAt:   expiresAt.Format(time.RFC3339Nano),
		Terminal:    false,
	}); err != nil {
		return fmt.Errorf("publishing approval_required event: %w", err)
	}

	e.notifier.DispatchWebhookAsync(e.rootCtx, payload.WebhookURL, hitl.WebhookPayload{
		ExecutionID:   payload.ExecutionID,
		TenantID:      payload.TenantID,
		ToolName:      payload.ToolName,
		Runtime:       payload.Runtime,
		ApprovalToken: rawToken,
		ExpiresAt:     expiresAt,
	})
	return nil
}

// DecideApproval approves or rejects a paused execution and immediately resumes execution when approved.
func (e *Engine) DecideApproval(ctx context.Context, rawToken, decision, decidedBy string) (*ApprovalDecisionResult, error) {
	normDecision := strings.ToUpper(strings.TrimSpace(decision))
	if normDecision != "APPROVED" && normDecision != "REJECTED" {
		return nil, fmt.Errorf("%w: decision must be APPROVED or REJECTED", ErrInvalidRequest)
	}
	if strings.TrimSpace(decidedBy) == "" {
		decidedBy = "human-reviewer"
	}

	tokenHash := hitl.HashToken(rawToken)
	rec, err := e.store.DecideApproval(ctx, tokenHash, normDecision, decidedBy)
	if err != nil {
		return nil, err
	}

	payload, loadErr := e.broker.LoadPendingPayload(ctx, rec.ExecutionID)
	if loadErr != nil {
		e.logger.Warn("pending HITL payload missing on approval decision", "execution_id", rec.ExecutionID, "error", loadErr)
	}

	if normDecision == "REJECTED" {
		e.metrics.RecordHITL("rejected")
		_ = e.broker.DeletePendingPayload(ctx, rec.ExecutionID)

		seq := uint64(2)
		var runtimeName, toolName, tenantID, idempKey string
		if payload != nil {
			seq = payload.BaseSeq + 1
			runtimeName = payload.Runtime
			toolName = payload.ToolName
			tenantID = payload.TenantID
			idempKey = payload.IdempotencyKey
		} else if execRec, getErr := e.store.GetExecution(ctx, rec.ExecutionID); getErr == nil {
			runtimeName = execRec.Runtime
			toolName = execRec.ToolName
			tenantID = execRec.TenantID
			idempKey = execRec.IdempotencyKey
		}

		e.metrics.RecordExecution("REJECTED", runtimeName, tenantID, 0, 0, 0)
		resp := &store.ExecutionResponse{
			ExecutionID:  rec.ExecutionID,
			Status:       "REJECTED",
			Runtime:      runtimeName,
			ToolName:     toolName,
			ExitCode:     -1,
			ErrorMessage: "execution rejected by approver",
		}
		if idempKey != "" {
			_ = e.locker.Complete(ctx, idempKey, rec.ExecutionID, resp)
		}
		_ = e.broker.Publish(ctx, &store.StreamEvent{
			ExecutionID:  rec.ExecutionID,
			SequenceNum:  seq,
			TimestampNs:  uint64(time.Now().UnixNano()),
			EventType:    "rejected",
			Decision:     "REJECTED",
			DecidedBy:    decidedBy,
			ErrorMessage: "execution rejected by approver",
			Terminal:     true,
		})

		return &ApprovalDecisionResult{
			ExecutionID: rec.ExecutionID,
			Decision:    "REJECTED",
			Status:      "REJECTED",
		}, nil
	}

	// APPROVED: wake subscribers and dispatch immediately.
	e.metrics.RecordHITL("approved")
	_ = e.broker.DeletePendingPayload(ctx, rec.ExecutionID)

	if payload == nil {
		return nil, fmt.Errorf("cannot resume execution %s: pending payload expired from cache", rec.ExecutionID)
	}

	seq := payload.BaseSeq + 1
	_ = e.locker.Renew(ctx, payload.IdempotencyKey, payload.ExecutionID, 5*time.Minute)
	if err := e.broker.Publish(ctx, &store.StreamEvent{
		ExecutionID: rec.ExecutionID,
		SequenceNum: seq,
		TimestampNs: uint64(time.Now().UnixNano()),
		EventType:   "approved",
		Decision:    "APPROVED",
		DecidedBy:   decidedBy,
		Terminal:    false,
	}); err != nil {
		return nil, fmt.Errorf("publishing approved event for %s: %w", rec.ExecutionID, err)
	}

	e.launchExecution(payload, seq+1)

	return &ApprovalDecisionResult{
		ExecutionID: rec.ExecutionID,
		Decision:    "APPROVED",
		Status:      "QUEUED",
	}, nil
}

func (e *Engine) launchExecution(payload *store.PendingExecutionPayload, startSeq uint64) {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.executeAndFinalize(e.rootCtx, payload, startSeq)
	}()
}

func (e *Engine) executeAndFinalize(ctx context.Context, payload *store.PendingExecutionPayload, startSeq uint64) {
	_, pbRuntime, err := NormalizeRuntime(payload.Runtime)
	if err != nil {
		e.failBeforeStart(ctx, payload, startSeq, err.Error())
		return
	}

	pbReq := &pb.ExecutionRequest{
		ExecutionId:    payload.ExecutionID,
		Runtime:        pbRuntime,
		EntrypointCode: payload.EntrypointCode,
		StdinPayload:   payload.StdinPayload,
		EnvVars:        payload.EnvVars,
		Limits: &pb.ResourceLimits{
			WallTimeoutMs:  payload.Limits.WallTimeoutMs,
			MemoryMaxBytes: payload.Limits.MemoryMaxBytes,
			CpuQuotaUs:     payload.Limits.CpuQuotaUs,
			PidsMax:        payload.Limits.PidsMax,
			TmpfsSizeBytes: payload.Limits.TmpfsSizeBytes,
		},
		EnableLoopbackOnly: payload.EnableLoopbackOnly,
	}

	timeout := time.Duration(payload.Limits.WallTimeoutMs)*time.Millisecond + 15*time.Second
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	exclude := make(map[string]bool)
	seq := startSeq
	nextSeq := func() uint64 {
		cur := seq
		seq++
		return cur
	}

	var (
		started    bool
		workerID   string
		stdoutBuf  bytes.Buffer
		stderrBuf  bytes.Buffer
		finalSent  bool
		lastErr    error
		maxWorkers = 2
	)

	for attempt := 0; attempt < maxWorkers; attempt++ {
		if started {
			break
		}
		worker, selErr := e.scheduler.SelectWorker(exclude)
		if selErr != nil {
			if lastErr == nil {
				lastErr = selErr
			}
			break
		}

		workerID = worker.ID
		e.pool.IncInFlight(worker.ID, 1)

		stream, callErr := worker.Client.ExecuteTool(execCtx, pbReq)
		if callErr != nil {
			e.pool.IncInFlight(worker.ID, -1)
			e.pool.MarkUnhealthy(worker.ID)
			exclude[worker.ID] = true
			e.metrics.WorkerFailoversTotal.Inc()
			lastErr = fmt.Errorf("opening ExecuteTool stream on %s: %w", worker.ID, callErr)
			continue
		}

		retryBeforeStart := false
		for {
			evt, recvErr := stream.Recv()
			if recvErr != nil {
				e.pool.IncInFlight(worker.ID, -1)
				if !started {
					e.pool.MarkUnhealthy(worker.ID)
					exclude[worker.ID] = true
					e.metrics.WorkerFailoversTotal.Inc()
					lastErr = fmt.Errorf("worker %s failed before STARTED: %w", worker.ID, recvErr)
					retryBeforeStart = true
				} else {
					lastErr = fmt.Errorf("worker %s stream dropped after STARTED: %w", worker.ID, recvErr)
				}
				break
			}

			switch evt.GetEventType() {
			case pb.EventType_STARTED:
				started = true
				if err := e.store.UpdateExecutionRunning(ctx, payload.ExecutionID, worker.ID); err != nil {
					e.logger.Warn("failed updating execution RUNNING status", "execution_id", payload.ExecutionID, "error", err)
				}
				_ = e.broker.Publish(ctx, &store.StreamEvent{
					ExecutionID:  payload.ExecutionID,
					SequenceNum:  nextSeq(),
					TimestampNs:  evt.GetTimestampNs(),
					EventType:    "started",
					WorkerNodeID: worker.ID,
					Terminal:     false,
				})

			case pb.EventType_STDOUT_CHUNK:
				chunk := evt.GetChunkData()
				if stdoutBuf.Len() < maxSyncResponseOutput {
					remaining := maxSyncResponseOutput - stdoutBuf.Len()
					if len(chunk) > remaining {
						stdoutBuf.Write(chunk[:remaining])
					} else {
						stdoutBuf.Write(chunk)
					}
				}
				_ = e.broker.Publish(ctx, &store.StreamEvent{
					ExecutionID:  payload.ExecutionID,
					SequenceNum:  nextSeq(),
					TimestampNs:  evt.GetTimestampNs(),
					EventType:    "stdout_chunk",
					Data:         string(chunk),
					WorkerNodeID: worker.ID,
					Terminal:     false,
				})

			case pb.EventType_STDERR_CHUNK:
				chunk := evt.GetChunkData()
				if stderrBuf.Len() < maxSyncResponseOutput {
					remaining := maxSyncResponseOutput - stderrBuf.Len()
					if len(chunk) > remaining {
						stderrBuf.Write(chunk[:remaining])
					} else {
						stderrBuf.Write(chunk)
					}
				}
				_ = e.broker.Publish(ctx, &store.StreamEvent{
					ExecutionID:  payload.ExecutionID,
					SequenceNum:  nextSeq(),
					TimestampNs:  evt.GetTimestampNs(),
					EventType:    "stderr_chunk",
					Data:         string(chunk),
					WorkerNodeID: worker.ID,
					Terminal:     false,
				})

			case pb.EventType_COMPLETED,
				pb.EventType_OOM_KILLED,
				pb.EventType_TIMEOUT_KILLED,
				pb.EventType_SYSCALL_VIOLATION,
				pb.EventType_INTERNAL_ERROR:
				e.pool.IncInFlight(worker.ID, -1)
				sqlStatus, sseName := mapTerminalEvent(evt.GetEventType())
				e.finalizeTerminal(
					ctx,
					payload,
					worker.ID,
					sqlStatus,
					sseName,
					evt.GetErrorMessage(),
					evt.GetMetrics(),
					stdoutBuf.Bytes(),
					stderrBuf.Bytes(),
					nextSeq(),
					evt.GetTimestampNs(),
				)
				finalSent = true
			}

			if finalSent {
				break
			}
		}

		if finalSent {
			return
		}
		if !retryBeforeStart {
			break
		}
	}

	if !finalSent {
		errMsg := "execution failed"
		if lastErr != nil {
			errMsg = lastErr.Error()
		}
		if !started {
			e.failBeforeStart(ctx, payload, nextSeq(), errMsg)
			return
		}
		e.finalizeTerminal(
			ctx,
			payload,
			workerID,
			"FAILED",
			"failed",
			errMsg,
			&pb.ExecutionMetrics{ExitCode: -1},
			stdoutBuf.Bytes(),
			stderrBuf.Bytes(),
			nextSeq(),
			uint64(time.Now().UnixNano()),
		)
	}
}

func (e *Engine) failBeforeStart(ctx context.Context, payload *store.PendingExecutionPayload, seq uint64, errMsg string) {
	_ = e.store.CompleteExecution(ctx, &store.ExecutionResultUpdate{
		ID:       payload.ExecutionID,
		Status:   "FAILED",
		ExitCode: -1,
		StderrTail: []byte(errMsg),
	})
	_ = e.locker.Release(ctx, payload.IdempotencyKey, payload.ExecutionID)
	e.metrics.RecordExecution("FAILED", payload.Runtime, payload.TenantID, 0, 0, 0)

	_ = e.broker.Publish(ctx, &store.StreamEvent{
		ExecutionID:  payload.ExecutionID,
		SequenceNum:  seq,
		TimestampNs:  uint64(time.Now().UnixNano()),
		EventType:    "failed",
		ErrorMessage: errMsg,
		Terminal:     true,
	})
}

func (e *Engine) finalizeTerminal(
	ctx context.Context,
	payload *store.PendingExecutionPayload,
	workerID string,
	sqlStatus string,
	sseEvent string,
	errMsg string,
	pbMetrics *pb.ExecutionMetrics,
	stdout []byte,
	stderr []byte,
	seq uint64,
	tsNs uint64,
) {
	var exitCode int32
	var signaled int32
	var setupUs, execUs, teardownUs, peakMem, cpuUs uint64
	if pbMetrics != nil {
		exitCode = pbMetrics.GetExitCode()
		signaled = pbMetrics.GetSignaled()
		setupUs = pbMetrics.GetSetupDurationUs()
		execUs = pbMetrics.GetExecDurationUs()
		teardownUs = pbMetrics.GetTeardownDurationUs()
		peakMem = pbMetrics.GetPeakMemoryBytes()
		cpuUs = pbMetrics.GetCpuUsageUs()
	} else {
		exitCode = -1
	}

	if err := e.store.CompleteExecution(ctx, &store.ExecutionResultUpdate{
		ID:              payload.ExecutionID,
		Status:          sqlStatus,
		WorkerNodeID:    workerID,
		ExitCode:        exitCode,
		SetupDurationUs: int64(setupUs),
		ExecDurationUs:  int64(execUs),
		PeakMemoryBytes: int64(peakMem),
		StdoutTail:      stdout,
		StderrTail:      stderr,
	}); err != nil {
		e.logger.Error("persisting terminal execution state failed", "execution_id", payload.ExecutionID, "error", err)
	}

	metricsJSON := &store.ExecutionMetricsJSON{
		ExitCode:           exitCode,
		Signaled:           signaled,
		SetupDurationUs:    setupUs,
		ExecDurationUs:     execUs,
		TeardownDurationUs: teardownUs,
		PeakMemoryBytes:    peakMem,
		CpuUsageUs:         cpuUs,
	}

	resp := &store.ExecutionResponse{
		ExecutionID:        payload.ExecutionID,
		Status:             sqlStatus,
		Runtime:            payload.Runtime,
		ToolName:           payload.ToolName,
		WorkerNodeID:       workerID,
		ExitCode:           exitCode,
		Signaled:           signaled,
		Stdout:             string(stdout),
		Stderr:             string(stderr),
		SetupDurationUs:    setupUs,
		ExecDurationUs:     execUs,
		TeardownDurationUs: teardownUs,
		PeakMemoryBytes:    peakMem,
		CpuUsageUs:         cpuUs,
		ErrorMessage:       errMsg,
		Metrics:            metricsJSON,
	}

	// Cache every post-STARTED terminal outcome (including FAILED/OOM/TIMEOUT) so side effects are not re-executed.
	_ = e.locker.Complete(ctx, payload.IdempotencyKey, payload.ExecutionID, resp)

	e.metrics.RecordExecution(sqlStatus, payload.Runtime, payload.TenantID, setupUs, execUs, teardownUs)

	if tsNs == 0 {
		tsNs = uint64(time.Now().UnixNano())
	}
	_ = e.broker.Publish(ctx, &store.StreamEvent{
		ExecutionID:  payload.ExecutionID,
		SequenceNum:  seq,
		TimestampNs:  tsNs,
		EventType:    sseEvent,
		WorkerNodeID: workerID,
		ErrorMessage: errMsg,
		Metrics:      metricsJSON,
		Terminal:     true,
	})
}

func mapTerminalEvent(t pb.EventType) (sqlStatus string, sseEvent string) {
	switch t {
	case pb.EventType_COMPLETED:
		return "COMPLETED", "completed"
	case pb.EventType_OOM_KILLED:
		return "OOM_KILLED", "oom_killed"
	case pb.EventType_TIMEOUT_KILLED:
		return "TIMEOUT_KILLED", "timeout_killed"
	case pb.EventType_SYSCALL_VIOLATION:
		return "SYSCALL_VIOLATION", "syscall_violation"
	default:
		return "FAILED", "failed"
	}
}

func (e *Engine) waitForExecution(ctx context.Context, executionID, runtimeName, toolName string) (*store.ExecutionResponse, error) {
	ch, err := e.broker.Subscribe(ctx, executionID)
	if err != nil {
		return nil, fmt.Errorf("subscribing to piggybacked execution %s: %w", executionID, err)
	}
	return collectResponseFromChannel(ctx, ch, executionID, runtimeName, toolName)
}

func collectResponseFromChannel(
	ctx context.Context,
	ch <-chan *store.StreamEvent,
	executionID, runtimeName, toolName string,
) (*store.ExecutionResponse, error) {
	var (
		stdout   bytes.Buffer
		stderr   bytes.Buffer
		workerID string
		lastEvt  *store.StreamEvent
	)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case evt, ok := <-ch:
			if !ok {
				if lastEvt == nil {
					return nil, fmt.Errorf("stream closed before receiving events for %s", executionID)
				}
				return buildExecutionResponse(executionID, runtimeName, toolName, workerID, stdout.String(), stderr.String(), lastEvt), nil
			}
			lastEvt = evt
			if evt.WorkerNodeID != "" {
				workerID = evt.WorkerNodeID
			}
			switch evt.EventType {
			case "stdout_chunk":
				stdout.WriteString(evt.Data)
			case "stderr_chunk":
				stderr.WriteString(evt.Data)
			}
			if evt.Terminal {
				return buildExecutionResponse(executionID, runtimeName, toolName, workerID, stdout.String(), stderr.String(), evt), nil
			}
		}
	}
}

func buildExecutionResponse(
	executionID, runtimeName, toolName, workerID, stdout, stderr string,
	terminalEvt *store.StreamEvent,
) *store.ExecutionResponse {
	resp := &store.ExecutionResponse{
		ExecutionID:  executionID,
		Status:       mapSSEToSQLStatus(terminalEvt.EventType),
		Runtime:      runtimeName,
		ToolName:     toolName,
		WorkerNodeID: workerID,
		Stdout:       stdout,
		Stderr:       stderr,
		ErrorMessage: terminalEvt.ErrorMessage,
		Metrics:      terminalEvt.Metrics,
	}
	if terminalEvt.Metrics != nil {
		resp.ExitCode = terminalEvt.Metrics.ExitCode
		resp.Signaled = terminalEvt.Metrics.Signaled
		resp.SetupDurationUs = terminalEvt.Metrics.SetupDurationUs
		resp.ExecDurationUs = terminalEvt.Metrics.ExecDurationUs
		resp.TeardownDurationUs = terminalEvt.Metrics.TeardownDurationUs
		resp.PeakMemoryBytes = terminalEvt.Metrics.PeakMemoryBytes
		resp.CpuUsageUs = terminalEvt.Metrics.CpuUsageUs
	} else if resp.Status == "FAILED" || resp.Status == "REJECTED" {
		resp.ExitCode = -1
	}
	return resp
}

func mapSSEToSQLStatus(sseEvent string) string {
	switch sseEvent {
	case "completed":
		return "COMPLETED"
	case "oom_killed":
		return "OOM_KILLED"
	case "timeout_killed":
		return "TIMEOUT_KILLED"
	case "syscall_violation":
		return "SYSCALL_VIOLATION"
	case "rejected", "REJECTED":
		return "REJECTED"
	default:
		return "FAILED"
	}
}

// GetExecutionRecord returns an execution record owned by tenantID, checking tenant isolation.
func (e *Engine) GetExecutionRecord(ctx context.Context, tenantID, executionID string) (*store.ExecutionRecord, error) {
	rec, err := e.store.GetExecution(ctx, executionID)
	if err != nil {
		return nil, err
	}
	if tenantID != "" && rec.TenantID != tenantID {
		return nil, ErrForbiddenTenant
	}
	return rec, nil
}
