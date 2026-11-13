package api_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aegisrun/aegisrun/control-go/internal/api"
	"github.com/aegisrun/aegisrun/control-go/internal/hitl"
	"github.com/aegisrun/aegisrun/control-go/internal/idempotency"
	"github.com/aegisrun/aegisrun/control-go/internal/orchestrator"
	"github.com/aegisrun/aegisrun/control-go/internal/store"
	"github.com/aegisrun/aegisrun/control-go/internal/telemetry"
	"github.com/aegisrun/aegisrun/control-go/pkg/pb"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

type mockSandboxWorker struct {
	pb.UnimplementedSandboxWorkerServer

	workerID        string
	maxSandboxes    uint32
	initialActive   uint32
	memoryUsedPct   float64
	heartbeatPeriod time.Duration
	execDelay       time.Duration

	execCount atomic.Int64
	stopped   atomic.Bool
	stopOnce  sync.Once
	stopCh    chan struct{}
}

func newMockWorker(id string, initialActive, maxSandboxes uint32, memPct float64, execDelay time.Duration) *mockSandboxWorker {
	return &mockSandboxWorker{
		workerID:        id,
		maxSandboxes:    maxSandboxes,
		initialActive:   initialActive,
		memoryUsedPct:   memPct,
		heartbeatPeriod: 50 * time.Millisecond,
		execDelay:       execDelay,
		stopCh:          make(chan struct{}),
	}
}

func (m *mockSandboxWorker) stopHeartbeats() {
	m.stopped.Store(true)
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})
}

func (m *mockSandboxWorker) StreamWorkerHeartbeat(_ *pb.Empty, stream grpc.ServerStreamingServer[pb.WorkerHeartbeat]) error {
	sendHB := func() error {
		return stream.Send(&pb.WorkerHeartbeat{
			WorkerNodeId:         m.workerID,
			ActiveSandboxes:      m.initialActive,
			MaxSandboxes:         m.maxSandboxes,
			AvailableMemoryBytes: 512 * 1024 * 1024,
			TotalMemoryBytes:     1024 * 1024 * 1024,
			MemoryUsedPct:        m.memoryUsedPct,
			Status:               pb.WorkerStatus_HEALTHY,
			TimestampNs:          uint64(time.Now().UnixNano()),
		})
	}

	if err := sendHB(); err != nil {
		return err
	}

	ticker := time.NewTicker(m.heartbeatPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-m.stopCh:
			return nil
		case <-ticker.C:
			if err := sendHB(); err != nil {
				return err
			}
		}
	}
}

func (m *mockSandboxWorker) ExecuteTool(req *pb.ExecutionRequest, stream grpc.ServerStreamingServer[pb.ExecutionEvent]) error {
	m.execCount.Add(1)
	if err := stream.Send(&pb.ExecutionEvent{
		ExecutionId: req.GetExecutionId(),
		SequenceNum: 1,
		TimestampNs: uint64(time.Now().UnixNano()),
		EventType:   pb.EventType_STARTED,
	}); err != nil {
		return err
	}

	if m.execDelay > 0 {
		timer := time.NewTimer(m.execDelay)
		select {
		case <-stream.Context().Done():
			timer.Stop()
			return stream.Context().Err()
		case <-timer.C:
		}
	}

	out := fmt.Sprintf("executed-by=%s stdin=%s", m.workerID, string(req.GetStdinPayload()))
	if err := stream.Send(&pb.ExecutionEvent{
		ExecutionId: req.GetExecutionId(),
		SequenceNum: 2,
		TimestampNs: uint64(time.Now().UnixNano()),
		EventType:   pb.EventType_STDOUT_CHUNK,
		ChunkData:   []byte(out),
	}); err != nil {
		return err
	}

	return stream.Send(&pb.ExecutionEvent{
		ExecutionId: req.GetExecutionId(),
		SequenceNum: 3,
		TimestampNs: uint64(time.Now().UnixNano()),
		EventType:   pb.EventType_COMPLETED,
		Metrics: &pb.ExecutionMetrics{
			ExitCode:           0,
			SetupDurationUs:    4200,
			ExecDurationUs:     8500,
			TeardownDurationUs: 1100,
			PeakMemoryBytes:    8 * 1024 * 1024,
			CpuUsageUs:         5400,
		},
	})
}

type startedMockServer struct {
	worker *mockSandboxWorker
	lis    net.Listener
	grpc   *grpc.Server
	addr   string
}

func startMockGRPCWorker(t *testing.T, w *mockSandboxWorker) *startedMockServer {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening for mock gRPC worker %s: %v", w.workerID, err)
	}
	srv := grpc.NewServer()
	pb.RegisterSandboxWorkerServer(srv, w)
	go func() {
		_ = srv.Serve(lis)
	}()
	return &startedMockServer{
		worker: w,
		lis:    lis,
		grpc:   srv,
		addr:   lis.Addr().String(),
	}
}

func (s *startedMockServer) kill() {
	s.worker.stopHeartbeats()
	s.grpc.Stop()
	_ = s.lis.Close()
}

type testHarness struct {
	t        *testing.T
	tenantID string
	apiToken string
	pgStore  *store.PostgresStore
	rdb      *redis.Client
	pool     *orchestrator.WorkerPool
	engine   *orchestrator.Engine
	httpSrv  *httptest.Server
	workers  []*startedMockServer
}

func setupTestHarness(t *testing.T, heartbeatTimeout time.Duration, workers ...*mockSandboxWorker) *testHarness {
	t.Helper()

	dsn := os.Getenv("AEGIS_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://aegis:aegis@localhost:5432/aegisrun?sslmode=disable"
	}
	redisAddr := os.Getenv("AEGIS_REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pgStore, err := store.NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatalf("connecting PostgreSQL for test (%s): %v", dsn, err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		pgStore.Close()
		t.Fatalf("connecting Redis for test (%s): %v", redisAddr, err)
	}

	tenantID := fmt.Sprintf("tenant-test-%d", time.Now().UnixNano())
	apiToken := "token-" + tenantID
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := telemetry.NewMetrics()
	broker := store.NewStreamBroker(rdb)
	locker := idempotency.NewLocker(rdb)

	started := make([]*startedMockServer, 0, len(workers))
	targets := make(map[string]string, len(workers))
	for _, w := range workers {
		s := startMockGRPCWorker(t, w)
		started = append(started, s)
		targets[w.workerID] = s.addr
	}

	pool, err := orchestrator.NewWorkerPool(targets, heartbeatTimeout, metrics, logger)
	if err != nil {
		for _, s := range started {
			s.kill()
		}
		_ = rdb.Close()
		pgStore.Close()
		t.Fatalf("creating worker pool: %v", err)
	}

	if err := pool.WaitReady(ctx, len(workers)); err != nil {
		_ = pool.Close()
		for _, s := range started {
			s.kill()
		}
		_ = rdb.Close()
		pgStore.Close()
		t.Fatalf("waiting for mock worker heartbeats: %v", err)
	}

	scheduler := orchestrator.NewScheduler(pool)
	notifier := hitl.NewNotifier(pgStore, broker, metrics, 2*time.Second, logger)
	engine := orchestrator.NewEngine(
		pgStore,
		broker,
		locker,
		pool,
		scheduler,
		notifier,
		metrics,
		orchestrator.EngineConfig{DefaultApprovalTTL: 2 * time.Minute},
		logger,
	)

	router := api.NewRouter(api.RouterDeps{
		Engine:   engine,
		Broker:   broker,
		Pool:     pool,
		Metrics:  metrics,
		Logger:   logger,
		Postgres: pgStore.Pool(),
		Redis:    rdb,
		APIKeys:  map[string]string{apiToken: tenantID},
		RateLimiter: api.RateLimiterConfig{
			Redis:              rdb,
			RequestsPerSecond:  200,
			Burst:              100,
			MaxActivePerTenant: 50,
		},
	})
	httpSrv := httptest.NewServer(router)

	h := &testHarness{
		t:        t,
		tenantID: tenantID,
		apiToken: apiToken,
		pgStore:  pgStore,
		rdb:      rdb,
		pool:     pool,
		engine:   engine,
		httpSrv:  httpSrv,
		workers:  started,
	}

	t.Cleanup(func() {
		httpSrv.Close()
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutCancel()
		_ = engine.Shutdown(shutCtx)
		for _, s := range started {
			s.kill()
		}
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanCancel()
		_, _ = pgStore.Pool().Exec(cleanCtx, `DELETE FROM executions WHERE tenant_id = $1`, tenantID)
		_ = rdb.Del(cleanCtx, "aegis:rl:"+tenantID, "aegis:active_count:"+tenantID).Err()
		_ = rdb.Close()
		pgStore.Close()
	})

	return h
}

func (h *testHarness) cleanupExecutionKeys(execID, idempKey string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if idempKey != "" {
		_ = h.rdb.Del(ctx, "aegis:idemp:"+idempKey).Err()
	}
	if execID != "" {
		_ = h.rdb.Del(ctx,
			"aegis:history:exec:"+execID,
			"aegis:hitl:payload:"+execID,
		).Err()
	}
}

func (h *testHarness) postExecute(payload any) (*http.Response, []byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, nil, err
	}
	req, err := http.NewRequest(http.MethodPost, h.httpSrv.URL+"/v1/execute", bytes.NewReader(raw))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+h.apiToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, readErr := io.ReadAll(resp.Body)
	return resp, body, readErr
}

func TestIdempotencyPiggybacking(t *testing.T) {
	worker := newMockWorker("worker-idem-1", 0, 16, 20.0, 40*time.Millisecond)
	h := setupTestHarness(t, 2*time.Second, worker)

	const concurrency = 20
	sessionID := fmt.Sprintf("sess-piggyback-%d", time.Now().UnixNano())
	code := "print(json.dumps({'ok': True}))"

	idempKeyA, _, err := idempotency.ComputeKey(h.tenantID, sessionID, "calc_tool", code, json.RawMessage(`{"a":1,"b":2}`))
	if err != nil {
		t.Fatalf("computing reference idempotency key: %v", err)
	}
	t.Cleanup(func() {
		h.cleanupExecutionKeys("", idempKeyA)
	})

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		results   = make([]store.ExecutionResponse, 0, concurrency)
		errs      = make([]error, 0)
		startGate = make(chan struct{})
	)

	for i := 0; i < concurrency; i++ {
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startGate

			// Alternate key order across callers to verify canonical JSON idempotency hashing.
			rawArgs := json.RawMessage(`{"a":1,"b":2}`)
			if idx%2 == 1 {
				rawArgs = json.RawMessage(`{"b":2,"a":1}`)
			}
			reqPayload := map[string]any{
				"agent_session_id": sessionID,
				"tool_name":        "calc_tool",
				"runtime":          "python3.12",
				"entrypoint_code":  code,
				"args":             rawArgs,
				"mode":             "sync",
			}
			resp, body, reqErr := h.postExecute(reqPayload)
			mu.Lock()
			defer mu.Unlock()
			if reqErr != nil {
				errs = append(errs, reqErr)
				return
			}
			if resp.StatusCode != http.StatusOK {
				errs = append(errs, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body)))
				return
			}
			var decoded store.ExecutionResponse
			if decErr := json.Unmarshal(body, &decoded); decErr != nil {
				errs = append(errs, fmt.Errorf("decoding response %q: %w", string(body), decErr))
				return
			}
			results = append(results, decoded)
		}()
	}

	close(startGate)
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("concurrent requests returned errors: %v", errs)
	}
	if len(results) != concurrency {
		t.Fatalf("expected %d responses, got %d", concurrency, len(results))
	}

	if got := worker.execCount.Load(); got != 1 {
		t.Fatalf("expected exactly 1 gRPC worker execution for 20 concurrent duplicates, got %d", got)
	}

	firstID := results[0].ExecutionID
	if firstID == "" {
		t.Fatalf("empty execution_id returned")
	}
	t.Cleanup(func() {
		h.cleanupExecutionKeys(firstID, idempKeyA)
	})

	for i, res := range results {
		if res.ExecutionID != firstID {
			t.Fatalf("response[%d] execution_id=%s != %s", i, res.ExecutionID, firstID)
		}
		if res.Status != "COMPLETED" {
			t.Fatalf("response[%d] status=%s, want COMPLETED", i, res.Status)
		}
		if res.Stdout != results[0].Stdout {
			t.Fatalf("response[%d] stdout=%q != %q", i, res.Stdout, results[0].Stdout)
		}
	}

	// 21st request must hit the cached replay path with X-Aegis-Idempotent-Replay header.
	replayResp, replayBody, err := h.postExecute(map[string]any{
		"agent_session_id": sessionID,
		"tool_name":        "calc_tool",
		"runtime":          "python3.12",
		"entrypoint_code":  code,
		"args":             json.RawMessage(`{"a":1,"b":2}`),
		"mode":             "sync",
	})
	if err != nil {
		t.Fatalf("21st replay request failed: %v", err)
	}
	if replayResp.StatusCode != http.StatusOK {
		t.Fatalf("21st replay status=%d body=%s", replayResp.StatusCode, string(replayBody))
	}
	if got := replayResp.Header.Get("X-Aegis-Idempotent-Replay"); got != "true" {
		t.Fatalf("expected X-Aegis-Idempotent-Replay: true, got %q", got)
	}
	var replayDecoded store.ExecutionResponse
	if err := json.Unmarshal(replayBody, &replayDecoded); err != nil {
		t.Fatalf("decoding 21st replay body: %v", err)
	}
	if replayDecoded.ExecutionID != firstID || replayDecoded.Stdout != results[0].Stdout {
		t.Fatalf("replay mismatch: got %+v, want execID=%s stdout=%q", replayDecoded, firstID, results[0].Stdout)
	}
	if got := worker.execCount.Load(); got != 1 {
		t.Fatalf("expected worker execCount to remain 1 after replay, got %d", got)
	}
}

type sseFrame struct {
	ID    string
	Event string
	Data  string
}

func readSSEFrames(r io.Reader, out chan<- sseFrame, errCh chan<- error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var cur sseFrame
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if cur.Event != "" || cur.Data != "" {
				out <- cur
				cur = sseFrame{}
			}
			continue
		}
		if strings.HasPrefix(line, "id:") {
			cur.ID = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
		} else if strings.HasPrefix(line, "event:") {
			cur.Event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			cur.Data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
	}
	if err := scanner.Err(); err != nil {
		errCh <- err
	}
	close(out)
}

func TestHITLPauseAndResume(t *testing.T) {
	worker := newMockWorker("worker-hitl-1", 0, 16, 15.0, 0)
	h := setupTestHarness(t, 2*time.Second, worker)

	webhookCh := make(chan hitl.WebhookPayload, 1)
	webhookSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p hitl.WebhookPayload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		select {
		case webhookCh <- p:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer webhookSrv.Close()

	sessionID := fmt.Sprintf("sess-hitl-%d", time.Now().UnixNano())
	code := "print('sensitive-action')"
	argsRaw := json.RawMessage(`{"target":"prod"}`)
	idempKey, _, _ := idempotency.ComputeKey(h.tenantID, sessionID, "drop_db", code, argsRaw)

	reqBody, _ := json.Marshal(map[string]any{
		"agent_session_id":  sessionID,
		"tool_name":         "drop_db",
		"runtime":           "python3.12",
		"entrypoint_code":   code,
		"args":              argsRaw,
		"mode":              "stream",
		"requires_approval": true,
		"webhook_url":       webhookSrv.URL,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.httpSrv.URL+"/v1/execute", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("creating SSE execute request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+h.apiToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	sseResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("opening SSE execute stream: %v", err)
	}
	defer func() { _ = sseResp.Body.Close() }()

	if sseResp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(sseResp.Body)
		t.Fatalf("expected 200 SSE stream, got %d: %s", sseResp.StatusCode, string(b))
	}

	frames := make(chan sseFrame, 16)
	readErrCh := make(chan error, 1)
	go readSSEFrames(sseResp.Body, frames, readErrCh)

	// 1. Expect `event: approval_required` over SSE.
	var execID string
	select {
	case f := <-frames:
		if f.Event != "approval_required" {
			t.Fatalf("expected first SSE event approval_required, got %q (data=%s)", f.Event, f.Data)
		}
		var evt store.StreamEvent
		if err := json.Unmarshal([]byte(f.Data), &evt); err != nil {
			t.Fatalf("decoding approval_required event: %v", err)
		}
		execID = evt.ExecutionID
		t.Cleanup(func() {
			h.cleanupExecutionKeys(execID, idempKey)
		})
	case err := <-readErrCh:
		t.Fatalf("SSE read error while awaiting approval_required: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for approval_required SSE event")
	}

	// 2. Capture webhook token while confirming worker has NOT executed yet.
	var hook hitl.WebhookPayload
	select {
	case hook = <-webhookCh:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for HITL webhook delivery")
	}
	if hook.ExecutionID != execID || hook.ApprovalToken == "" {
		t.Fatalf("unexpected webhook payload: %+v", hook)
	}
	if got := worker.execCount.Load(); got != 0 {
		t.Fatalf("expected 0 worker executions before HITL approval, got %d", got)
	}

	// 3. Approve execution and measure latency to sandbox completion event.
	decideBody, _ := json.Marshal(map[string]string{
		"decision":   "APPROVED",
		"decided_by": "ops-lead",
	})
	decideStart := time.Now()
	decideResp, err := http.Post(
		h.httpSrv.URL+"/v1/approvals/"+hook.ApprovalToken+"/decide",
		"application/json",
		bytes.NewReader(decideBody),
	)
	if err != nil {
		t.Fatalf("calling decide endpoint: %v", err)
	}
	decideRaw, _ := io.ReadAll(decideResp.Body)
	_ = decideResp.Body.Close()
	if decideResp.StatusCode != http.StatusOK {
		t.Fatalf("decide returned %d: %s", decideResp.StatusCode, string(decideRaw))
	}

	var (
		sawStdout    strings.Builder
		sawCompleted bool
	)
	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()
	for !sawCompleted {
		select {
		case f, ok := <-frames:
			if !ok {
				t.Fatalf("SSE stream closed before completed event")
			}
			var evt store.StreamEvent
			_ = json.Unmarshal([]byte(f.Data), &evt)
			if f.Event == "stdout_chunk" {
				sawStdout.WriteString(evt.Data)
			}
			if f.Event == "completed" {
				sawCompleted = true
			}
		case err := <-readErrCh:
			t.Fatalf("SSE read error after approval: %v", err)
		case <-timeout.C:
			t.Fatalf("timed out waiting for SSE completed after HITL approval")
		}
	}

	resumeLatency := time.Since(decideStart)
	t.Logf("HITL approve -> completed SSE latency: %s", resumeLatency)
	if resumeLatency > 50*time.Millisecond {
		t.Fatalf("HITL approve -> completed SSE latency %s exceeded 50ms budget", resumeLatency)
	}
	if !strings.Contains(sawStdout.String(), "executed-by=worker-hitl-1") {
		t.Fatalf("expected worker stdout chunk after approval, got %q", sawStdout.String())
	}
	if got := worker.execCount.Load(); got != 1 {
		t.Fatalf("expected exactly 1 execution after approval, got %d", got)
	}
}

func TestWorkerFailover(t *testing.T) {
	w1 := newMockWorker("worker-1", 0, 16, 10.0, 0)
	w2 := newMockWorker("worker-2", 6, 16, 40.0, 0)
	h := setupTestHarness(t, 120*time.Millisecond, w1, w2)

	sessionBase := fmt.Sprintf("sess-failover-%d", time.Now().UnixNano())

	// Sanity check: before worker-1 dies, lower-load worker-1 should win selection.
	firstPayload := map[string]any{
		"agent_session_id": sessionBase + "-pre",
		"tool_name":        "probe",
		"runtime":          "bash5",
		"entrypoint_code":  "echo ok",
		"args":             json.RawMessage(`{"step":0}`),
	}
	firstKey, _, _ := idempotency.ComputeKey(h.tenantID, sessionBase+"-pre", "probe", "echo ok", json.RawMessage(`{"step":0}`))
	resp, body, err := h.postExecute(firstPayload)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("pre-failover execution failed: err=%v status=%v body=%s", err, resp, string(body))
	}
	var preOut store.ExecutionResponse
	_ = json.Unmarshal(body, &preOut)
	t.Cleanup(func() {
		h.cleanupExecutionKeys(preOut.ExecutionID, firstKey)
	})
	if preOut.WorkerNodeID != "worker-1" {
		t.Fatalf("expected least-loaded pre-failover worker to be worker-1, got %s", preOut.WorkerNodeID)
	}

	// Kill worker-1 and wait >120ms heartbeat timeout (150ms) per failover specification.
	h.workers[0].kill()
	time.Sleep(150 * time.Millisecond)

	const totalRequests = 10
	for i := 0; i < totalRequests; i++ {
		args := json.RawMessage(fmt.Sprintf(`{"iter":%d}`, i+1))
		sess := fmt.Sprintf("%s-%d", sessionBase, i+1)
		idKey, _, _ := idempotency.ComputeKey(h.tenantID, sess, "failover_echo", "echo hi", args)

		r, b, reqErr := h.postExecute(map[string]any{
			"agent_session_id": sess,
			"tool_name":        "failover_echo",
			"runtime":          "bash5",
			"entrypoint_code":  "echo hi",
			"args":             args,
			"mode":             "sync",
		})
		if reqErr != nil {
			t.Fatalf("request %d error after failover: %v", i, reqErr)
		}
		if r.StatusCode != http.StatusOK {
			t.Fatalf("request %d returned HTTP %d: %s", i, r.StatusCode, string(b))
		}
		var out store.ExecutionResponse
		if err := json.Unmarshal(b, &out); err != nil {
			t.Fatalf("decoding response %d: %v", i, err)
		}
		h.cleanupExecutionKeys(out.ExecutionID, idKey)

		if out.Status != "COMPLETED" {
			t.Fatalf("request %d status=%s, want COMPLETED", i, out.Status)
		}
		if out.WorkerNodeID != "worker-2" {
			t.Fatalf("request %d routed to %s instead of worker-2", i, out.WorkerNodeID)
		}
	}

	if got := w2.execCount.Load(); got != totalRequests {
		t.Fatalf("expected worker-2 execCount=%d, got %d", totalRequests, got)
	}
}
