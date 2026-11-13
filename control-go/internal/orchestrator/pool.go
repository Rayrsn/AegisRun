package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aegisrun/aegisrun/control-go/internal/telemetry"
	"github.com/aegisrun/aegisrun/control-go/pkg/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// WorkerInfo is an immutable point-in-time snapshot of a sandbox worker's health and load.
type WorkerInfo struct {
	ID                   string
	Addr                 string
	Client               pb.SandboxWorkerClient
	ActiveSandboxes      uint32
	MaxSandboxes         uint32
	AvailableMemoryBytes uint64
	TotalMemoryBytes     uint64
	CpuPressurePsi       float64
	MemoryUsedPct        float64
	Status               pb.WorkerStatus
	LastHeartbeat        time.Time
	Healthy              bool
	InFlightLocal        int64
}

type managedWorker struct {
	id                   string
	addr                 string
	conn                 *grpc.ClientConn
	client               pb.SandboxWorkerClient
	activeSandboxes      uint32
	maxSandboxes         uint32
	availableMemoryBytes uint64
	totalMemoryBytes     uint64
	cpuPressurePsi       float64
	memoryUsedPct        float64
	status               pb.WorkerStatus
	lastHeartbeat        time.Time
	healthy              bool
	inFlightLocal        int64
}

// WorkerPool maintains persistent HTTP/2 gRPC connections and real-time heartbeat state for daemon-rs nodes.
type WorkerPool struct {
	mu               sync.RWMutex
	workers          map[string]*managedWorker
	heartbeatTimeout time.Duration
	metrics          *telemetry.Metrics
	logger           *slog.Logger

	rootCtx context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	readyMu sync.Mutex
	readyCh chan struct{}
}

// NewWorkerPool connects to each worker target and spawns tracked heartbeat subscribers.
func NewWorkerPool(
	targets map[string]string,
	heartbeatTimeout time.Duration,
	metrics *telemetry.Metrics,
	logger *slog.Logger,
) (*WorkerPool, error) {
	if heartbeatTimeout <= 0 {
		heartbeatTimeout = 5 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}

	ctx, cancel := context.WithCancel(context.Background())
	pool := &WorkerPool{
		workers:          make(map[string]*managedWorker, len(targets)),
		heartbeatTimeout: heartbeatTimeout,
		metrics:          metrics,
		logger:           logger,
		rootCtx:          ctx,
		cancel:           cancel,
		readyCh:          make(chan struct{}),
	}

	for workerID, addr := range targets {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			_ = pool.Close()
			return nil, fmt.Errorf("creating gRPC client for worker %s (%s): %w", workerID, addr, err)
		}
		mw := &managedWorker{
			id:     workerID,
			addr:   addr,
			conn:   conn,
			client: pb.NewSandboxWorkerClient(conn),
		}
		pool.workers[workerID] = mw
	}

	for workerID := range pool.workers {
		id := workerID
		pool.wg.Add(1)
		go func() {
			defer pool.wg.Done()
			pool.runHeartbeatSubscriber(id)
		}()
	}

	return pool, nil
}

func (p *WorkerPool) notifyReadyWaiters() {
	p.readyMu.Lock()
	close(p.readyCh)
	p.readyCh = make(chan struct{})
	p.readyMu.Unlock()
}

func (p *WorkerPool) currentReadyCh() <-chan struct{} {
	p.readyMu.Lock()
	ch := p.readyCh
	p.readyMu.Unlock()
	return ch
}

func (p *WorkerPool) runHeartbeatSubscriber(workerID string) {
	p.mu.RLock()
	mw := p.workers[workerID]
	p.mu.RUnlock()
	if mw == nil {
		return
	}

	backoff := 50 * time.Millisecond
	const maxBackoff = 1 * time.Second

	for {
		if p.rootCtx.Err() != nil {
			return
		}

		stream, err := mw.client.StreamWorkerHeartbeat(p.rootCtx, &pb.Empty{})
		if err != nil {
			p.MarkUnhealthy(workerID)
			if p.rootCtx.Err() == nil {
				p.logger.Debug("worker heartbeat stream connect failed", "worker_id", workerID, "error", err)
			}
			if !sleepWithContext(p.rootCtx, backoff) {
				return
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		backoff = 50 * time.Millisecond
		for {
			hb, recvErr := stream.Recv()
			if recvErr != nil {
				p.MarkUnhealthy(workerID)
				if p.rootCtx.Err() == nil {
					p.logger.Warn("worker heartbeat stream disconnected", "worker_id", workerID, "error", recvErr)
				}
				break
			}
			p.applyHeartbeat(workerID, hb)
		}

		if !sleepWithContext(p.rootCtx, backoff) {
			return
		}
	}
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (p *WorkerPool) applyHeartbeat(workerID string, hb *pb.WorkerHeartbeat) {
	if hb == nil {
		return
	}
	now := time.Now()
	p.mu.Lock()
	mw := p.workers[workerID]
	if mw != nil {
		mw.activeSandboxes = hb.GetActiveSandboxes()
		mw.maxSandboxes = hb.GetMaxSandboxes()
		mw.availableMemoryBytes = hb.GetAvailableMemoryBytes()
		mw.totalMemoryBytes = hb.GetTotalMemoryBytes()
		mw.cpuPressurePsi = hb.GetCpuPressurePsi()
		mw.memoryUsedPct = hb.GetMemoryUsedPct()
		mw.status = hb.GetStatus()
		mw.lastHeartbeat = now
		mw.healthy = mw.status == pb.WorkerStatus_HEALTHY || mw.status == pb.WorkerStatus_DEGRADED
	}
	p.mu.Unlock()

	p.metrics.SetWorkerActiveSandboxes(workerID, hb.GetActiveSandboxes())
	p.notifyReadyWaiters()
}

// MarkUnhealthy immediately excludes workerID from scheduling decisions until a fresh heartbeat arrives.
func (p *WorkerPool) MarkUnhealthy(workerID string) {
	p.mu.Lock()
	if mw, ok := p.workers[workerID]; ok {
		mw.healthy = false
	}
	p.mu.Unlock()
}

// IncInFlight adjusts the local optimistic in-flight counter when dispatching a sandbox to workerID.
func (p *WorkerPool) IncInFlight(workerID string, delta int64) {
	p.mu.Lock()
	if mw, ok := p.workers[workerID]; ok {
		mw.inFlightLocal += delta
		if mw.inFlightLocal < 0 {
			mw.inFlightLocal = 0
		}
	}
	p.mu.Unlock()
}

// HeartbeatTimeout returns the configured staleness window after which silent workers are unhealthy.
func (p *WorkerPool) HeartbeatTimeout() time.Duration {
	return p.heartbeatTimeout
}

// Snapshot returns point-in-time copies of all registered workers and their health state.
func (p *WorkerPool) Snapshot(now time.Time) []WorkerInfo {
	p.mu.RLock()
	defer p.mu.RUnlock()

	out := make([]WorkerInfo, 0, len(p.workers))
	for _, mw := range p.workers {
		healthy := mw.healthy &&
			!mw.lastHeartbeat.IsZero() &&
			now.Sub(mw.lastHeartbeat) <= p.heartbeatTimeout &&
			(mw.status == pb.WorkerStatus_HEALTHY || mw.status == pb.WorkerStatus_DEGRADED)

		out = append(out, WorkerInfo{
			ID:                   mw.id,
			Addr:                 mw.addr,
			Client:               mw.client,
			ActiveSandboxes:      mw.activeSandboxes,
			MaxSandboxes:         mw.maxSandboxes,
			AvailableMemoryBytes: mw.availableMemoryBytes,
			TotalMemoryBytes:     mw.totalMemoryBytes,
			CpuPressurePsi:       mw.cpuPressurePsi,
			MemoryUsedPct:        mw.memoryUsedPct,
			Status:               mw.status,
			LastHeartbeat:        mw.lastHeartbeat,
			Healthy:              healthy,
			InFlightLocal:        mw.inFlightLocal,
		})
	}
	return out
}

// WaitReady blocks until at least minHealthy workers have reported healthy heartbeats or ctx expires.
func (p *WorkerPool) WaitReady(ctx context.Context, minHealthy int) error {
	for {
		ch := p.currentReadyCh()
		healthyCount := 0
		for _, w := range p.Snapshot(time.Now()) {
			if w.Healthy {
				healthyCount++
			}
		}
		if healthyCount >= minHealthy {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
}

// Close terminates heartbeat subscribers and closes all gRPC connections deterministically.
func (p *WorkerPool) Close() error {
	if p == nil {
		return nil
	}
	p.cancel()

	p.mu.Lock()
	conns := make([]*grpc.ClientConn, 0, len(p.workers))
	for _, mw := range p.workers {
		mw.healthy = false
		if mw.conn != nil {
			conns = append(conns, mw.conn)
		}
	}
	p.mu.Unlock()

	var firstErr error
	for _, conn := range conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.wg.Wait()
	return firstErr
}
