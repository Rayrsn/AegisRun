package orchestrator

import (
	"errors"
	"math"
	"time"

	"github.com/aegisrun/aegisrun/control-go/pkg/pb"
)

// ErrNoHealthyWorkers is returned when no healthy worker with capacity can handle an execution.
var ErrNoHealthyWorkers = errors.New("no healthy sandbox workers available")

// Scheduler selects the least-loaded healthy worker node from a WorkerPool.
type Scheduler struct {
	pool *WorkerPool
}

// NewScheduler creates a least-loaded worker scheduler over pool.
func NewScheduler(pool *WorkerPool) *Scheduler {
	return &Scheduler{pool: pool}
}

// ComputeLoadScore calculates `(ActiveSandboxes/MaxSandboxes)*0.7 + (MemoryUsedPct/100)*0.3`.
func ComputeLoadScore(w WorkerInfo) float64 {
	var sandboxRatio float64
	if w.MaxSandboxes > 0 {
		sandboxRatio = float64(w.ActiveSandboxes) / float64(w.MaxSandboxes)
	}
	memRatio := w.MemoryUsedPct / 100.0
	if memRatio < 0 {
		memRatio = 0
	} else if memRatio > 1 {
		memRatio = 1
	}

	score := sandboxRatio*0.7 + memRatio*0.3
	if w.Status == pb.WorkerStatus_DEGRADED {
		score += 0.25
	}
	return score
}

// SelectWorker chooses the lowest-score healthy worker not listed in exclude.
func (s *Scheduler) SelectWorker(exclude map[string]bool) (WorkerInfo, error) {
	candidates := s.pool.Snapshot(time.Now())

	var best WorkerInfo
	bestScore := math.MaxFloat64
	found := false

	for _, w := range candidates {
		if exclude[w.ID] || !w.Healthy || w.Client == nil {
			continue
		}
		if w.MaxSandboxes > 0 && w.ActiveSandboxes >= w.MaxSandboxes {
			continue
		}

		score := ComputeLoadScore(w)
		if !found ||
			score < bestScore-1e-9 ||
			(math.Abs(score-bestScore) <= 1e-9 &&
				(w.InFlightLocal < best.InFlightLocal ||
					(w.InFlightLocal == best.InFlightLocal && w.ID < best.ID))) {
			best = w
			bestScore = score
			found = true
		}
	}

	if !found {
		return WorkerInfo{}, ErrNoHealthyWorkers
	}
	return best, nil
}
