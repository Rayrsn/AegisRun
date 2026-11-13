package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	streamHistoryTTL    = 900 * time.Second
	hitlPayloadExtraTTL = 10 * time.Minute
)

// ExecutionMetricsJSON represents execution metrics serialized into JSON responses and SSE events.
type ExecutionMetricsJSON struct {
	ExitCode           int32  `json:"exit_code"`
	Signaled           int32  `json:"signaled,omitempty"`
	SetupDurationUs    uint64 `json:"setup_duration_us"`
	ExecDurationUs     uint64 `json:"exec_duration_us"`
	TeardownDurationUs uint64 `json:"teardown_duration_us"`
	PeakMemoryBytes    uint64 `json:"peak_memory_bytes"`
	CpuUsageUs         uint64 `json:"cpu_usage_us"`
}

// ExecutionResponse is the canonical JSON result returned by `/v1/execute` and stored in idempotency caches.
type ExecutionResponse struct {
	ExecutionID        string                `json:"execution_id"`
	Status             string                `json:"status"`
	Runtime            string                `json:"runtime"`
	ToolName           string                `json:"tool_name,omitempty"`
	WorkerNodeID       string                `json:"worker_node_id,omitempty"`
	ExitCode           int32                 `json:"exit_code"`
	Signaled           int32                 `json:"signaled,omitempty"`
	Stdout             string                `json:"stdout"`
	Stderr             string                `json:"stderr"`
	SetupDurationUs    uint64                `json:"setup_duration_us"`
	ExecDurationUs     uint64                `json:"exec_duration_us"`
	TeardownDurationUs uint64                `json:"teardown_duration_us"`
	PeakMemoryBytes    uint64                `json:"peak_memory_bytes"`
	CpuUsageUs         uint64                `json:"cpu_usage_us"`
	ErrorMessage       string                `json:"error_message,omitempty"`
	ApprovalRequired   bool                  `json:"approval_required,omitempty"`
	ExpiresAt          string                `json:"expires_at,omitempty"`
	Metrics            *ExecutionMetricsJSON `json:"metrics,omitempty"`
}

// StreamEvent represents a single ordered event published over Redis Pub/Sub and SSE.
type StreamEvent struct {
	ExecutionID   string                `json:"execution_id"`
	SequenceNum   uint64                `json:"sequence_num"`
	TimestampNs   uint64                `json:"timestamp_ns"`
	EventType     string                `json:"event_type"`
	Data          string                `json:"data,omitempty"`
	ErrorMessage  string                `json:"error_message,omitempty"`
	ApprovalToken string                `json:"approval_token,omitempty"`
	WebhookURL    string                `json:"webhook_url,omitempty"`
	ExpiresAt     string                `json:"expires_at,omitempty"`
	Decision      string                `json:"decision,omitempty"`
	DecidedBy     string                `json:"decided_by,omitempty"`
	WorkerNodeID  string                `json:"worker_node_id,omitempty"`
	Metrics       *ExecutionMetricsJSON `json:"metrics,omitempty"`
	Terminal      bool                  `json:"terminal"`
}

// ResourceLimitsSpec stores execution limits across async/HITL queues.
type ResourceLimitsSpec struct {
	WallTimeoutMs  uint32 `json:"wall_timeout_ms"`
	MemoryMaxBytes uint64 `json:"memory_max_bytes"`
	CpuQuotaUs     uint32 `json:"cpu_quota_us"`
	PidsMax        uint32 `json:"pids_max"`
	TmpfsSizeBytes uint64 `json:"tmpfs_size_bytes"`
}

// PendingExecutionPayload persists full request details needed to resume HITL-gated executions.
type PendingExecutionPayload struct {
	ExecutionID        string             `json:"execution_id"`
	TenantID           string             `json:"tenant_id"`
	AgentSessionID     string             `json:"agent_session_id"`
	ToolName           string             `json:"tool_name"`
	IdempotencyKey     string             `json:"idempotency_key"`
	Runtime            string             `json:"runtime"`
	EntrypointCode     string             `json:"entrypoint_code"`
	StdinPayload       []byte             `json:"stdin_payload,omitempty"`
	EnvVars            map[string]string  `json:"env_vars,omitempty"`
	Limits             ResourceLimitsSpec `json:"limits"`
	EnableLoopbackOnly bool               `json:"enable_loopback_only"`
	WebhookURL         string             `json:"webhook_url,omitempty"`
	BaseSeq            uint64             `json:"base_seq"`
}

// StreamBroker fans out ordered execution events across concurrent HTTP/SSE piggybacking clients.
type StreamBroker struct {
	rdb *redis.Client
	wg  sync.WaitGroup
}

// NewStreamBroker initializes a Redis-backed execution event broker.
func NewStreamBroker(rdb *redis.Client) *StreamBroker {
	return &StreamBroker{rdb: rdb}
}

// Shutdown waits for all active subscriber relay goroutines to exit after ctx cancellation.
func (b *StreamBroker) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func historyKey(executionID string) string {
	return "aegis:history:exec:" + executionID
}

func pubsubChannel(executionID string) string {
	return "aegis:pubsub:exec:" + executionID
}

func hitlPayloadKey(executionID string) string {
	return "aegis:hitl:payload:" + executionID
}

// Publish appends an event to the short-lived replay list and broadcasts it to live subscribers.
func (b *StreamBroker) Publish(ctx context.Context, evt *StreamEvent) error {
	raw, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshaling stream event for %s: %w", evt.ExecutionID, err)
	}

	hKey := historyKey(evt.ExecutionID)
	ch := pubsubChannel(evt.ExecutionID)

	pipe := b.rdb.TxPipeline()
	pipe.RPush(ctx, hKey, string(raw))
	pipe.Expire(ctx, hKey, streamHistoryTTL)
	pipe.Publish(ctx, ch, string(raw))
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("publishing stream event seq=%d for %s: %w", evt.SequenceNum, evt.ExecutionID, err)
	}
	return nil
}

// Subscribe replays any historical events for executionID and streams live events until Terminal or ctx cancellation.
func (b *StreamBroker) Subscribe(ctx context.Context, executionID string) (<-chan *StreamEvent, error) {
	pubsub := b.rdb.Subscribe(ctx, pubsubChannel(executionID))
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		return nil, fmt.Errorf("subscribing to pubsub channel for %s: %w", executionID, err)
	}

	historyRaw, err := b.rdb.LRange(ctx, historyKey(executionID), 0, -1).Result()
	if err != nil && err != redis.Nil {
		_ = pubsub.Close()
		return nil, fmt.Errorf("reading stream history for %s: %w", executionID, err)
	}

	out := make(chan *StreamEvent, 64)
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		defer close(out)
		defer func() { _ = pubsub.Close() }()

		var lastSeq uint64
		for _, item := range historyRaw {
			var evt StreamEvent
			if err := json.Unmarshal([]byte(item), &evt); err != nil {
				continue
			}
			if evt.SequenceNum <= lastSeq && lastSeq > 0 {
				continue
			}
			lastSeq = evt.SequenceNum
			select {
			case out <- &evt:
			case <-ctx.Done():
				return
			}
			if evt.Terminal {
				return
			}
		}

		msgCh := pubsub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				var evt StreamEvent
				if err := json.Unmarshal([]byte(msg.Payload), &evt); err != nil {
					continue
				}
				if evt.SequenceNum <= lastSeq && lastSeq > 0 {
					continue
				}
				lastSeq = evt.SequenceNum
				select {
				case out <- &evt:
				case <-ctx.Done():
					return
				}
				if evt.Terminal {
					return
				}
			}
		}
	}()

	return out, nil
}

// SavePendingPayload stores a paused HITL execution's input payload in Redis until approval resolution.
func (b *StreamBroker) SavePendingPayload(ctx context.Context, payload *PendingExecutionPayload, ttl time.Duration) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshaling pending HITL payload for %s: %w", payload.ExecutionID, err)
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	if err := b.rdb.Set(ctx, hitlPayloadKey(payload.ExecutionID), raw, ttl+hitlPayloadExtraTTL).Err(); err != nil {
		return fmt.Errorf("saving pending HITL payload for %s: %w", payload.ExecutionID, err)
	}
	return nil
}

// LoadPendingPayload retrieves a paused HITL execution's input payload from Redis.
func (b *StreamBroker) LoadPendingPayload(ctx context.Context, executionID string) (*PendingExecutionPayload, error) {
	raw, err := b.rdb.Get(ctx, hitlPayloadKey(executionID)).Bytes()
	if err != nil {
		return nil, fmt.Errorf("loading pending HITL payload for %s: %w", executionID, err)
	}
	var payload PendingExecutionPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("unmarshaling pending HITL payload for %s: %w", executionID, err)
	}
	return &payload, nil
}

// DeletePendingPayload removes a pending HITL execution payload after resolution.
func (b *StreamBroker) DeletePendingPayload(ctx context.Context, executionID string) error {
	if err := b.rdb.Del(ctx, hitlPayloadKey(executionID)).Err(); err != nil {
		return fmt.Errorf("deleting pending HITL payload for %s: %w", executionID, err)
	}
	return nil
}
