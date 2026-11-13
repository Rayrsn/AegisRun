package hitl

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/aegisrun/aegisrun/control-go/internal/store"
	"github.com/aegisrun/aegisrun/control-go/internal/telemetry"
)

const webhookMaxAttempts = 3

// WebhookPayload is delivered via HTTP POST to the caller-supplied HITL approval webhook.
type WebhookPayload struct {
	ExecutionID   string    `json:"execution_id"`
	TenantID      string    `json:"tenant_id"`
	ToolName      string    `json:"tool_name"`
	Runtime       string    `json:"runtime"`
	ApprovalToken string    `json:"approval_token"`
	ExpiresAt     time.Time `json:"expires_at"`
}

// Notifier generates cryptographically random HITL tokens, dispatches webhooks with backoff, and runs the 30s expiration reaper.
type Notifier struct {
	client  *http.Client
	store   store.Store
	broker  *store.StreamBroker
	metrics *telemetry.Metrics
	logger  *slog.Logger

	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

// NewNotifier initializes a HITL notifier and reaper bound to graceful shutdown.
func NewNotifier(
	st store.Store,
	broker *store.StreamBroker,
	metrics *telemetry.Metrics,
	webhookTimeout time.Duration,
	logger *slog.Logger,
) *Notifier {
	if webhookTimeout <= 0 {
		webhookTimeout = 5 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Notifier{
		client: &http.Client{
			Timeout: webhookTimeout,
		},
		store:   st,
		broker:  broker,
		metrics: metrics,
		logger:  logger,
	}
}

// GenerateToken mints a 32-byte URL-safe random token along with its hex-encoded SHA-256 hash.
func GenerateToken() (string, string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", "", fmt.Errorf("generating random approval token: %w", err)
	}
	raw := base64.RawURLEncoding.EncodeToString(buf)
	return raw, HashToken(raw), nil
}

// HashToken hashes a raw approval token into the fixed 64-char SHA-256 hex digest persisted in PostgreSQL.
func HashToken(rawToken string) string {
	sum := sha256.Sum256([]byte(rawToken))
	return hex.EncodeToString(sum[:])
}

// SendWebhook posts the HITL approval request up to 3 times with exponential backoff.
func (n *Notifier) SendWebhook(ctx context.Context, webhookURL string, payload WebhookPayload) error {
	if webhookURL == "" {
		return nil
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshaling HITL webhook payload: %w", err)
	}

	backoff := 100 * time.Millisecond
	var lastErr error
	for attempt := 1; attempt <= webhookMaxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("building webhook request to %s: %w", webhookURL, err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "AegisRun-Control/1.0")

		resp, err := n.client.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			lastErr = fmt.Errorf("webhook %s returned HTTP %d", webhookURL, resp.StatusCode)
		} else {
			lastErr = fmt.Errorf("posting webhook %s: %w", webhookURL, err)
		}

		if attempt < webhookMaxAttempts {
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			backoff *= 2
		}
	}
	return lastErr
}

// DispatchWebhookAsync dispatches the webhook on a shutdown-tracked background goroutine.
func (n *Notifier) DispatchWebhookAsync(parentCtx context.Context, webhookURL string, payload WebhookPayload) {
	if webhookURL == "" {
		return
	}
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	n.wg.Add(1)
	n.mu.Unlock()

	go func() {
		defer n.wg.Done()
		ctx, cancel := context.WithTimeout(parentCtx, 15*time.Second)
		defer cancel()
		if err := n.SendWebhook(ctx, webhookURL, payload); err != nil {
			n.logger.Warn("HITL webhook delivery failed after retries",
				"execution_id", payload.ExecutionID,
				"webhook_url", webhookURL,
				"error", err,
			)
		}
	}()
}

// StartReaper starts the periodic HITL approval expiration reaper loop tied to ctx cancellation and Shutdown.
func (n *Notifier) StartReaper(ctx context.Context, interval time.Duration, onReaped func(context.Context, []string)) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	n.wg.Add(1)
	n.mu.Unlock()

	go func() {
		defer n.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				reapCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				expiredIDs, err := n.store.ReapExpiredApprovals(reapCtx)
				if err != nil {
					if ctx.Err() == nil {
						n.logger.Error("reaping expired HITL approvals failed", "error", err)
					}
					cancel()
					continue
				}
				if len(expiredIDs) > 0 {
					for _, execID := range expiredIDs {
						n.metrics.RecordHITL("expired")
						if n.broker != nil {
							_ = n.broker.Publish(reapCtx, &store.StreamEvent{
								ExecutionID:  execID,
								SequenceNum:  uint64(time.Now().UnixNano()),
								TimestampNs:  uint64(time.Now().UnixNano()),
								EventType:    "REJECTED",
								Decision:     "REJECTED",
								DecidedBy:    "system:timeout",
								ErrorMessage: "approval request expired",
								Terminal:     true,
							})
							_ = n.broker.DeletePendingPayload(reapCtx, execID)
						}
					}
					if onReaped != nil {
						onReaped(reapCtx, expiredIDs)
					}
				}
				cancel()
			}
		}
	}()
}

// Shutdown waits for all tracked webhook deliveries and reaper loops to exit.
func (n *Notifier) Shutdown(ctx context.Context) error {
	n.mu.Lock()
	n.closed = true
	n.mu.Unlock()

	done := make(chan struct{})
	go func() {
		n.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
