package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/aegisrun/aegisrun/control-go/internal/telemetry"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type ctxKey string

const (
	ctxTenantKey    ctxKey = "aegis_tenant_id"
	ctxRequestIDKey ctxKey = "aegis_request_id"
)

var tokenBucketScript = redis.NewScript(`
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local data = redis.call("HMGET", key, "tokens", "last_refill_ms")
local tokens = tonumber(data[1])
local last_ms = tonumber(data[2])

if not tokens or not last_ms then
	tokens = burst
	last_ms = now_ms
else
	local elapsed_sec = math.max(0, (now_ms - last_ms) / 1000.0)
	tokens = math.min(burst, tokens + elapsed_sec * rate)
	last_ms = now_ms
end

if tokens < requested then
	local missing = requested - tokens
	local retry_ms = math.ceil((missing / rate) * 1000.0)
	redis.call("HSET", key, "tokens", string.format("%.6f", tokens), "last_refill_ms", tostring(last_ms))
	redis.call("EXPIRE", key, 120)
	return {0, retry_ms}
end

tokens = tokens - requested
redis.call("HSET", key, "tokens", string.format("%.6f", tokens), "last_refill_ms", tostring(last_ms))
redis.call("EXPIRE", key, 120)
return {1, 0}
`)

var activeAcquireScript = redis.NewScript(`
local cur = tonumber(redis.call("GET", KEYS[1]) or "0")
local limit = tonumber(ARGV[1])
if cur >= limit then
	return {0, cur}
end
local next_val = redis.call("INCR", KEYS[1])
redis.call("EXPIRE", KEYS[1], tonumber(ARGV[2]))
return {1, next_val}
`)

var activeReleaseScript = redis.NewScript(`
local cur = tonumber(redis.call("GET", KEYS[1]) or "0")
if cur <= 1 then
	redis.call("DEL", KEYS[1])
	return 0
end
return redis.call("DECR", KEYS[1])
`)

var rateAndActiveAcquireScript = redis.NewScript(`
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])
local limit = tonumber(ARGV[5])
local active_ttl = tonumber(ARGV[6])

local data = redis.call("HMGET", KEYS[1], "tokens", "last_refill_ms")
local tokens = tonumber(data[1])
local last_ms = tonumber(data[2])

if not tokens or not last_ms then
	tokens = burst
	last_ms = now_ms
else
	local elapsed_sec = math.max(0, (now_ms - last_ms) / 1000.0)
	tokens = math.min(burst, tokens + elapsed_sec * rate)
	last_ms = now_ms
end

if tokens < requested then
	local missing = requested - tokens
	local retry_ms = math.ceil((missing / rate) * 1000.0)
	redis.call("HSET", KEYS[1], "tokens", string.format("%.6f", tokens), "last_refill_ms", tostring(last_ms))
	redis.call("EXPIRE", KEYS[1], 120)
	return {-1, retry_ms, 0}
end

tokens = tokens - requested
redis.call("HSET", KEYS[1], "tokens", string.format("%.6f", tokens), "last_refill_ms", tostring(last_ms))
redis.call("EXPIRE", KEYS[1], 120)

local cur = tonumber(redis.call("GET", KEYS[2]) or "0")
if cur >= limit then
	return {-2, 0, cur}
end
local next_val = redis.call("INCR", KEYS[2])
redis.call("EXPIRE", KEYS[2], active_ttl)
return {1, 0, next_val}
`)

// TenantFromContext retrieves the authenticated tenant ID injected by BearerAuth middleware.
func TenantFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxTenantKey).(string)
	return v
}

// RequestIDFromContext retrieves the correlation request ID injected by RequestID middleware.
func RequestIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxRequestIDKey).(string)
	return v
}

// RateLimiterConfig configures per-tenant token bucket and concurrent request bounds in Redis.
type RateLimiterConfig struct {
	Redis              *redis.Client
	RequestsPerSecond  float64
	Burst              float64
	MaxActivePerTenant int64
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// Flush forwards SSE flushes to the underlying ResponseWriter.
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap supports Go 1.20+ http.ResponseController.
func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

// RequestIDMiddleware sets or propagates X-Request-ID.
func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
		if reqID == "" {
			reqID = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", reqID)
		ctx := context.WithValue(r.Context(), ctxRequestIDKey, reqID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RecoveryMiddleware catches handler panics cleanly without tearing down the gateway process.
func RecoveryMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				logger.Error("recovered HTTP handler panic",
					"request_id", RequestIDFromContext(r.Context()),
					"path", r.URL.Path,
					"panic", fmt.Sprint(rec),
					"stack", string(debug.Stack()),
				)
				writeJSONError(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// MetricsAndLoggingMiddleware records Prometheus request counts and structured access logs.
func MetricsAndLoggingMiddleware(routeName string, metrics *telemetry.Metrics, logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		metrics.RecordHTTPRequest(r.Method, routeName, rec.status)
		logger.Debug("http request completed",
			"request_id", RequestIDFromContext(r.Context()),
			"method", r.Method,
			"route", routeName,
			"status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

// BearerAuthMiddleware authenticates `Authorization: Bearer <token>` and sets tenant context.
func BearerAuthMiddleware(apiKeys map[string]string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
		const prefix = "Bearer "
		if !strings.HasPrefix(authHeader, prefix) {
			writeJSONError(w, http.StatusUnauthorized, "missing or invalid Authorization Bearer token")
			return
		}
		token := strings.TrimSpace(strings.TrimPrefix(authHeader, prefix))
		tenantID, ok := apiKeys[token]
		if !ok || tenantID == "" {
			writeJSONError(w, http.StatusUnauthorized, "unauthorized API token")
			return
		}
		ctx := context.WithValue(r.Context(), ctxTenantKey, tenantID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RateLimitAndConcurrencyMiddleware enforces atomic Redis Lua token-bucket RPS and active sandbox limits.
func RateLimitAndConcurrencyMiddleware(cfg RateLimiterConfig, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cfg.Redis == nil {
			next.ServeHTTP(w, r)
			return
		}
		tenantID := TenantFromContext(r.Context())
		if tenantID == "" {
			tenantID = "default"
		}

		rateEnabled := cfg.RequestsPerSecond > 0 && cfg.Burst > 0
		activeEnabled := cfg.MaxActivePerTenant > 0
		rlKey := "aegis:rl:" + tenantID
		activeKey := "aegis:active_count:" + tenantID
		var activeAcquired bool

		if rateEnabled && activeEnabled {
			nowMs := time.Now().UnixMilli()
			res, err := rateAndActiveAcquireScript.Run(r.Context(), cfg.Redis, []string{rlKey, activeKey},
				strconv.FormatFloat(cfg.RequestsPerSecond, 'f', 4, 64),
				strconv.FormatFloat(cfg.Burst, 'f', 4, 64),
				strconv.FormatInt(nowMs, 10),
				"1",
				strconv.FormatInt(cfg.MaxActivePerTenant, 10),
				"300",
			).Int64Slice()
			if err != nil {
				writeJSONError(w, http.StatusServiceUnavailable, "rate limiter backend unavailable")
				return
			}
			if len(res) >= 2 && res[0] == -1 {
				retrySec := int(math.Ceil(float64(res[1]) / 1000.0))
				if retrySec < 1 {
					retrySec = 1
				}
				w.Header().Set("Retry-After", strconv.Itoa(retrySec))
				writeJSONError(w, http.StatusTooManyRequests, "tenant rate limit exceeded")
				return
			}
			if len(res) >= 1 && res[0] == -2 {
				w.Header().Set("Retry-After", "1")
				writeJSONError(w, http.StatusTooManyRequests, "tenant max concurrent executions exceeded")
				return
			}
			activeAcquired = true
		} else {
			if rateEnabled {
				nowMs := time.Now().UnixMilli()
				res, err := tokenBucketScript.Run(r.Context(), cfg.Redis, []string{rlKey},
					strconv.FormatFloat(cfg.RequestsPerSecond, 'f', 4, 64),
					strconv.FormatFloat(cfg.Burst, 'f', 4, 64),
					strconv.FormatInt(nowMs, 10),
					"1",
				).Int64Slice()
				if err != nil {
					writeJSONError(w, http.StatusServiceUnavailable, "rate limiter backend unavailable")
					return
				}
				if len(res) == 2 && res[0] == 0 {
					retrySec := int(math.Ceil(float64(res[1]) / 1000.0))
					if retrySec < 1 {
						retrySec = 1
					}
					w.Header().Set("Retry-After", strconv.Itoa(retrySec))
					writeJSONError(w, http.StatusTooManyRequests, "tenant rate limit exceeded")
					return
				}
			}

			if activeEnabled {
				res, err := activeAcquireScript.Run(r.Context(), cfg.Redis, []string{activeKey},
					strconv.FormatInt(cfg.MaxActivePerTenant, 10),
					"300",
				).Int64Slice()
				if err != nil {
					writeJSONError(w, http.StatusServiceUnavailable, "concurrency limiter backend unavailable")
					return
				}
				if len(res) == 2 && res[0] == 0 {
					w.Header().Set("Retry-After", "1")
					writeJSONError(w, http.StatusTooManyRequests, "tenant max concurrent executions exceeded")
					return
				}
				activeAcquired = true
			}
		}

		if activeAcquired {
			defer func() {
				_, _ = activeReleaseScript.Run(context.Background(), cfg.Redis, []string{activeKey}).Result()
			}()
		}

		next.ServeHTTP(w, r)
	})
}

func writeJSONError(w http.ResponseWriter, statusCode int, msg string) {
	writeJSON(w, statusCode, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	body, err := json.Marshal(payload)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(statusCode)
	_, _ = w.Write(body)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

