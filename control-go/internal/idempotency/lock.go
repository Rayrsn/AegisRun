package idempotency

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"

	"github.com/aegisrun/aegisrun/control-go/internal/store"
	"github.com/redis/go-redis/v9"
)

const (
	defaultInFlightTTL  = 60 * time.Second
	defaultCompletedTTL = 900 * time.Second
)

// Outcome describes the atomic state transition returned by Acquire.
type Outcome string

const (
	OutcomeAcquired     Outcome = "ACQUIRED"
	OutcomeInFlight     Outcome = "IN_FLIGHT"
	OutcomeCompleted    Outcome = "COMPLETED"
)

// Record mirrors the JSON structure stored under `aegis:idemp:{key}`.
type Record struct {
	Status         string                   `json:"status"`
	ExecutionID    string                   `json:"execution_id"`
	CachedResponse *store.ExecutionResponse `json:"cached_response,omitempty"`
}

// AcquireResult reports whether the caller owns execution or should piggyback/replay.
type AcquireResult struct {
	Outcome Outcome
	Record  Record
}

var acquireScript = redis.NewScript(`
local existing = redis.call("GET", KEYS[1])
if not existing then
	redis.call("SET", KEYS[1], ARGV[1], "EX", tonumber(ARGV[2]))
	return {"ACQUIRED", ARGV[1]}
end
return {"EXISTS", existing}
`)

var completeScript = redis.NewScript(`
local existing = redis.call("GET", KEYS[1])
if existing then
	local ok, decoded = pcall(cjson.decode, existing)
	if ok and decoded and decoded["execution_id"] and decoded["execution_id"] ~= ARGV[1] then
		return 0
	end
end
redis.call("SET", KEYS[1], ARGV[2], "EX", tonumber(ARGV[3]))
return 1
`)

var releaseScript = redis.NewScript(`
local existing = redis.call("GET", KEYS[1])
if not existing then
	return 0
end
local ok, decoded = pcall(cjson.decode, existing)
if ok and decoded and decoded["execution_id"] == ARGV[1] and decoded["status"] == "IN_FLIGHT" then
	return redis.call("DEL", KEYS[1])
end
return 0
`)

var renewScript = redis.NewScript(`
local existing = redis.call("GET", KEYS[1])
if not existing then
	return 0
end
local ok, decoded = pcall(cjson.decode, existing)
if ok and decoded and decoded["execution_id"] == ARGV[1] and decoded["status"] == "IN_FLIGHT" then
	return redis.call("EXPIRE", KEYS[1], tonumber(ARGV[2]))
end
return 0
`)

// Locker coordinates atomic idempotency locks and response caching in Redis.
type Locker struct {
	rdb          *redis.Client
	inFlightTTL  time.Duration
	completedTTL time.Duration
}

// NewLocker creates a Locker with default 60s in-flight and 900s completed TTLs.
func NewLocker(rdb *redis.Client) *Locker {
	return &Locker{
		rdb:          rdb,
		inFlightTTL:  defaultInFlightTTL,
		completedTTL: defaultCompletedTTL,
	}
}

func redisKey(idempKey string) string {
	return "aegis:idemp:" + idempKey
}

// CanonicalJSON serializes arbitrary JSON-compatible values deterministically with sorted keys.
func CanonicalJSON(raw json.RawMessage) ([]byte, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return []byte("null"), nil
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.UseNumber()

	var decoded any
	if err := dec.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("decoding JSON for canonicalization: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("unexpected trailing JSON token")
	}

	var buf bytes.Buffer
	if err := writeCanonical(&buf, decoded); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeCanonical(buf *bytes.Buffer, v any) error {
	switch val := v.(type) {
	case nil:
		buf.WriteString("null")
	case bool:
		if val {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case json.Number:
		buf.WriteString(val.String())
	case string:
		encoded, err := json.Marshal(val)
		if err != nil {
			return fmt.Errorf("marshaling canonical string: %w", err)
		}
		buf.Write(encoded)
	case []any:
		buf.WriteByte('[')
		for i, item := range val {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeCanonical(buf, item); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			encodedKey, err := json.Marshal(k)
			if err != nil {
				return fmt.Errorf("marshaling canonical map key: %w", err)
			}
			buf.Write(encodedKey)
			buf.WriteByte(':')
			if err := writeCanonical(buf, val[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	default:
		// Fallback for Go values passed directly in unit/integration calls.
		raw, err := json.Marshal(val)
		if err != nil {
			return fmt.Errorf("marshaling fallback canonical value: %w", err)
		}
		canon, err := CanonicalJSON(raw)
		if err != nil {
			return err
		}
		buf.Write(canon)
	}
	return nil
}

// ComputeKey computes `SHA256(tenant:session:tool:sha256(code):canonical_args)` and returns both the key and code SHA-256.
func ComputeKey(tenantID, sessionID, toolName, code string, args json.RawMessage) (string, string, error) {
	codeSumBytes := sha256.Sum256([]byte(code))
	codeSHA := hex.EncodeToString(codeSumBytes[:])

	canonArgs, err := CanonicalJSON(args)
	if err != nil {
		return "", "", fmt.Errorf("canonicalizing tool arguments: %w", err)
	}

	h := sha256.New()
	h.Write([]byte(tenantID))
	h.Write([]byte{':'})
	h.Write([]byte(sessionID))
	h.Write([]byte{':'})
	h.Write([]byte(toolName))
	h.Write([]byte{':'})
	h.Write([]byte(codeSHA))
	h.Write([]byte{':'})
	h.Write(canonArgs)

	return hex.EncodeToString(h.Sum(nil)), codeSHA, nil
}

// Acquire atomically claims an idempotency key or returns an existing IN_FLIGHT or COMPLETED record.
func (l *Locker) Acquire(ctx context.Context, key, candidateExecutionID string) (*AcquireResult, error) {
	initial := Record{
		Status:      string(OutcomeInFlight),
		ExecutionID: candidateExecutionID,
	}
	raw, err := json.Marshal(initial)
	if err != nil {
		return nil, fmt.Errorf("marshaling initial idempotency record: %w", err)
	}

	ttlSec := int(l.inFlightTTL.Seconds())
	if ttlSec < 1 {
		ttlSec = 60
	}

	res, err := acquireScript.Run(ctx, l.rdb, []string{redisKey(key)}, string(raw), strconv.Itoa(ttlSec)).StringSlice()
	if err != nil {
		return nil, fmt.Errorf("running acquire lua script for %s: %w", key, err)
	}
	if len(res) != 2 {
		return nil, fmt.Errorf("unexpected acquire lua response length %d", len(res))
	}

	var rec Record
	if err := json.Unmarshal([]byte(res[1]), &rec); err != nil {
		return nil, fmt.Errorf("unmarshaling idempotency record for %s: %w", key, err)
	}

	if res[0] == "ACQUIRED" {
		return &AcquireResult{Outcome: OutcomeAcquired, Record: rec}, nil
	}
	if rec.Status == string(OutcomeCompleted) {
		return &AcquireResult{Outcome: OutcomeCompleted, Record: rec}, nil
	}
	return &AcquireResult{Outcome: OutcomeInFlight, Record: rec}, nil
}

// Complete persists the terminal cached response for 900s cleanly when owned by executionID.
func (l *Locker) Complete(ctx context.Context, key, executionID string, resp *store.ExecutionResponse) error {
	rec := Record{
		Status:         string(OutcomeCompleted),
		ExecutionID:    executionID,
		CachedResponse: resp,
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshaling completed idempotency record: %w", err)
	}

	ttlSec := int(l.completedTTL.Seconds())
	if ttlSec < 1 {
		ttlSec = 900
	}

	if _, err := completeScript.Run(ctx, l.rdb, []string{redisKey(key)}, executionID, string(raw), strconv.Itoa(ttlSec)).Result(); err != nil {
		return fmt.Errorf("running complete lua script for %s: %w", key, err)
	}
	return nil
}

// Release deletes an IN_FLIGHT lock only if still owned by executionID (e.g., on pre-STARTED transient failure).
func (l *Locker) Release(ctx context.Context, key, executionID string) error {
	if _, err := releaseScript.Run(ctx, l.rdb, []string{redisKey(key)}, executionID).Result(); err != nil {
		return fmt.Errorf("running release lua script for %s: %w", key, err)
	}
	return nil
}

// Renew extends the TTL of an IN_FLIGHT lock owned by executionID.
func (l *Locker) Renew(ctx context.Context, key, executionID string, ttl time.Duration) error {
	ttlSec := int(ttl.Seconds())
	if ttlSec < 1 {
		ttlSec = 60
	}
	if _, err := renewScript.Run(ctx, l.rdb, []string{redisKey(key)}, executionID, strconv.Itoa(ttlSec)).Result(); err != nil {
		return fmt.Errorf("running renew lua script for %s: %w", key, err)
	}
	return nil
}
