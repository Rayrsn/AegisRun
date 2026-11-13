package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds runtime configuration for the Go control plane orchestrator.
type Config struct {
	HTTPAddr             string
	PostgresDSN          string
	RedisAddr            string
	RedisPassword        string
	RedisDB              int
	WorkerTargets        map[string]string
	APIKeys              map[string]string
	RateLimitRPS         float64
	RateLimitBurst       float64
	MaxActivePerTenant   int64
	HeartbeatTimeout     time.Duration
	ApprovalTTL          time.Duration
	ApprovalReapInterval time.Duration
	ShutdownTimeout      time.Duration
	WebhookTimeout       time.Duration
	HTTPReadTimeout      time.Duration
	HTTPWriteTimeout     time.Duration
	HTTPIdleTimeout      time.Duration
}

// LoadFromEnv loads configuration from environment variables with sensible development defaults.
func LoadFromEnv() (*Config, error) {
	redisDB, err := parseIntEnv("AEGIS_REDIS_DB", 0)
	if err != nil {
		return nil, err
	}
	rps, err := parseFloatEnv("AEGIS_RATE_LIMIT_RPS", 50)
	if err != nil {
		return nil, err
	}
	burst, err := parseFloatEnv("AEGIS_RATE_LIMIT_BURST", 100)
	if err != nil {
		return nil, err
	}
	maxActive, err := parseInt64Env("AEGIS_MAX_ACTIVE_PER_TENANT", 32)
	if err != nil {
		return nil, err
	}
	hbTimeout, err := parseDurationEnv("AEGIS_HEARTBEAT_TIMEOUT", 5*time.Second)
	if err != nil {
		return nil, err
	}
	approvalTTL, err := parseDurationEnv("AEGIS_APPROVAL_TTL", 5*time.Minute)
	if err != nil {
		return nil, err
	}
	reapInterval, err := parseDurationEnv("AEGIS_APPROVAL_REAP_INTERVAL", 30*time.Second)
	if err != nil {
		return nil, err
	}
	shutdownTimeout, err := parseDurationEnv("AEGIS_SHUTDOWN_TIMEOUT", 30*time.Second)
	if err != nil {
		return nil, err
	}
	webhookTimeout, err := parseDurationEnv("AEGIS_WEBHOOK_TIMEOUT", 5*time.Second)
	if err != nil {
		return nil, err
	}
	readTimeout, err := parseDurationEnv("AEGIS_HTTP_READ_TIMEOUT", 15*time.Second)
	if err != nil {
		return nil, err
	}
	writeTimeout, err := parseDurationEnv("AEGIS_HTTP_WRITE_TIMEOUT", 0)
	if err != nil {
		return nil, err
	}
	idleTimeout, err := parseDurationEnv("AEGIS_HTTP_IDLE_TIMEOUT", 60*time.Second)
	if err != nil {
		return nil, err
	}

	return &Config{
		HTTPAddr:             getEnv("AEGIS_HTTP_ADDR", ":8080"),
		PostgresDSN:          getEnv("AEGIS_POSTGRES_DSN", "postgres://aegis:aegis@localhost:5432/aegisrun?sslmode=disable"),
		RedisAddr:            getEnv("AEGIS_REDIS_ADDR", "localhost:6379"),
		RedisPassword:        getEnv("AEGIS_REDIS_PASSWORD", ""),
		RedisDB:              redisDB,
		WorkerTargets:        parseWorkerTargets(getEnv("AEGIS_WORKERS", "worker-1=localhost:50051")),
		APIKeys:              parseAPIKeys(getEnv("AEGIS_API_KEYS", "aegis-dev-token:default")),
		RateLimitRPS:         rps,
		RateLimitBurst:       burst,
		MaxActivePerTenant:   maxActive,
		HeartbeatTimeout:     hbTimeout,
		ApprovalTTL:          approvalTTL,
		ApprovalReapInterval: reapInterval,
		ShutdownTimeout:      shutdownTimeout,
		WebhookTimeout:       webhookTimeout,
		HTTPReadTimeout:      readTimeout,
		HTTPWriteTimeout:     writeTimeout,
		HTTPIdleTimeout:      idleTimeout,
	}, nil
}

func getEnv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func parseIntEnv(key string, fallback int) (int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("parsing %s=%q: %w", key, v, err)
	}
	return parsed, nil
}

func parseInt64Env(key string, fallback int64) (int64, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing %s=%q: %w", key, v, err)
	}
	return parsed, nil
}

func parseFloatEnv(key string, fallback float64) (float64, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing %s=%q: %w", key, v, err)
	}
	return parsed, nil
}

func parseDurationEnv(key string, fallback time.Duration) (time.Duration, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("parsing %s=%q: %w", key, v, err)
	}
	return parsed, nil
}

func parseWorkerTargets(raw string) map[string]string {
	out := make(map[string]string)
	for i, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if id, addr, ok := strings.Cut(part, "="); ok {
			id = strings.TrimSpace(id)
			addr = strings.TrimSpace(addr)
			if id != "" && addr != "" {
				out[id] = addr
			}
			continue
		}
		out[fmt.Sprintf("worker-%d", i+1)] = part
	}
	return out
}

func parseAPIKeys(raw string) map[string]string {
	out := make(map[string]string)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if token, tenant, ok := strings.Cut(part, ":"); ok {
			token = strings.TrimSpace(token)
			tenant = strings.TrimSpace(tenant)
			if token != "" && tenant != "" {
				out[token] = tenant
			}
			continue
		}
		out[part] = "default"
	}
	return out
}
