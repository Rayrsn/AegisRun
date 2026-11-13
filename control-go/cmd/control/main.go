package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aegisrun/aegisrun/control-go/internal/api"
	"github.com/aegisrun/aegisrun/control-go/internal/config"
	"github.com/aegisrun/aegisrun/control-go/internal/hitl"
	"github.com/aegisrun/aegisrun/control-go/internal/idempotency"
	"github.com/aegisrun/aegisrun/control-go/internal/orchestrator"
	"github.com/aegisrun/aegisrun/control-go/internal/store"
	"github.com/aegisrun/aegisrun/control-go/internal/telemetry"
	"github.com/redis/go-redis/v9"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg, err := config.LoadFromEnv()
	if err != nil {
		logger.Error("loading control plane configuration failed", "error", err)
		os.Exit(1)
	}

	rootCtx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	initCtx, initCancel := context.WithTimeout(rootCtx, 10*time.Second)
	pgStore, err := store.NewPostgresStore(initCtx, cfg.PostgresDSN)
	initCancel()
	if err != nil {
		logger.Error("connecting to PostgreSQL failed", "error", err)
		os.Exit(1)
	}
	defer pgStore.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		Password:     cfg.RedisPassword,
		DB:           cfg.RedisDB,
		PoolSize:     256,
		MinIdleConns: 64,
	})
	defer func() { _ = rdb.Close() }()

	pingCtx, pingCancel := context.WithTimeout(rootCtx, 5*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		pingCancel()
		logger.Error("connecting to Redis failed", "error", err)
		os.Exit(1)
	}
	pingCancel()

	metrics := telemetry.NewMetrics()
	broker := store.NewStreamBroker(rdb)
	locker := idempotency.NewLocker(rdb)

	pool, err := orchestrator.NewWorkerPool(cfg.WorkerTargets, cfg.HeartbeatTimeout, metrics, logger)
	if err != nil {
		logger.Error("initializing worker pool failed", "error", err)
		os.Exit(1)
	}
	scheduler := orchestrator.NewScheduler(pool)
	notifier := hitl.NewNotifier(pgStore, broker, metrics, cfg.WebhookTimeout, logger)

	engine := orchestrator.NewEngine(
		pgStore,
		broker,
		locker,
		pool,
		scheduler,
		notifier,
		metrics,
		orchestrator.EngineConfig{DefaultApprovalTTL: cfg.ApprovalTTL},
		logger,
	)

	notifier.StartReaper(rootCtx, cfg.ApprovalReapInterval, engine.HandleExpiredApprovals)

	handler := api.NewRouter(api.RouterDeps{
		Engine:   engine,
		Broker:   broker,
		Pool:     pool,
		Metrics:  metrics,
		Logger:   logger,
		Postgres: pgStore.Pool(),
		Redis:    rdb,
		APIKeys:  cfg.APIKeys,
		RateLimiter: api.RateLimiterConfig{
			Redis:              rdb,
			RequestsPerSecond:  cfg.RateLimitRPS,
			Burst:              cfg.RateLimitBurst,
			MaxActivePerTenant: cfg.MaxActivePerTenant,
		},
	})

	srv := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      handler,
		ReadTimeout:  cfg.HTTPReadTimeout,
		WriteTimeout: cfg.HTTPWriteTimeout,
		IdleTimeout:  cfg.HTTPIdleTimeout,
	}

	var httpWG sync.WaitGroup
	errCh := make(chan error, 1)
	httpWG.Add(1)
	go func() {
		defer httpWG.Done()
		logger.Info("aegis control plane HTTP gateway listening", "addr", cfg.HTTPAddr)
		if serveErr := srv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- serveErr
		}
		close(errCh)
	}()

	select {
	case <-rootCtx.Done():
		logger.Info("shutdown signal received; draining control plane")
	case serveErr := <-errCh:
		if serveErr != nil {
			logger.Error("HTTP server failed", "error", serveErr)
		}
	}

	engine.BeginDrain()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("HTTP server graceful shutdown incomplete", "error", err)
	}
	httpWG.Wait()
	if err := engine.Shutdown(shutdownCtx); err != nil {
		logger.Warn("orchestrator shutdown incomplete", "error", err)
	}
	logger.Info("aegis control plane exited cleanly")
}
