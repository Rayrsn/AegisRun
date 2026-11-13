package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MaxTailBytes bounds stdout_tail and stderr_tail persisted in PostgreSQL to 64 KiB.
const MaxTailBytes = 64 * 1024

var (
	ErrExecutionNotFound      = errors.New("execution not found")
	ErrApprovalNotFound       = errors.New("approval token not found")
	ErrApprovalAlreadyDecided = errors.New("approval already decided")
	ErrApprovalExpired        = errors.New("approval token expired")
)

// ExecutionRecord mirrors a row in the PostgreSQL `executions` table.
type ExecutionRecord struct {
	ID               string
	TenantID         string
	AgentSessionID   string
	ToolName         string
	IdempotencyKey   string
	Runtime          string
	CodeSHA256       string
	Status           string
	RequiresApproval bool
	ApprovedBy       *string
	WorkerNodeID     *string
	ExitCode         *int32
	SetupDurationUs  *int64
	ExecDurationUs   *int64
	PeakMemoryBytes  *int64
	StdoutTail       []byte
	StderrTail       []byte
	CreatedAt        time.Time
	CompletedAt      *time.Time
}

// ExecutionResultUpdate holds terminal execution fields persisted on completion.
type ExecutionResultUpdate struct {
	ID              string
	Status          string
	WorkerNodeID    string
	ExitCode        int32
	SetupDurationUs int64
	ExecDurationUs  int64
	PeakMemoryBytes int64
	StdoutTail      []byte
	StderrTail      []byte
}

// ApprovalRecord mirrors a row in `hitl_approvals` joined with approval metadata.
type ApprovalRecord struct {
	ExecutionID       string
	ApprovalTokenHash string
	WebhookURL        string
	ExpiresAt         time.Time
	Decision          *string
	DecidedAt         *time.Time
}

// Store defines the persistence contract required by the orchestrator and HITL handlers.
type Store interface {
	CreateExecution(ctx context.Context, rec *ExecutionRecord) error
	GetExecution(ctx context.Context, executionID string) (*ExecutionRecord, error)
	UpdateExecutionRunning(ctx context.Context, executionID, workerNodeID string) error
	CompleteExecution(ctx context.Context, upd *ExecutionResultUpdate) error
	CreateApproval(ctx context.Context, rec *ApprovalRecord) error
	GetApprovalByTokenHash(ctx context.Context, tokenHash string) (*ApprovalRecord, error)
	DecideApproval(ctx context.Context, tokenHash, decision, decidedBy string) (*ApprovalRecord, error)
	ReapExpiredApprovals(ctx context.Context) ([]string, error)
	Close()
}

// PostgresStore implements Store backed by a pgxpool.Pool.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore creates a validated pgx connection pool against PostgreSQL.
func NewPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing postgres DSN: %w", err)
	}
	cfg.MaxConns = 32
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging postgres: %w", err)
	}
	return &PostgresStore{pool: pool}, nil
}

// Close closes all connections in the pool.
func (s *PostgresStore) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

// Pool exposes the underlying pgxpool for health checks or tests.
func (s *PostgresStore) Pool() *pgxpool.Pool {
	return s.pool
}

// CapTail truncates data from the head so only the trailing max bytes are kept.
func CapTail(data []byte, max int) []byte {
	if max <= 0 || len(data) <= max {
		return data
	}
	return data[len(data)-max:]
}

// CreateExecution inserts a new execution audit row.
func (s *PostgresStore) CreateExecution(ctx context.Context, rec *ExecutionRecord) error {
	const q = `
		INSERT INTO executions (
			id, tenant_id, agent_session_id, tool_name, idempotency_key,
			runtime, code_sha256, status, requires_approval
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`
	_, err := s.pool.Exec(ctx, q,
		rec.ID,
		rec.TenantID,
		rec.AgentSessionID,
		rec.ToolName,
		rec.IdempotencyKey,
		rec.Runtime,
		rec.CodeSHA256,
		rec.Status,
		rec.RequiresApproval,
	)
	if err != nil {
		return fmt.Errorf("inserting execution %s: %w", rec.ID, err)
	}
	return nil
}

// GetExecution fetches an execution record by UUID.
func (s *PostgresStore) GetExecution(ctx context.Context, executionID string) (*ExecutionRecord, error) {
	const q = `
		SELECT
			id::text, tenant_id, agent_session_id, tool_name, idempotency_key,
			runtime, code_sha256, status, requires_approval, approved_by,
			worker_node_id, exit_code, setup_duration_us, exec_duration_us,
			peak_memory_bytes, stdout_tail, stderr_tail, created_at, completed_at
		FROM executions
		WHERE id = $1
	`
	var rec ExecutionRecord
	err := s.pool.QueryRow(ctx, q, executionID).Scan(
		&rec.ID,
		&rec.TenantID,
		&rec.AgentSessionID,
		&rec.ToolName,
		&rec.IdempotencyKey,
		&rec.Runtime,
		&rec.CodeSHA256,
		&rec.Status,
		&rec.RequiresApproval,
		&rec.ApprovedBy,
		&rec.WorkerNodeID,
		&rec.ExitCode,
		&rec.SetupDurationUs,
		&rec.ExecDurationUs,
		&rec.PeakMemoryBytes,
		&rec.StdoutTail,
		&rec.StderrTail,
		&rec.CreatedAt,
		&rec.CompletedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrExecutionNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("selecting execution %s: %w", executionID, err)
	}
	return &rec, nil
}

// UpdateExecutionRunning transitions an execution to RUNNING on the assigned worker node.
func (s *PostgresStore) UpdateExecutionRunning(ctx context.Context, executionID, workerNodeID string) error {
	const q = `
		UPDATE executions
		SET status = 'RUNNING', worker_node_id = $2
		WHERE id = $1
	`
	tag, err := s.pool.Exec(ctx, q, executionID, workerNodeID)
	if err != nil {
		return fmt.Errorf("marking execution %s running on %s: %w", executionID, workerNodeID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrExecutionNotFound
	}
	return nil
}

// CompleteExecution persists final telemetry metrics and 64 KiB-capped stdout/stderr tails.
func (s *PostgresStore) CompleteExecution(ctx context.Context, upd *ExecutionResultUpdate) error {
	const q = `
		UPDATE executions
		SET
			status = $2,
			worker_node_id = COALESCE(NULLIF($3, ''), worker_node_id),
			exit_code = $4,
			setup_duration_us = $5,
			exec_duration_us = $6,
			peak_memory_bytes = $7,
			stdout_tail = $8,
			stderr_tail = $9,
			completed_at = NOW()
		WHERE id = $1
	`
	stdoutTail := CapTail(upd.StdoutTail, MaxTailBytes)
	stderrTail := CapTail(upd.StderrTail, MaxTailBytes)

	tag, err := s.pool.Exec(ctx, q,
		upd.ID,
		upd.Status,
		upd.WorkerNodeID,
		upd.ExitCode,
		upd.SetupDurationUs,
		upd.ExecDurationUs,
		upd.PeakMemoryBytes,
		stdoutTail,
		stderrTail,
	)
	if err != nil {
		return fmt.Errorf("completing execution %s: %w", upd.ID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrExecutionNotFound
	}
	return nil
}

// CreateApproval inserts a pending Human-in-the-Loop approval gate row.
func (s *PostgresStore) CreateApproval(ctx context.Context, rec *ApprovalRecord) error {
	const q = `
		INSERT INTO hitl_approvals (execution_id, approval_token_hash, webhook_url, expires_at)
		VALUES ($1, $2, $3, $4)
	`
	_, err := s.pool.Exec(ctx, q, rec.ExecutionID, rec.ApprovalTokenHash, rec.WebhookURL, rec.ExpiresAt)
	if err != nil {
		return fmt.Errorf("inserting approval for execution %s: %w", rec.ExecutionID, err)
	}
	return nil
}

// GetApprovalByTokenHash looks up a HITL approval record by SHA-256 token hash.
func (s *PostgresStore) GetApprovalByTokenHash(ctx context.Context, tokenHash string) (*ApprovalRecord, error) {
	const q = `
		SELECT execution_id::text, approval_token_hash, webhook_url, expires_at, decision, decided_at
		FROM hitl_approvals
		WHERE approval_token_hash = $1
	`
	var rec ApprovalRecord
	err := s.pool.QueryRow(ctx, q, tokenHash).Scan(
		&rec.ExecutionID,
		&rec.ApprovalTokenHash,
		&rec.WebhookURL,
		&rec.ExpiresAt,
		&rec.Decision,
		&rec.DecidedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrApprovalNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("selecting approval by token hash: %w", err)
	}
	return &rec, nil
}

// DecideApproval atomically records an APPROVED or REJECTED decision and updates the parent execution.
func (s *PostgresStore) DecideApproval(ctx context.Context, tokenHash, decision, decidedBy string) (*ApprovalRecord, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("beginning approval decision transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	const lockQ = `
		SELECT execution_id::text, approval_token_hash, webhook_url, expires_at, decision, decided_at
		FROM hitl_approvals
		WHERE approval_token_hash = $1
		FOR UPDATE
	`
	var rec ApprovalRecord
	err = tx.QueryRow(ctx, lockQ, tokenHash).Scan(
		&rec.ExecutionID,
		&rec.ApprovalTokenHash,
		&rec.WebhookURL,
		&rec.ExpiresAt,
		&rec.Decision,
		&rec.DecidedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrApprovalNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("locking approval row: %w", err)
	}
	if rec.Decision != nil {
		return nil, ErrApprovalAlreadyDecided
	}
	if time.Now().UTC().After(rec.ExpiresAt.UTC()) {
		return nil, ErrApprovalExpired
	}

	const updateApprovalQ = `
		UPDATE hitl_approvals
		SET decision = $2, decided_at = NOW()
		WHERE approval_token_hash = $1
		RETURNING decision, decided_at
	`
	if err := tx.QueryRow(ctx, updateApprovalQ, tokenHash, decision).Scan(&rec.Decision, &rec.DecidedAt); err != nil {
		return nil, fmt.Errorf("updating approval decision: %w", err)
	}

	nextStatus := "REJECTED"
	if decision == "APPROVED" {
		nextStatus = "QUEUED"
	}

	const updateExecQ = `
		UPDATE executions
		SET
			status = $2::varchar,
			approved_by = $3,
			completed_at = CASE WHEN $2::text = 'REJECTED' THEN NOW() ELSE completed_at END
		WHERE id = $1
	`
	if _, err := tx.Exec(ctx, updateExecQ, rec.ExecutionID, nextStatus, decidedBy); err != nil {
		return nil, fmt.Errorf("updating execution %s after approval decision: %w", rec.ExecutionID, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing approval decision transaction: %w", err)
	}
	return &rec, nil
}

// ReapExpiredApprovals marks expired undecided approvals as REJECTED and returns their execution IDs.
func (s *PostgresStore) ReapExpiredApprovals(ctx context.Context) ([]string, error) {
	const q = `
		WITH expired AS (
			UPDATE hitl_approvals
			SET decision = 'REJECTED', decided_at = NOW()
			WHERE decision IS NULL AND expires_at < NOW()
			RETURNING execution_id
		),
		updated_execs AS (
			UPDATE executions e
			SET status = 'REJECTED', approved_by = 'system:timeout', completed_at = NOW()
			FROM expired
			WHERE e.id = expired.execution_id AND e.status = 'AWAITING_APPROVAL'
			RETURNING e.id::text
		)
		SELECT id FROM updated_execs
	`
	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("reaping expired approvals: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scanning reaped execution id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating reaped execution ids: %w", err)
	}
	return ids, nil
}
