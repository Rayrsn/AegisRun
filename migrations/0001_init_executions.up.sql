CREATE TABLE IF NOT EXISTS executions (
    id UUID PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    agent_session_id VARCHAR(128) NOT NULL,
    tool_name VARCHAR(128) NOT NULL,
    idempotency_key CHAR(64) NOT NULL UNIQUE,
    runtime VARCHAR(32) NOT NULL,
    code_sha256 CHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL CHECK (
        status IN (
            'QUEUED',
            'AWAITING_APPROVAL',
            'REJECTED',
            'RUNNING',
            'COMPLETED',
            'OOM_KILLED',
            'TIMEOUT_KILLED',
            'SYSCALL_VIOLATION',
            'FAILED'
        )
    ),
    requires_approval BOOLEAN NOT NULL DEFAULT FALSE,
    approved_by VARCHAR(128) NULL,
    worker_node_id VARCHAR(64) NULL,
    exit_code INT NULL,
    setup_duration_us BIGINT NULL,
    exec_duration_us BIGINT NULL,
    peak_memory_bytes BIGINT NULL,
    stdout_tail BYTEA NULL,
    stderr_tail BYTEA NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_executions_session
    ON executions (tenant_id, agent_session_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_executions_status
    ON executions (status)
    WHERE status IN ('QUEUED', 'AWAITING_APPROVAL', 'RUNNING');

CREATE TABLE IF NOT EXISTS hitl_approvals (
    execution_id UUID PRIMARY KEY REFERENCES executions(id) ON DELETE CASCADE,
    approval_token_hash CHAR(64) NOT NULL UNIQUE,
    webhook_url TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    decision VARCHAR(16) NULL CHECK (decision IS NULL OR decision IN ('APPROVED', 'REJECTED')),
    decided_at TIMESTAMPTZ NULL
);

