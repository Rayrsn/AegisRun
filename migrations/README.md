# `migrations/` — PostgreSQL Audit & HITL Schema

SQL migrations defining AegisRun's durable execution audit log and Human-in-the-Loop (HITL) approval state tables.

---

## Schema Overview (`0001_init_executions`)

### Tables
- **`executions`**: Stores every sandbox execution (`id` UUIDv7 PK, `tenant_id`, `agent_session_id`, `tool_name`, `idempotency_key` CHAR(64) UNIQUE, `runtime`, `code_sha256`, `status`, `requires_approval`, `approved_by`, `worker_node_id`, `exit_code`, `setup_duration_us`, `exec_duration_us`, `peak_memory_bytes`, `stdout_tail`, `stderr_tail`, `created_at`, `completed_at`).
  - Allowed statuses: `QUEUED`, `AWAITING_APPROVAL`, `REJECTED`, `RUNNING`, `COMPLETED`, `OOM_KILLED`, `TIMEOUT_KILLED`, `SYSCALL_VIOLATION`, `FAILED`.
  - Indexes:
    - `idx_executions_session` on `(tenant_id, agent_session_id, created_at DESC)`
    - Partial index `idx_executions_status` on `(status)` for active states (`QUEUED`, `AWAITING_APPROVAL`, `RUNNING`)
- **`hitl_approvals`**: Tracks webhook approvals (`execution_id` UUID PK referencing `executions(id) ON DELETE CASCADE`, `approval_token_hash` CHAR(64) UNIQUE, `webhook_url`, `expires_at`, `decision` in `APPROVED` | `REJECTED`, `decided_at`).

---

## Apply / Rollback

```bash
# Apply migration:
psql "postgres://aegis:aegis@localhost:5432/aegisrun?sslmode=disable" \
  -f migrations/0001_init_executions.up.sql

# Rollback migration:
psql "postgres://aegis:aegis@localhost:5432/aegisrun?sslmode=disable" \
  -f migrations/0001_init_executions.down.sql
```
