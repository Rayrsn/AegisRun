#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DAEMON_PID_FILE="/run/aegis-daemon.pid"
CONTROL_PID_FILE="/run/aegis-control.pid"
DAEMON_LOG="/var/log/aegis-daemon.log"
CONTROL_LOG="/var/log/aegis-control.log"
CONTROL_BIN="/tmp/aegis-control"
DAEMON_BIN="${REPO_ROOT}/daemon-rs/target/release/daemon-rs"

export PATH="/usr/local/go/bin:/root/.cargo/bin:/usr/local/cargo/bin:/usr/local/bin:${PATH}"

usage() {
  cat <<'EOF'
Usage: sudo bash deploy/scripts/quickstart.sh [--stop]

Starts PostgreSQL, Redis/Valkey, the AegisRun Rust worker daemon (:50051),
and the Go control plane (:8080), then waits for /readyz readiness.

Options:
  --stop    Stop background control-go and daemon-rs processes and check leaks
  -h,--help Show this help message
EOF
}

stop_process_file() {
  local pid_file="$1"
  local label="$2"
  if [[ -f "${pid_file}" ]]; then
    local pid
    pid="$(cat "${pid_file}" 2>/dev/null || true)"
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      echo "[aegis] Stopping ${label} (pid=${pid})..."
      kill -TERM "${pid}" 2>/dev/null || true
      for _ in $(seq 1 20); do
        if ! kill -0 "${pid}" 2>/dev/null; then
          break
        fi
        sleep 0.25
      done
      if kill -0 "${pid}" 2>/dev/null; then
        kill -KILL "${pid}" 2>/dev/null || true
      fi
    fi
    rm -f "${pid_file}"
  fi
}

if [[ "${1:-}" == "-h" ]] || [[ "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${EUID}" -ne 0 ]]; then
  echo "Error: quickstart.sh must run as root (e.g. sudo bash deploy/scripts/quickstart.sh)" >&2
  exit 1
fi

if [[ "${1:-}" == "--stop" ]]; then
  stop_process_file "${CONTROL_PID_FILE}" "control-go"
  stop_process_file "${DAEMON_PID_FILE}" "daemon-rs"
  echo -n "[aegis] Leak check mounts="
  mount | grep "/run/aegis/sandboxes" | wc -l || true
  echo -n "[aegis] Leak check cgroups="
  ls -1 /sys/fs/cgroup/aegis.slice/ 2>/dev/null | grep "^exec-" | wc -l || true
  exit 0
fi

cd "${REPO_ROOT}"
mkdir -p /run /var/log

# 1. Start Redis / Valkey on 127.0.0.1:6379
redis_ping() {
  if command -v valkey-cli >/dev/null 2>&1; then
    valkey-cli -h 127.0.0.1 -p 6379 ping >/dev/null 2>&1
  elif command -v redis-cli >/dev/null 2>&1; then
    redis-cli -h 127.0.0.1 -p 6379 ping >/dev/null 2>&1
  else
    timeout 1 bash -c '</dev/tcp/127.0.0.1/6379' >/dev/null 2>&1
  fi
}

if ! redis_ping; then
  if command -v valkey-server >/dev/null 2>&1; then
    echo "[aegis] Starting native valkey-server on 127.0.0.1:6379..."
    valkey-server --bind 127.0.0.1 --port 6379 --save "" --appendonly no --daemonize yes
  elif command -v redis-server >/dev/null 2>&1; then
    echo "[aegis] Starting native redis-server on 127.0.0.1:6379..."
    redis-server --bind 127.0.0.1 --port 6379 --save "" --appendonly no --daemonize yes
  elif command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    echo "[aegis] Starting Redis via docker compose..."
    docker compose -f "${REPO_ROOT}/deploy/docker-compose.yml" up -d redis
  else
    echo "Error: neither valkey-server/redis-server nor a running Docker daemon is available." >&2
    exit 1
  fi
fi

# 2. Start PostgreSQL on 127.0.0.1:5432 and apply schema migration
postgres_ready() {
  if command -v pg_isready >/dev/null 2>&1; then
    pg_isready -h 127.0.0.1 -p 5432 >/dev/null 2>&1
  else
    timeout 1 bash -c '</dev/tcp/127.0.0.1/5432' >/dev/null 2>&1
  fi
}

if ! postgres_ready; then
  if command -v pg_ctl >/dev/null 2>&1 && id postgres >/dev/null 2>&1; then
    echo "[aegis] Starting native PostgreSQL on 127.0.0.1:5432..."
    mkdir -p /var/lib/postgres/data /run/postgresql
    chown -R postgres:postgres /var/lib/postgres /run/postgresql
    if [[ ! -s /var/lib/postgres/data/PG_VERSION ]]; then
      runuser -u postgres -- initdb -D /var/lib/postgres/data --locale=C -E UTF8
    fi
    runuser -u postgres -- pg_ctl -D /var/lib/postgres/data -l /var/lib/postgres/logfile -o "-h localhost" start
  elif command -v systemctl >/dev/null 2>&1 && systemctl list-unit-files postgresql.service >/dev/null 2>&1; then
    echo "[aegis] Starting PostgreSQL systemd service..."
    systemctl start postgresql
  elif command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    echo "[aegis] Starting PostgreSQL via docker compose..."
    docker compose -f "${REPO_ROOT}/deploy/docker-compose.yml" up -d postgres
    for _ in $(seq 1 30); do
      if postgres_ready; then
        break
      fi
      sleep 0.5
    done
  else
    echo "Error: neither native PostgreSQL nor a running Docker daemon is available." >&2
    exit 1
  fi
fi

if command -v psql >/dev/null 2>&1; then
  if id postgres >/dev/null 2>&1 && [[ -S /run/postgresql/.s.PGSQL.5432 ]]; then
    if ! runuser -u postgres -- psql -tAc "SELECT 1 FROM pg_roles WHERE rolname = 'aegis'" | grep -q 1; then
      runuser -u postgres -- psql -c "CREATE USER aegis WITH SUPERUSER PASSWORD 'aegis';"
    fi
    if ! runuser -u postgres -- psql -tAc "SELECT 1 FROM pg_database WHERE datname = 'aegisrun'" | grep -q 1; then
      runuser -u postgres -- psql -c "CREATE DATABASE aegisrun OWNER aegis;"
    fi
  fi
  if ! PGPASSWORD=aegis psql -h 127.0.0.1 -U aegis -d aegisrun -tAc "SELECT to_regclass('public.executions')" | grep -q executions; then
    echo "[aegis] Applying PostgreSQL schema migration..."
    PGPASSWORD=aegis psql -h 127.0.0.1 -U aegis -d aegisrun -f "${REPO_ROOT}/migrations/0001_init_executions.up.sql"
  fi
elif command -v docker >/dev/null 2>&1 && docker ps --format '{{.Names}}' | grep -qx 'aegis-postgres'; then
  if ! docker exec aegis-postgres psql -U aegis -d aegisrun -tAc "SELECT to_regclass('public.executions')" | grep -q executions; then
    echo "[aegis] Applying PostgreSQL schema migration via aegis-postgres container..."
    docker exec -i aegis-postgres psql -U aegis -d aegisrun < "${REPO_ROOT}/migrations/0001_init_executions.up.sql"
  fi
fi

# 3. Ensure read-only sandbox rootfs trees exist
if [[ ! -x /var/lib/aegis/rootfs/python3.12/usr/bin/python3 ]]; then
  echo "[aegis] Building base sandbox rootfs images..."
  bash "${REPO_ROOT}/deploy/scripts/build-rootfs.sh"
fi

# 4. Build daemon-rs & control-go binaries if needed
if [[ ! -x "${DAEMON_BIN}" ]]; then
  echo "[aegis] Building daemon-rs (--release)..."
  cargo build --release --manifest-path "${REPO_ROOT}/daemon-rs/Cargo.toml"
fi

echo "[aegis] Building control-go..."
(
  cd "${REPO_ROOT}/control-go"
  go build -o "${CONTROL_BIN}" ./cmd/control
)

# 5. Restart background daemon-rs and control-go cleanly
stop_process_file "${CONTROL_PID_FILE}" "control-go"
stop_process_file "${DAEMON_PID_FILE}" "daemon-rs"

echo "[aegis] Starting daemon-rs..."
RUST_LOG="${RUST_LOG:-info}" \
  WORKER_NODE_ID="${WORKER_NODE_ID:-worker-1}" \
  GRPC_LISTEN_ADDR="${GRPC_LISTEN_ADDR:-0.0.0.0:50051}" \
  METRICS_LISTEN_ADDR="${METRICS_LISTEN_ADDR:-0.0.0.0:9091}" \
  AEGIS_CGROUP_ROOT="${AEGIS_CGROUP_ROOT:-/sys/fs/cgroup}" \
  AEGIS_CGROUP_SLICE="${AEGIS_CGROUP_SLICE:-/sys/fs/cgroup/aegis.slice}" \
  AEGIS_SANDBOX_DIR="${AEGIS_SANDBOX_DIR:-/run/aegis/sandboxes}" \
  AEGIS_ROOTFS_DIR="${AEGIS_ROOTFS_DIR:-/var/lib/aegis/rootfs}" \
  AEGIS_MAX_SANDBOXES="${AEGIS_MAX_SANDBOXES:-64}" \
  "${DAEMON_BIN}" >"${DAEMON_LOG}" 2>&1 &
echo $! > "${DAEMON_PID_FILE}"

echo "[aegis] Starting control-go..."
AEGIS_HTTP_ADDR="${AEGIS_HTTP_ADDR:-0.0.0.0:8080}" \
  AEGIS_POSTGRES_DSN="${AEGIS_POSTGRES_DSN:-postgres://aegis:aegis@127.0.0.1:5432/aegisrun?sslmode=disable}" \
  AEGIS_REDIS_ADDR="${AEGIS_REDIS_ADDR:-127.0.0.1:6379}" \
  AEGIS_WORKERS="${AEGIS_WORKERS:-worker-1=127.0.0.1:50051}" \
  AEGIS_API_KEYS="${AEGIS_API_KEYS:-aegis-dev-token:default}" \
  AEGIS_RATE_LIMIT_RPS="${AEGIS_RATE_LIMIT_RPS:-200}" \
  AEGIS_RATE_LIMIT_BURST="${AEGIS_RATE_LIMIT_BURST:-100}" \
  AEGIS_MAX_ACTIVE_PER_TENANT="${AEGIS_MAX_ACTIVE_PER_TENANT:-50}" \
  "${CONTROL_BIN}" >"${CONTROL_LOG}" 2>&1 &
echo $! > "${CONTROL_PID_FILE}"

# 6. Poll /readyz until workers_healthy >= 1
echo "[aegis] Waiting for http://127.0.0.1:8080/readyz ..."
for _ in $(seq 1 40); do
  if body="$(curl -fsS http://127.0.0.1:8080/readyz 2>/dev/null)" && [[ "${body}" =~ \"workers_healthy\":[[:space:]]*[1-9] ]]; then
    echo "[aegis] Stack ready: ${body}"
    echo "[aegis] Logs: ${DAEMON_LOG}, ${CONTROL_LOG}"
    echo "[aegis] Stop with: sudo bash deploy/scripts/quickstart.sh --stop"
    exit 0
  fi
  sleep 0.5
done

echo "Error: control-go did not become ready with a healthy worker." >&2
echo "--- ${DAEMON_LOG} ---" >&2
tail -n 30 "${DAEMON_LOG}" >&2 || true
echo "--- ${CONTROL_LOG} ---" >&2
tail -n 30 "${CONTROL_LOG}" >&2 || true
exit 1
