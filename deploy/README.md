# `deploy/` — Infrastructure, Observability & Rootfs Provisioning

Infrastructure manifests and provisioning scripts for running AegisRun's backing services and minimal read-only rootfs images.

---

## Contents

- [`docker-compose.yml`](docker-compose.yml): Spins up:
  - **PostgreSQL 16 (`aegis-postgres`, `:5432`)**: `aegis` / `aegis`, DB `aegisrun`
  - **Redis 7 (`aegis-redis`, `:6379`)**: Append-only persistence enabled
  - **Prometheus (`aegis-prometheus`, `:9090`)**: Scrapes [`control-go`](http://host.docker.internal:8080) and [`daemon-rs`](http://host.docker.internal:9091) every `5s` via [`prometheus.yml`](prometheus.yml)
  - **Grafana (`aegis-grafana`, `:3000`)**: Admin password `admin`, pre-provisioned with Prometheus datasource ([`grafana/provisioning/datasources/prometheus.yml`](grafana/provisioning/datasources/prometheus.yml)) and AegisRun dashboard ([`grafana/dashboards/aegis_overview.json`](grafana/dashboards/aegis_overview.json))
- [`scripts/build-rootfs.sh`](scripts/build-rootfs.sh): Downloads Alpine `3.20.3` minirootfs (`x86_64`), provisions unprivileged `sandbox` (`UID/GID 10000`) and minimal packages for `python3.12` (`python3`, `py3-numpy`, `py3-pandas`, `py3-pydantic`), `node20` (`nodejs`), and `bash5` (`bash`), and locks `/var/lib/aegis/rootfs/*` as read-only (`chmod -R a-w`).
- [`scripts/quickstart.sh`](scripts/quickstart.sh): Single-command Linux / WSL2 launcher that ensures Redis/Valkey (`:6379`) and PostgreSQL (`:5432`, schema `0001_init_executions.up.sql`) are up, provisions rootfs if missing, builds & launches `daemon-rs` and `control-go` in the background, and waits for `http://127.0.0.1:8080/readyz` readiness (`--stop` shuts down services and checks mounts/cgroups).

---

## Usage

```bash
# Single-command end-to-end backend launch on Linux / WSL2:
sudo bash deploy/scripts/quickstart.sh
sudo bash deploy/scripts/quickstart.sh --stop

# Start Postgres, Redis, Prometheus, and Grafana:
make up
# or:
docker compose -f deploy/docker-compose.yml up -d

# Stop stack:
make down

# Provision read-only base rootfs images on Linux / WSL2:
make rootfs
# Rebuild unconditionally:
sudo ./deploy/scripts/build-rootfs.sh --force
```
