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

---

## Usage

```bash
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
