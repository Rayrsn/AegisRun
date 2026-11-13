#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Error: build-rootfs.sh must be run as root (e.g. sudo ./deploy/scripts/build-rootfs.sh)" >&2
  exit 1
fi

FORCE=false
if [[ "${1:-}" == "--force" ]]; then
  FORCE=true
fi

ALPINE_VERSION="3.20"
ALPINE_PATCH="3.20.3"
ARCH="x86_64"
TARBALL_URL="https://dl-cdn.alpinelinux.org/alpine/v${ALPINE_VERSION}/releases/${ARCH}/alpine-minirootfs-${ALPINE_PATCH}-${ARCH}.tar.gz"

ROOTFS_BASE="/var/lib/aegis/rootfs"
CACHE_DIR="/var/cache/aegis"
TARBALL_PATH="${CACHE_DIR}/alpine-minirootfs-${ALPINE_PATCH}-${ARCH}.tar.gz"

mkdir -p "${ROOTFS_BASE}" "${CACHE_DIR}"

if [[ ! -f "${TARBALL_PATH}" ]] || [[ "${FORCE}" == "true" ]]; then
  echo "[aegis] Downloading Alpine ${ALPINE_PATCH} minirootfs..."
  curl -fsSL -o "${TARBALL_PATH}" "${TARBALL_URL}"
fi

build_runtime() {
  local runtime="$1"
  local sentinel_bin="$2"
  shift 2
  local packages=("$@")

  local target_dir="${ROOTFS_BASE}/${runtime}"
  local staging_dir="${ROOTFS_BASE}/.${runtime}.staging.$$"

  if [[ -x "${target_dir}${sentinel_bin}" ]] && [[ "${FORCE}" != "true" ]]; then
    echo "[aegis] Rootfs '${runtime}' already provisioned at ${target_dir}; enforcing read-only permissions."
    chmod -R a-w "${target_dir}"
    return 0
  fi

  echo "[aegis] Provisioning rootfs '${runtime}'..."
  rm -rf "${staging_dir}"
  mkdir -p "${staging_dir}"

  tar -xzf "${TARBALL_PATH}" -C "${staging_dir}"

  cat <<EOF > "${staging_dir}/etc/apk/repositories"
https://dl-cdn.alpinelinux.org/alpine/v${ALPINE_VERSION}/main
https://dl-cdn.alpinelinux.org/alpine/v${ALPINE_VERSION}/community
EOF

  if [[ -f /etc/resolv.conf ]]; then
    cp /etc/resolv.conf "${staging_dir}/etc/resolv.conf"
  fi

  if ! grep -q '^sandbox:' "${staging_dir}/etc/group"; then
    echo "sandbox:x:10000:" >> "${staging_dir}/etc/group"
  fi
  if ! grep -q '^sandbox:' "${staging_dir}/etc/passwd"; then
    echo "sandbox:x:10000:10000:Aegis Sandbox User:/tmp:/sbin/nologin" >> "${staging_dir}/etc/passwd"
  fi

  chroot "${staging_dir}" /sbin/apk add --no-cache "${packages[@]}"
  rm -rf "${staging_dir}/var/cache/apk/"*
  mkdir -p "${staging_dir}/tmp" "${staging_dir}/dev" "${staging_dir}/proc" "${staging_dir}/.old_root"
  chmod 1777 "${staging_dir}/tmp"

  if [[ -d "${target_dir}" ]]; then
    chmod -R u+w "${target_dir}" || true
    rm -rf "${target_dir}"
  fi

  mv "${staging_dir}" "${target_dir}"
  chmod -R a-w "${target_dir}"
  echo "[aegis] Provisioned '${runtime}' at ${target_dir} (read-only)."
}

build_runtime "python3.12" "/usr/bin/python3" python3 py3-numpy py3-pandas py3-pydantic
build_runtime "node20" "/usr/bin/node" nodejs
build_runtime "bash5" "/bin/bash" bash

if [[ ! -x "${ROOTFS_BASE}/python3.12/usr/bin/python3" ]]; then
  echo "Error: ${ROOTFS_BASE}/python3.12/usr/bin/python3 is not executable" >&2
  exit 1
fi

echo "[aegis] All rootfs images built and verified successfully."

