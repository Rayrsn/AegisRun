"""Reproducible cold-start latency benchmark: AegisRun vs. Docker (`python:3.12-alpine`)."""

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SDK_DIR = _REPO_ROOT / "sdk-py"
if str(_SDK_DIR) not in sys.path:
    sys.path.insert(0, str(_SDK_DIR))

from aegis_sdk import AegisClient
from aegis_sdk.models import ResourceLimits

PAYLOAD_CODE = "import math; print(math.factorial(20))"
EXPECTED_OUTPUT = "2432902008176640000"


@dataclasses.dataclass(slots=True)
class LatencyStats:
    """Summary percentiles and memory measurements for a benchmark series."""

    name: str
    mode: str
    samples: int
    p50_ms: float
    p95_ms: float
    p99_ms: float
    mean_ms: float
    setup_p50_us: float | None = None
    setup_p95_us: float | None = None
    peak_mem_p50_mib: float | None = None


def _percentile(sorted_vals: list[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    rank = (pct / 100.0) * (len(sorted_vals) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = rank - lo
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * frac


def _summarize(
    name: str,
    mode: str,
    latencies_ms: list[float],
    setup_us: list[float] | None = None,
    peak_mem_bytes: list[float] | None = None,
) -> LatencyStats:
    s_lat = sorted(latencies_ms)
    s_setup = sorted(setup_us) if setup_us else []
    s_mem = sorted(peak_mem_bytes) if peak_mem_bytes else []
    return LatencyStats(
        name=name,
        mode=mode,
        samples=len(s_lat),
        p50_ms=_percentile(s_lat, 50.0),
        p95_ms=_percentile(s_lat, 95.0),
        p99_ms=_percentile(s_lat, 99.0),
        mean_ms=statistics.fmean(s_lat) if s_lat else 0.0,
        setup_p50_us=_percentile(s_setup, 50.0) if s_setup else None,
        setup_p95_us=_percentile(s_setup, 95.0) if s_setup else None,
        peak_mem_p50_mib=(_percentile(s_mem, 50.0) / (1024.0 * 1024.0)) if s_mem else None,
    )


def _run_one_aegis(client: AegisClient, tag: str) -> tuple[float, float, float]:
    t0 = time.perf_counter()
    res = client.execute(
        tool_name="factorial_bench",
        entrypoint_code=PAYLOAD_CODE,
        runtime="PYTHON_3_12",
        limits=ResourceLimits(wall_timeout_ms=5000, memory_max_bytes=128 * 1024 * 1024, pids_max=16),
        session_id=f"bench-{tag}-{time.time_ns()}",
    )
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    if EXPECTED_OUTPUT not in res.stdout:
        raise RuntimeError(f"Unexpected AegisRun output: {res.stdout!r}")
    return (
        elapsed_ms,
        float(res.metrics.setup_duration_us),
        float(res.metrics.peak_memory_bytes),
    )


def _run_one_docker(image: str) -> float:
    t0 = time.perf_counter()
    proc = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--memory",
            "128m",
            "--pids-limit",
            "16",
            image,
            "python",
            "-c",
            PAYLOAD_CODE,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    if EXPECTED_OUTPUT not in proc.stdout:
        raise RuntimeError(f"Unexpected Docker output: {proc.stdout!r}")
    return elapsed_ms


def bench_aegis(
    url: str,
    api_key: str,
    seq_iters: int,
    conc_iters: int,
    concurrency: int,
) -> tuple[LatencyStats, LatencyStats]:
    """Run sequential and concurrent AegisRun cold-start benchmarks."""
    limits_ok = 3
    with AegisClient(base_url=url, api_key=api_key, timeout=30.0) as warmup_client:
        for i in range(limits_ok):
            _run_one_aegis(warmup_client, f"warmup-{i}")

    seq_lat: list[float] = []
    seq_setup: list[float] = []
    seq_mem: list[float] = []
    with AegisClient(base_url=url, api_key=api_key, timeout=30.0) as client:
        for i in range(seq_iters):
            lat, setup_us, peak_b = _run_one_aegis(client, f"seq-{i}")
            seq_lat.append(lat)
            seq_setup.append(setup_us)
            seq_mem.append(peak_b)

    conc_lat: list[float] = []
    conc_setup: list[float] = []
    conc_mem: list[float] = []
    with (
        AegisClient(base_url=url, api_key=api_key, timeout=30.0) as shared_client,
        concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool,
    ):
        futs = [
            pool.submit(_run_one_aegis, shared_client, f"conc-{i}")
            for i in range(conc_iters)
        ]
        for fut in concurrent.futures.as_completed(futs):
            lat, setup_us, peak_b = fut.result()
            conc_lat.append(lat)
            conc_setup.append(setup_us)
            conc_mem.append(peak_b)

    return (
        _summarize("AegisRun", "Sequential", seq_lat, seq_setup, seq_mem),
        _summarize("AegisRun", f"Concurrent ({concurrency}w)", conc_lat, conc_setup, conc_mem),
    )


def bench_docker(image: str, seq_iters: int, conc_iters: int, concurrency: int) -> tuple[LatencyStats, LatencyStats]:
    """Ensure Docker image exists locally and run sequential + concurrent benchmarks."""
    subprocess.run(["docker", "image", "inspect", image], check=True, capture_output=True)
    _run_one_docker(image)

    seq_lat = [_run_one_docker(image) for _ in range(seq_iters)]

    conc_lat: list[float] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futs = [pool.submit(_run_one_docker, image) for _ in range(conc_iters)]
        for fut in concurrent.futures.as_completed(futs):
            conc_lat.append(fut.result())

    return (
        _summarize("Docker (runc)", "Sequential", seq_lat),
        _summarize("Docker (runc)", f"Concurrent ({concurrency}w)", conc_lat),
    )


def render_svg_chart(aegis_seq: LatencyStats, docker_seq: LatencyStats, out_path: Path) -> None:
    """Write a standalone dark-themed SVG comparison chart for P50/P95/P99 cold-start latency."""
    metrics = [
        ("P50 Latency", docker_seq.p50_ms, aegis_seq.p50_ms),
        ("P95 Latency", docker_seq.p95_ms, aegis_seq.p95_ms),
        ("P99 Latency", docker_seq.p99_ms, aegis_seq.p99_ms),
    ]
    max_val = max(max(d, a) for _, d, a in metrics) * 1.15 or 100.0
    chart_left = 155
    bar_max_w = 500
    speedup = (docker_seq.p50_ms / aegis_seq.p50_ms) if aegis_seq.p50_ms > 0 else 0.0

    rows_svg: list[str] = []
    y = 95
    for label, d_val, a_val in metrics:
        d_w = max(4.0, (d_val / max_val) * bar_max_w)
        a_w = max(4.0, (a_val / max_val) * bar_max_w)
        rows_svg.append(
            f'<text x="24" y="{y + 18}" fill="#e2e8f0" font-size="14" font-family="system-ui, sans-serif" font-weight="600">{label}</text>'
            f'<rect x="{chart_left}" y="{y}" width="{d_w:.1f}" height="22" rx="4" fill="#ef4444" fill-opacity="0.88" />'
            f'<text x="{chart_left + d_w + 8:.1f}" y="{y + 16}" fill="#fca5a5" font-size="13" font-family="monospace">{d_val:.1f} ms</text>'
            f'<rect x="{chart_left}" y="{y + 28}" width="{a_w:.1f}" height="22" rx="4" fill="#06b6d4" />'
            f'<text x="{chart_left + a_w + 8:.1f}" y="{y + 44}" fill="#67e8f9" font-size="13" font-family="monospace">{a_val:.1f} ms</text>'
        )
        y += 84

    setup_str = f"{(aegis_seq.setup_p50_us or 0.0) / 1000.0:.2f} ms"
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="760" height="370" viewBox="0 0 760 370">
  <rect width="760" height="370" rx="12" fill="#0f172a" />
  <text x="24" y="38" fill="#f8fafc" font-size="19" font-family="system-ui, sans-serif" font-weight="700">Cold-Start Latency: AegisRun vs. Docker (python:3.12-alpine)</text>
  <text x="24" y="60" fill="#94a3b8" font-size="13" font-family="system-ui, sans-serif">Payload: math.factorial(20) | Speedup (P50): {speedup:.1f}x faster | AegisRun setup P50: {setup_str}</text>
  <rect x="490" y="20" width="12" height="12" rx="2" fill="#ef4444" />
  <text x="508" y="31" fill="#cbd5e1" font-size="12" font-family="system-ui, sans-serif">Docker (n={docker_seq.samples})</text>
  <rect x="615" y="20" width="12" height="12" rx="2" fill="#06b6d4" />
  <text x="633" y="31" fill="#cbd5e1" font-size="12" font-family="system-ui, sans-serif">AegisRun (n={aegis_seq.samples})</text>
  {"".join(rows_svg)}
</svg>
"""
    out_path.write_text(svg, encoding="utf-8")


def write_markdown_report(
    stats: list[LatencyStats],
    aegis_seq: LatencyStats,
    docker_seq: LatencyStats,
    out_path: Path,
) -> None:
    """Write a GitHub-flavored Markdown report with exact measured latency percentiles."""
    speedup = (docker_seq.p50_ms / aegis_seq.p50_ms) if aegis_seq.p50_ms > 0 else 0.0
    lines = [
        "# AegisRun vs. Docker Cold-Start Benchmark",
        "",
        f"- **Payload**: `{PAYLOAD_CODE}`",
        f"- **Sequential P50 Speedup**: **{speedup:.1f}x faster** (`{aegis_seq.p50_ms:.2f} ms` vs `{docker_seq.p50_ms:.2f} ms`)",
        f"- **AegisRun Kernel Setup Overhead (P50 / P95)**: `{(aegis_seq.setup_p50_us or 0.0):.0f} us` (`{(aegis_seq.setup_p50_us or 0.0) / 1000.0:.2f} ms`) / `{(aegis_seq.setup_p95_us or 0.0):.0f} us` (`{(aegis_seq.setup_p95_us or 0.0) / 1000.0:.2f} ms`)",
        f"- **Per-Sandbox Peak Cgroup Memory (P50)**: `{(aegis_seq.peak_mem_p50_mib or 0.0):.2f} MiB`",
        "",
        "| Runtime | Mode | Samples | P50 Latency (ms) | P95 Latency (ms) | P99 Latency (ms) | Mean (ms) | Setup P50 (us) |",
        "| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for s in stats:
        setup_cell = f"{s.setup_p50_us:.0f}" if s.setup_p50_us is not None else "n/a"
        lines.append(
            f"| {s.name} | {s.mode} | {s.samples} | {s.p50_ms:.2f} | {s.p95_ms:.2f} | {s.p99_ms:.2f} | {s.mean_ms:.2f} | {setup_cell} |"
        )
    lines.append("")
    out_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    """Execute benchmark runs and write Markdown + SVG reports."""
    parser = argparse.ArgumentParser(description="Benchmark AegisRun vs Docker container cold starts.")
    parser.add_argument("--url", default=os.getenv("AEGIS_CONTROL_URL", "http://localhost:8080"))
    parser.add_argument("--api-key", default=os.getenv("AEGIS_API_KEY", "aegis-dev-token"))
    parser.add_argument("--aegis-iterations", type=int, default=500)
    parser.add_argument("--docker-iterations", type=int, default=20)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--docker-image", default="python:3.12-alpine")
    parser.add_argument(
        "--docker-latencies-json",
        default="",
        help="Optional path to pre-recorded Docker sequential/concurrent latencies JSON when running in split host/WSL2 environments.",
    )
    parser.add_argument("--out-dir", default=str(_REPO_ROOT / "benchmarks" / "results"))
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    aegis_seq, aegis_conc = bench_aegis(
        args.url,
        args.api_key,
        args.aegis_iterations,
        args.aegis_iterations,
        args.concurrency,
    )
    if args.docker_latencies_json:
        docker_data = json.loads(Path(args.docker_latencies_json).read_text(encoding="utf-8"))
        docker_seq = _summarize("Docker (runc)", "Sequential", [float(x) for x in docker_data["sequential_ms"]])
        docker_conc = _summarize(
            "Docker (runc)",
            f"Concurrent ({args.concurrency}w)",
            [float(x) for x in docker_data["concurrent_ms"]],
        )
    else:
        docker_seq, docker_conc = bench_docker(
            args.docker_image,
            args.docker_iterations,
            args.docker_iterations,
            args.concurrency,
        )

    md_path = out_dir / "vs_docker_summary.md"
    svg_path = out_dir / "cold_start_comparison.svg"
    all_stats = [aegis_seq, aegis_conc, docker_seq, docker_conc]
    write_markdown_report(all_stats, aegis_seq, docker_seq, md_path)
    render_svg_chart(aegis_seq, docker_seq, svg_path)

    print(md_path.read_text(encoding="utf-8"))
    print(f"Generated SVG chart: {svg_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
