# AegisRun vs. Docker Cold-Start Benchmark

- **Payload**: `import math; print(math.factorial(20))`
- **Sequential P50 Speedup**: **28.0x faster** (`21.45 ms` vs `600.52 ms`)
- **AegisRun Kernel Setup Overhead (P50 / P95)**: `555 us` (`0.56 ms`) / `829 us` (`0.83 ms`)
- **Per-Sandbox Peak Cgroup Memory (P50)**: `3.24 MiB`

| Runtime | Mode | Samples | P50 Latency (ms) | P95 Latency (ms) | P99 Latency (ms) | Mean (ms) | Setup P50 (us) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| AegisRun | Sequential | 500 | 21.45 | 26.70 | 54.71 | 22.84 | 555 |
| AegisRun | Concurrent (8w) | 500 | 33.32 | 64.86 | 114.07 | 36.93 | 776 |
| Docker (runc) | Sequential | 20 | 600.52 | 724.86 | 783.77 | 616.88 | n/a |
| Docker (runc) | Concurrent (8w) | 20 | 1819.97 | 2196.43 | 2265.31 | 1808.39 | n/a |
