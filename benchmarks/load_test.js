import http from "k6/http";
import { check, sleep } from "k6";
import { Rate, Trend } from "k6/metrics";

const BASE_URL = __ENV.AEGIS_CONTROL_URL || "http://localhost:8080";
const API_KEY = __ENV.AEGIS_API_KEY || "aegis-dev-token";

const http5xxRate = new Rate("http_5xx_errors");
const idempReplayDurationMs = new Trend("idempotent_replay_duration_ms", true);
const uniqueExecDurationMs = new Trend("unique_exec_duration_ms", true);

export const options = {
  scenarios: {
    agents_200: {
      executor: "constant-vus",
      vus: Number(__ENV.K6_VUS || 200),
      duration: __ENV.K6_DURATION || "60s",
    },
  },
  thresholds: {
    http_5xx_errors: ["rate==0"],
    idempotent_replay_duration_ms: ["p(95)<3"],
  },
};

function postExecute(payload) {
  return http.post(`${BASE_URL}/v1/execute`, JSON.stringify(payload), {
    headers: {
      Authorization: `Bearer ${API_KEY}`,
      "Content-Type": "application/json",
    },
    timeout: "30s",
  });
}

export function setup() {
  for (let bucket = 0; bucket < 4; bucket++) {
    const res = postExecute({
      agent_session_id: `k6-idemp-shared-${bucket}`,
      tool_name: "k6_idempotent_tool",
      runtime: "python3.12",
      entrypoint_code: "print('cached-ok')",
      args: { bucket },
      mode: "sync",
    });
    if (res.status !== 200 || res.json("status") !== "COMPLETED") {
      throw new Error(`setup warmup failed for bucket ${bucket}: status=${res.status} body=${res.body}`);
    }
  }
}

export default function () {
  if (__ITER === 0) {
    sleep(((__VU - 1) % 100) * 0.01);
  }
  const roll = Math.random();
  const headersOk = {
    "no 5xx": (r) => r.status < 500,
  };

  if (roll < 0.7) {
    // 70%: Unique compute payloads
    const res = postExecute({
      agent_session_id: `k6-uniq-${__VU}-${__ITER}-${Date.now()}`,
      tool_name: "k6_compute",
      runtime: "python3.12",
      entrypoint_code: "import math; print(math.factorial(15))",
      args: { vu: __VU, iter: __ITER },
      mode: "sync",
    });
    http5xxRate.add(res.status >= 500);
    uniqueExecDurationMs.add(res.timings.duration);
    check(res, {
      ...headersOk,
      "unique status 200": (r) => r.status === 200,
      "unique COMPLETED": (r) => r.json("status") === "COMPLETED",
    });
  } else if (roll < 0.9) {
    // 20%: Fixed deterministic payload hitting Redis idempotency cache across VUs
    const bucket = __VU % 4;
    const res = postExecute({
      agent_session_id: `k6-idemp-shared-${bucket}`,
      tool_name: "k6_idempotent_tool",
      runtime: "python3.12",
      entrypoint_code: "print('cached-ok')",
      args: { bucket },
      mode: "sync",
    });
    http5xxRate.add(res.status >= 500);
    const replayHeader = res.headers["X-Aegis-Idempotent-Replay"];
    if (replayHeader === "true") {
      idempReplayDurationMs.add(res.timings.duration);
    }
    check(res, {
      ...headersOk,
      "idemp status 200": (r) => r.status === 200,
      "idemp COMPLETED": (r) => r.json("status") === "COMPLETED",
    });
  } else {
    // 10%: Resource-violating payloads (OOM / timeout containment check)
    const res = postExecute({
      agent_session_id: `k6-abuse-${__VU}-${__ITER}-${Date.now()}`,
      tool_name: "k6_oom_probe",
      runtime: "python3.12",
      entrypoint_code: "x = bytearray(b'A' * (128 * 1024 * 1024))",
      args: { iter: __ITER },
      limits: {
        wall_timeout_ms: 2000,
        memory_max_bytes: 33554432,
        pids_max: 16,
      },
      mode: "sync",
    });
    http5xxRate.add(res.status >= 500);
    check(res, {
      ...headersOk,
      "abuse status 200": (r) => r.status === 200,
      "abuse OOM_KILLED": (r) => r.json("status") === "OOM_KILLED",
    });
  }
  sleep(0.8 + Math.random() * 0.4);
}
