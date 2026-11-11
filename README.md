# Google Cloud ADK 2.2 SRE & BigQuery Capacity Control Governor

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Google ADK](https://img.shields.io/badge/Google_ADK-2.2.0_Workflow_DAG-4285F4)](https://adk.dev/workflows/index.md)
[![Gemini Models](https://img.shields.io/badge/Vertex_AI-Gemini_3.8_Flash_%26_Gemini_4-0F9D58)](https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/model-versions)

Reference production implementation accompanying the Google Cloud Community mathematical & distributed-systems article:
**"The control theory of autonomous Google Cloud agents: Lyapunov stability, ADK 2.2 workflow DAGs, Kingman queueing, and Cloud Spanner TrueTime"** by **Aniket Agrawal**.

![Closed-Loop Google Cloud SRE & FinOps Control Plane Architecture](docs/figures/gcp_sre_fig1_hero_control_plane.gif)

## Architecture Overview

This repository demonstrates how to replace unconstrained ReAct loops with a deterministic, mathematically bounded control plane using **Google Agent Development Kit (`google-adk >= 2.2.0`) Graph `Workflow`s**:

1. **ADK 2.2 Diamond Fan-Out / Fan-In Graph (`app/agent.py`):**
   - `START -> telemetry_dispatcher` normalizes multi-region incident webhooks (`us-central1` + `europe-west4`).
   - Four concurrent telemetry nodes (`cloud_monitoring_slo_probe`, `bigquery_slot_forensics_probe`, `cloud_trace_span_probe`, `spanner_lease_status_probe`) execute in parallel and synchronize at `JoinNode(name="telemetry_join")`.
   - `sre_root_cause_agent` (`LlmAgent` on `gemini-3.8-flash` / `gemini-4-argon` / `gemini-4-carbon` via `location="global"`) computes exact Kingman heavy-traffic wait reductions and emits a strict Pydantic `SRESynthesisDecision` schema.
   - `route_remediation_gate` emits a typed `Event(route=...)` dispatching either to `execute_spanner_truetime_saga` or `hitl_sre_escalation`.
2. **Deterministic Lifecycle Callbacks (`app/control_math.py` & `app/agent.py`):**
   - `before_tool_callback` (`lyapunov_sha256_loop_breaker`): Enforces discrete-time Lyapunov drift negativity via an $\mathcal{O}(1)$ 64-bit SHA-256 sliding window ($W = 6, k = 3$, birthday collision bound $\le 8.13 \times 10^{-19}$).
   - `after_tool_callback` (`cloud_sdp_telemetry_redactor`): Enforces zero mutual information $I(S_{\text{cred}}; X_{\text{LLM}}) = 0$ aligned with Google Cloud Sensitive Data Protection (SDP) before Gemini prompt ingestion.
3. **Cloud Spanner TrueTime Linearizable Saga (`app/spanner_saga.py`):**
   - Enforces mandatory $2\varepsilon = 7.0\text{ ms}$ TrueTime commit-wait before mutating Cloud Run traffic splits (`25% -> 0%` canary drain) and BigQuery Enterprise slot reservations (`2,000 -> 3,200` slots, cutting Kingman stage wait by `99.01%` from `11,828.60 ms` to `117.11 ms`).
   - Automatically executes reverse-order groupoid compensation upon downstream `HTTP 503` faults with zero orphaned locks.

## Quickstart & Verification

```bash
# 1. Install dependencies with uv
uv sync --dev

# 2. Run deterministic unit & invariant tests
uv run pytest -v

# 3. Run live Vertex AI (location="global") ADK 2.2 Workflow + Gemini 3.8 Flash / Gemini 4 verification
export GOOGLE_CLOUD_PROJECT="your-gcp-project-id"
export GOOGLE_CLOUD_LOCATION="global"
export GOOGLE_GENAI_USE_VERTEXAI="True"
uv run python scripts/live_gcp_verify.py
```

## Live Verified Benchmark Summary (Vertex AI `global`, `google-adk==2.2.0`)

| Model / Runtime | Endpoint Location | Status | Wall Latency | Total Tokens | Structured Action Parity |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **ADK 2.2 Full 8-Node `Workflow` (`gemini-3.8-flash`)** | `global` | `COMMITTED` (12 events) | `11,355.64 ms` | `4,573` (2 turns) | `drain_canary=True`, `+1,200` BQ slots |
| **`gemini-3.8-flash` (GA)** | `global` | `HTTP 200 OK` | `8,062.97 ms` | `1,569` | `100%` (`rho: 0.994 -> 0.62125`) |
| **`gemini-4-argon` (GA)** | `global` | `HTTP 200 OK` | `7,176.87 ms` | `1,216` | `100%` (`rho: 0.994 -> 0.62125`) |
| **`gemini-4-carbon`** | `global` | `HTTP 200 OK` | `53,019.60 ms` | `1,742` | `100%` (`rho: 0.994 -> 0.62125`) |
