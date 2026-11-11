#!/usr/bin/env python3
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Live GCP Vertex AI + ADK 2.2 Graph Workflow verification & benchmark runner."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
from pathlib import Path

import google.auth
import google.oauth2.credentials
from google import genai
from google.adk.runners import InMemoryRunner
from google.genai import types

from app.agent import (
    SRESynthesisDecision,
    app,
    query_bigquery_slot_wait_projection,
    sre_root_cause_agent,
)
from app.control_math import (
    birthday_collision_upper_bound,
    expected_absorbed_retries,
    fork_join_exponential_expectation_ms,
    kingman_vut_wait_ms,
    kv_cache_bytes_per_token,
)
from app.spanner_saga import CloudSpannerTrueTimeSaga

_, project_id = google.auth.default()
os.environ.setdefault("GOOGLE_CLOUD_PROJECT", project_id or "your-gcp-project-id")
os.environ.setdefault("GOOGLE_CLOUD_LOCATION", "global")
os.environ.setdefault("GOOGLE_GENAI_USE_VERTEXAI", "True")


def _get_vertex_client() -> genai.Client:
    token = subprocess.check_output(
        ["gcloud", "auth", "application-default", "print-access-token"],
        text=True,
    ).strip()
    creds = google.oauth2.credentials.Credentials(token=token)
    return genai.Client(
        vertexai=True,
        project=os.environ["GOOGLE_CLOUD_PROJECT"],
        location="global",
        credentials=creds,
    )

SRE_DIAGNOSTIC_PROMPT = (
    "Production Google Cloud incident INC-2026-1111-0342 at 03:42 UTC across us-central1 and europe-west4:\n"
    "- Cloud Run service `checkout-edge`: stable revision `checkout-edge-00047-stable` (75% weight, p99=138 ms) vs. "
    "canary revision `checkout-edge-00048-v9x` (25% weight, p99=1940 ms, 1h error-budget burn=16.2x > 14.4x SLO).\n"
    "- BigQuery Enterprise reservation `prod-bi-reservation-us`: 2000 baseline slots, 1988 active consumed slots "
    "(utilization rho=0.994, shuffle_spill_gb=54.8 GB, mean stage service=42.0 ms) caused by query fingerprint "
    "`bq_fp_77c19e02a4b8` from `checkout-edge-00048-v9x`.\n"
    "- Cloud Spanner TrueTime instance `sre-control-plane-nam-eur`: epsilon=3.5 ms (2*epsilon commit-wait=7.0 ms), "
    "active_conflicting_leases=0.\n"
    "Compute the post-mitigation utilization rho if +1200 BigQuery burst slots are added (1988 / 3200 = 0.62125) "
    "and output a strict JSON object matching SRESynthesisDecision."
)


async def run_live_adk_workflow() -> dict:
    sre_root_cause_agent.model.__dict__["api_client"] = _get_vertex_client()
    runner = InMemoryRunner(app=app)
    session = await runner.session_service.create_session(
        app_name="gcp_sre_control_governor",
        user_id="sre_verifier",
    )
    t0 = time.perf_counter()
    events_log = []
    final_output = None

    async for event in runner.run_async(
        user_id="sre_verifier",
        session_id=session.id,
        new_message=types.Content(
            role="user",
            parts=[
                types.Part.from_text(
                    text=(
                        "Execute full ADK 2.2 SRE telemetry fan-out, BigQuery Kingman projection, and "
                        "Cloud Spanner TrueTime remediation saga for INC-2026-1111-0342."
                    )
                )
            ],
        ),
    ):
        elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
        author = getattr(event, "author", None) or "workflow"
        out = getattr(event, "output", None)
        route = getattr(event.actions, "route", None) if getattr(event, "actions", None) else None
        usage = getattr(event, "usage_metadata", None)
        usage_dict = None
        if usage is not None:
            usage_dict = {
                "prompt_token_count": getattr(usage, "prompt_token_count", None),
                "candidates_token_count": getattr(usage, "candidates_token_count", None),
                "thoughts_token_count": getattr(usage, "thoughts_token_count", None),
                "total_token_count": getattr(usage, "total_token_count", None),
                "cached_content_token_count": getattr(usage, "cached_content_token_count", None),
            }
        events_log.append(
            {
                "elapsed_ms": elapsed_ms,
                "author": author,
                "route": route,
                "has_output": out is not None,
                "usage_metadata": usage_dict,
            }
        )
        if isinstance(out, dict) and "spanner_saga" in out:
            final_output = out

    total_ms = round((time.perf_counter() - t0) * 1000.0, 2)
    return {
        "workflow_name": "gcp_sre_control_governor",
        "masked_project": "gcp-sre-governor-prod",
        "location": "global",
        "total_wall_ms": total_ms,
        "event_count": len(events_log),
        "events": events_log,
        "final_output": final_output,
    }


def benchmark_live_gemini_models() -> list[dict]:
    client = _get_vertex_client()
    models = ["gemini-3.8-flash", "gemini-4-argon", "gemini-4-carbon"]
    results = []
    for model_id in models:
        t0 = time.perf_counter()
        try:
            resp = client.models.generate_content(
                model=model_id,
                contents=SRE_DIAGNOSTIC_PROMPT,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=SRESynthesisDecision,
                    temperature=0.0,
                ),
            )
            dt_ms = round((time.perf_counter() - t0) * 1000.0, 2)
            usage = resp.usage_metadata
            parsed = json.loads(resp.text)
            results.append(
                {
                    "model": model_id,
                    "endpoint_location": "global",
                    "status": "LIVE_VERIFIED_200_OK",
                    "latency_ms": dt_ms,
                    "prompt_tokens": getattr(usage, "prompt_token_count", None),
                    "candidate_tokens": getattr(usage, "candidates_token_count", None),
                    "thoughts_tokens": getattr(usage, "thoughts_token_count", None),
                    "total_tokens": getattr(usage, "total_token_count", None),
                    "parsed_decision": parsed,
                }
            )
        except Exception as exc:
            dt_ms = round((time.perf_counter() - t0) * 1000.0, 2)
            results.append(
                {
                    "model": model_id,
                    "endpoint_location": "global",
                    "status": f"ERROR: {type(exc).__name__}: {exc}",
                    "latency_ms": dt_ms,
                }
            )
    return results


async def main() -> None:
    wf_result = await run_live_adk_workflow()
    model_results = benchmark_live_gemini_models()

    # Exact closed-form analytical physics ledger
    kingman = query_bigquery_slot_wait_projection(1988, 2000, 1200, 42.0)
    saga_503 = CloudSpannerTrueTimeSaga(epsilon_ms=3.5).execute_remediation_saga(
        incident_id="INC-2026-1111-0342-CHAOS",
        burst_slots=1200,
        inject_downstream_503=True,
    )
    physics = {
        "kingman_vut": kingman,
        "birthday_collision_w6_b64": birthday_collision_upper_bound(6, 64),
        "expected_retries_q070_k3": expected_absorbed_retries(0.70, 3),
        "fork_join_mean_ms": round(fork_join_exponential_expectation_ms([480.0, 620.0, 540.0, 390.0]), 2),
        "gqa_kv_bytes_per_token": kv_cache_bytes_per_token(64, 8, 128, 2),
        "spanner_503_rollback_test": saga_503,
    }

    summary = {
        "adk_workflow_live_run": wf_result,
        "gemini_global_endpoint_parity": model_results,
        "analytical_physics": physics,
    }
    out_path = Path(os.environ.get("OUTPUT_PATH", "live_gcp_sre_verified_results.json"))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Saved live GCP verification results to {out_path}")
    print("ADK Workflow total_wall_ms:", wf_result["total_wall_ms"], "events:", wf_result["event_count"])
    print("ADK Workflow outcome:", wf_result["final_output"]["spanner_saga"]["outcome"] if wf_result["final_output"] else None)
    for m in model_results:
        print("Model:", m["model"], "status:", m["status"], "latency_ms:", m["latency_ms"], "tokens:", m.get("total_tokens"))


if __name__ == "__main__":
    asyncio.run(main())
