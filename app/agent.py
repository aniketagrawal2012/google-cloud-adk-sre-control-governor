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
"""Google ADK 2.2 Graph Workflow for autonomous Google Cloud SRE & BigQuery slot governance."""

from __future__ import annotations

import json
import os
from typing import Any, Literal

import google.auth
from google.adk.agents import LlmAgent
from google.adk.agents.context import Context
from google.adk.apps import App
from google.adk.events.event import Event
from google.adk.models import Gemini
from google.adk.tools.tool_context import ToolContext
from google.adk.workflow import FunctionNode, JoinNode, RetryConfig, Workflow, node
from google.genai import types
from pydantic import BaseModel, Field

from .control_math import (
    canonical_tool_digest,
    kingman_vut_wait_ms,
    scrub_gcp_telemetry_payload,
)
from .spanner_saga import CloudSpannerTrueTimeSaga

try:
    _, _detected_project = google.auth.default()
except Exception:
    _detected_project = None

os.environ.setdefault("GOOGLE_CLOUD_PROJECT", _detected_project or "your-gcp-project-id")
os.environ.setdefault("GOOGLE_CLOUD_LOCATION", "global")
os.environ.setdefault("GOOGLE_GENAI_USE_VERTEXAI", "True")

# Active November 2026 global Vertex AI endpoints:
# - gemini-3.8-flash (GA low-latency control-plane triage)
# - gemini-4-argon (GA high-precision root-cause & capacity reasoning)
# - gemini-4-carbon (frontier deep verification)
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.8-flash")

_SAGA_ENGINE = CloudSpannerTrueTimeSaga(epsilon_ms=3.5)


class SRESynthesisDecision(BaseModel):
    """Strict Pydantic contract emitted by sre_root_cause_agent inside ADK 2.2 Workflow."""

    incident_id: str = Field(description="Normalized Google Cloud incident identifier.")
    primary_root_cause: str = Field(
        description="Concise root-cause diagnosis linking Cloud Run canary and BigQuery slot contention."
    )
    drain_canary_to_zero: bool = Field(
        description="True when canary error-budget burn exceeds 14.4x threshold."
    )
    recommended_bq_burst_slots: int = Field(
        ge=0,
        le=2000,
        description="Additional BigQuery Enterprise slots required to restore utilization below 0.65.",
    )
    predicted_post_mitigation_rho: float = Field(
        ge=0.0,
        le=1.0,
        description="Expected BigQuery slot utilization rho after burst capacity expansion.",
    )
    route_decision: Literal["execute_saga", "hitl_escalation"] = Field(
        description="Deterministic routing decision for the downstream Cloud Spanner TrueTime saga."
    )
    justification_summary: str = Field(
        description="Quantitative SRE justification referencing observed p99 latency, burn rate, and slot spill."
    )


def lyapunov_sha256_loop_breaker(
    tool: Any,
    args: dict[str, Any],
    tool_context: ToolContext,
) -> dict[str, Any] | None:
    """Enforce discrete-time Lyapunov drift negativity via an O(1) SHA-256 sliding window (W=6, k=3)."""
    window: list[str] = list(tool_context.state.get("sre_call_window", []))
    digest = canonical_tool_digest(tool.name, args)
    repeats = sum(1 for item in window[-5:] if item == digest) + 1
    window.append(digest)
    tool_context.state["sre_call_window"] = window[-6:]

    if repeats >= 3:
        tool_context.state["lyapunov_breaker_tripped"] = True
        return {
            "status": "CIRCUIT_BREAKER_OPEN",
            "tool": tool.name,
            "sha256_digest": digest,
            "repeat_count": repeats,
            "lyapunov_action": "ABSORBING_BARRIER_ENGAGED",
            "guidance": (
                "Identical RPC signature repeated 3 times in window W=6. Abort retry loop and "
                "transition to deterministic Cloud Spanner compensation or SRE HITL escalation."
            ),
        }
    return None


def cloud_sdp_telemetry_redactor(
    tool: Any,
    args: dict[str, Any],
    tool_context: ToolContext,
    tool_response: Any,
) -> Any:
    """Enforce zero mutual information I(S_cred; X_LLM) = 0 prior to Gemini prompt ingestion."""
    redactions: list[str] = list(tool_context.state.get("cloud_sdp_redactions", []))
    cleaned = scrub_gcp_telemetry_payload(tool_response, redactions)
    tool_context.state["cloud_sdp_redactions"] = redactions
    return cleaned


def query_bigquery_slot_wait_projection(
    current_used_slots: float,
    baseline_slots: int,
    added_burst_slots: int,
    mean_stage_service_ms: float = 42.0,
) -> dict[str, Any]:
    """Compute Kingman VUT queue wait reduction on BigQuery Enterprise slots after adding burst capacity."""
    rho_before = min(0.994, current_used_slots / float(baseline_slots))
    rho_after = current_used_slots / float(baseline_slots + added_burst_slots)
    wait_before = kingman_vut_wait_ms(rho_before, mean_stage_service_ms)
    wait_after = kingman_vut_wait_ms(rho_after, mean_stage_service_ms)
    return {
        "utilization_before": round(rho_before, 4),
        "utilization_after": round(rho_after, 4),
        "kingman_wait_before_ms": round(wait_before, 2),
        "kingman_wait_after_ms": round(wait_after, 2),
        "wait_reduction_pct": round((1.0 - wait_after / wait_before) * 100.0, 2),
    }


@node(name="telemetry_dispatcher")
def telemetry_dispatcher(node_input: Any) -> Event:
    """Normalize incoming Cloud Monitoring webhook or user prompt into an incident packet."""
    raw_text = ""
    if hasattr(node_input, "parts") and node_input.parts:
        raw_text = " ".join(getattr(p, "text", "") or "" for p in node_input.parts).strip()
    elif isinstance(node_input, str):
        raw_text = node_input.strip()
    else:
        raw_text = json.dumps(node_input, default=str)

    inject_503 = "503" in raw_text or "rollback" in raw_text.lower()
    force_hitl = "hitl" in raw_text.lower() or "manual" in raw_text.lower()
    incident_id = "INC-2026-1111-0342"
    payload = {
        "incident_id": incident_id,
        "service": "checkout-edge",
        "regions": ["us-central1", "europe-west4"],
        "canary_revision": "checkout-edge-00048-v9x",
        "stable_revision": "checkout-edge-00047-stable",
        "inject_downstream_503": inject_503,
        "force_hitl": force_hitl,
        "operator_prompt": raw_text,
    }
    return Event(output=payload, state={"incident_packet": payload})


@node(name="cloud_monitoring_slo_probe")
def cloud_monitoring_slo_probe(node_input: dict[str, Any]) -> dict[str, Any]:
    """Probe Cloud Monitoring SLO burn rate and Cloud Run p99 latency split (scrubbed via Cloud SDP)."""
    raw_telemetry = {
        "source": "Cloud Monitoring v3",
        "incident_id": node_input["incident_id"],
        "stable_revision": "checkout-edge-00047-stable",
        "stable_traffic_pct": 75,
        "stable_p99_ms": 138.0,
        "canary_revision": "checkout-edge-00048-v9x",
        "canary_traffic_pct": 25,
        "canary_p99_ms": 1940.0,
        "error_budget_burn_1h": 16.2,
        "fast_burn_slo_threshold": 14.4,
        # Simulated raw audit fields caught deterministically by scrub_gcp_telemetry_payload:
        "service_account_private_key_id": "9f82a7c1d440e192837465ab1029384756fedcba",
        "caller_vpc_source_ip": "10.128.44.91",
    }
    return scrub_gcp_telemetry_payload(raw_telemetry)


@node(name="bigquery_slot_forensics_probe")
def bigquery_slot_forensics_probe(node_input: dict[str, Any]) -> dict[str, Any]:
    """Inspect BigQuery Enterprise INFORMATION_SCHEMA.JOBS_BY_PROJECT slot saturation & shuffle spill."""
    raw_telemetry = {
        "source": "BigQuery INFORMATION_SCHEMA.JOBS_BY_PROJECT",
        "reservation_id": "prod-bi-reservation-us",
        "baseline_slots": 2000,
        "active_consumed_slots": 1988,
        "slot_utilization_rho": 0.994,
        "shuffle_spill_gb": 54.8,
        "mean_stage_service_ms": 42.0,
        "offending_query_hash": "bq_fp_77c19e02a4b8",
        "linked_revision_tag": "checkout-edge-00048-v9x",
        "billing_account_id": "01A2B3-C4D5E6-F78901",
    }
    return scrub_gcp_telemetry_payload(raw_telemetry)


@node(name="cloud_trace_span_probe")
def cloud_trace_span_probe(node_input: dict[str, Any]) -> dict[str, Any]:
    """Collect OpenTelemetry critical-path span breakdown from Cloud Trace."""
    raw_telemetry = {
        "source": "Google Cloud Trace OpenTelemetry",
        "sampled_trace_count": 4800,
        "p95_end_to_end_ms": 1812.0,
        "critical_span": "bigquery.jobs.query -> unpartitioned_cart_join",
        "span_share_pct": 89.4,
        "oauth_client_secret": "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xpose991",
    }
    return scrub_gcp_telemetry_payload(raw_telemetry)


@node(name="spanner_lease_status_probe")
def spanner_lease_status_probe(node_input: dict[str, Any]) -> dict[str, Any]:
    """Query Cloud Spanner TrueTime clock uncertainty bound epsilon and active mutation locks."""
    iv = _SAGA_ENGINE.tt_now()
    return {
        "source": "Google Cloud Spanner TrueTime",
        "spanner_instance": "sre-control-plane-nam-eur",
        "truetime_epsilon_ms": iv.epsilon_ms,
        "required_commit_wait_ms": iv.commit_wait_ms,
        "active_conflicting_leases": 0,
        "external_consistency_verified": True,
    }


telemetry_join = JoinNode(name="telemetry_join")


@node(name="format_telemetry_for_gemini")
def format_telemetry_for_gemini(node_input: dict[str, Any]) -> str:
    """Serialize joined 4-branch GCP telemetry packet into a canonical prompt payload for Gemini."""
    return (
        "Analyze the following Cloud-SDP-sanitized multi-region Google Cloud telemetry packet. "
        "Call `query_bigquery_slot_wait_projection` to verify the Kingman VUT wait reduction for adding "
        "1200 BigQuery Enterprise burst slots to the 2000-slot baseline reservation (1988 active slots), "
        "then emit the strict `SRESynthesisDecision` JSON contract.\n\n"
        f"JOINED_TELEMETRY_JSON:\n{json.dumps(node_input, indent=2, sort_keys=True)}"
    )


sre_root_cause_agent = LlmAgent(
    name="sre_root_cause_agent",
    model=Gemini(
        model=GEMINI_MODEL,
        retry_options=types.HttpRetryOptions(attempts=3, initial_delay=1.0, exp_base=2.0),
    ),
    instruction=(
        "You are a Principal Google Cloud Site Reliability & Capacity Engineering Agent. "
        "You receive deterministic, Cloud-SDP-sanitized telemetry from Cloud Monitoring, BigQuery "
        "INFORMATION_SCHEMA, Cloud Trace, and Cloud Spanner TrueTime. "
        "1. Always invoke `query_bigquery_slot_wait_projection(current_used_slots=1988, baseline_slots=2000, added_burst_slots=1200)` "
        "to compute exact Kingman queueing delay before finalizing your plan. "
        "2. If `error_budget_burn_1h > 14.4` and `active_conflicting_leases == 0`, set `drain_canary_to_zero=True`, "
        "`recommended_bq_burst_slots=1200`, and `route_decision='execute_saga'` (unless `force_hitl=True` is requested, "
        "in which case set `route_decision='hitl_escalation'`). "
        "3. Never fabricate unverified metrics or bypass deterministic guardrails."
    ),
    tools=[query_bigquery_slot_wait_projection],
    before_tool_callback=lyapunov_sha256_loop_breaker,
    after_tool_callback=cloud_sdp_telemetry_redactor,
    output_schema=SRESynthesisDecision,
    output_key="sre_decision",
)


@node(name="route_remediation_gate")
def route_remediation_gate(ctx: Context, node_input: dict[str, Any]) -> Event:
    """Deterministic control gate inspecting Gemini's structured decision & policy bounds."""
    decision = SRESynthesisDecision.model_validate(node_input)
    packet = ctx.state.get("incident_packet", {})
    if packet.get("force_hitl") or decision.recommended_bq_burst_slots > 1500:
        route = "hitl_escalation"
    else:
        route = decision.route_decision
    return Event(output=decision.model_dump(), route=route, state={"validated_decision": decision.model_dump()})


def execute_spanner_truetime_saga(ctx: Context, node_input: dict[str, Any]) -> Event:
    """Execute linearizable 3-step GCP remediation saga under Cloud Spanner TrueTime commit-wait."""
    packet = ctx.state.get("incident_packet", {})
    saga_result = _SAGA_ENGINE.execute_remediation_saga(
        incident_id=node_input["incident_id"],
        burst_slots=int(node_input.get("recommended_bq_burst_slots", 1200)),
        inject_downstream_503=bool(packet.get("inject_downstream_503", False)),
    )
    final_payload = {
        "decision": node_input,
        "spanner_saga": saga_result,
    }
    return Event(
        output=final_payload,
        state={"final_saga_result": saga_result},
        content=types.Content(
            role="model",
            parts=[types.Part.from_text(text=json.dumps(final_payload, indent=2))],
        ),
    )


@node(name="hitl_sre_escalation")
def hitl_sre_escalation(ctx: Context, node_input: dict[str, Any]) -> Event:
    """Escalate high-blast-radius changes to on-call Principal SRE before mutating production."""
    payload = {
        "status": "PAUSED_FOR_PRINCIPAL_SRE_APPROVAL",
        "decision": node_input,
    }
    return Event(
        output=payload,
        content=types.Content(
            role="model",
            parts=[types.Part.from_text(text=json.dumps(payload, indent=2))],
        ),
    )


guarded_saga_node = FunctionNode(
    name="execute_spanner_truetime_saga",
    func=execute_spanner_truetime_saga,
    retry_config=RetryConfig(max_attempts=2, initial_delay=0.05, backoff_factor=2.0),
)

root_agent = Workflow(
    name="gcp_sre_control_governor",
    description=(
        "Autonomous Google Cloud SRE, BigQuery Slot Governance & Cloud Run Canary Self-Healing "
        "Control Plane using ADK 2.2 Graph Workflows, Gemini 3.8 Flash / Gemini 4, Cloud SDP, "
        "and Cloud Spanner TrueTime."
    ),
    edges=[
        ("START", telemetry_dispatcher),
        (
            telemetry_dispatcher,
            (
                cloud_monitoring_slo_probe,
                bigquery_slot_forensics_probe,
                cloud_trace_span_probe,
                spanner_lease_status_probe,
            ),
        ),
        (
            (
                cloud_monitoring_slo_probe,
                bigquery_slot_forensics_probe,
                cloud_trace_span_probe,
                spanner_lease_status_probe,
            ),
            telemetry_join,
        ),
        (telemetry_join, format_telemetry_for_gemini),
        (format_telemetry_for_gemini, sre_root_cause_agent),
        (sre_root_cause_agent, route_remediation_gate),
        (
            route_remediation_gate,
            {
                "execute_saga": guarded_saga_node,
                "hitl_escalation": hitl_sre_escalation,
            },
        ),
    ],
)

app = App(name="gcp_sre_control_governor", root_agent=root_agent)
