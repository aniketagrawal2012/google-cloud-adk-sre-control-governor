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
"""Unit and invariant tests for the ADK 2.2 Google Cloud SRE & FinOps Control Governor."""

from __future__ import annotations

import math
from types import SimpleNamespace

from app.agent import (
    cloud_monitoring_slo_probe,
    cloud_sdp_telemetry_redactor,
    lyapunov_sha256_loop_breaker,
    query_bigquery_slot_wait_projection,
    root_agent,
)
from app.control_math import (
    birthday_collision_upper_bound,
    expected_absorbed_retries,
    fork_join_exponential_expectation_ms,
    kingman_vut_wait_ms,
    kv_cache_bytes_per_token,
    kv_prefix_retention_ratio,
    lyapunov_energy,
    scrub_gcp_telemetry_payload,
)
from app.spanner_saga import CloudSpannerTrueTimeSaga


def test_workflow_dag_topology_and_nodes() -> None:
    assert root_agent.name == "gcp_sre_control_governor"
    assert len(root_agent.edges) == 7


def test_cloud_sdp_redactor_enforces_zero_credential_leakage() -> None:
    tool = SimpleNamespace(name="cloud_logging_read")
    ctx = SimpleNamespace(state={})
    raw = {
        "service_account_private_key_id": "deadbeef1234567890",
        "oauth_client_secret": "GOCSPX-secret123",
        "billing_account_id": "01A2B3-C4D5E6-F78901",
        "caller_vpc_source_ip": "10.24.19.8",
        "log_line": "Authorization: Bearer ya29.a0AfH6SMBx999999999999999999999 from 10.24.19.8",
    }
    cleaned = cloud_sdp_telemetry_redactor(tool, {}, ctx, raw)
    serialized = str(cleaned)
    assert "deadbeef" not in serialized
    assert "GOCSPX-" not in serialized
    assert "01A2B3" not in serialized
    assert "10.24.19.8" not in serialized
    assert "ya29." not in serialized
    assert len(ctx.state["cloud_sdp_redactions"]) >= 5


def test_lyapunov_sha256_sliding_window_trips_at_k3() -> None:
    tool = SimpleNamespace(name="bigquery_update_reservation")
    ctx = SimpleNamespace(state={})
    args = {"reservation": "prod-bi-reservation-us", "slots": 3200}

    assert lyapunov_sha256_loop_breaker(tool, args, ctx) is None
    assert lyapunov_sha256_loop_breaker(tool, args, ctx) is None
    tripped = lyapunov_sha256_loop_breaker(tool, args, ctx)
    assert tripped is not None
    assert tripped["status"] == "CIRCUIT_BREAKER_OPEN"
    assert tripped["repeat_count"] == 3
    assert ctx.state["lyapunov_breaker_tripped"] is True

    # Birthday bound for W=6, b=64 must equal 30 / 2^65 = 15 / 2^64
    assert math.isclose(birthday_collision_upper_bound(6, 64), 30.0 / (2 ** 65), rel_tol=1e-12)
    assert lyapunov_energy(0.0, 0) < lyapunov_energy(2.0, 3)
    assert math.isclose(expected_absorbed_retries(0.70, 3), 2.19, rel_tol=1e-9)


def test_kingman_vut_and_fork_join_order_statistics() -> None:
    proj = query_bigquery_slot_wait_projection(
        current_used_slots=1988,
        baseline_slots=2000,
        added_burst_slots=1200,
        mean_stage_service_ms=42.0,
    )
    assert proj["utilization_before"] == 0.994
    assert proj["utilization_after"] == 0.6212
    assert proj["kingman_wait_before_ms"] > 11000.0
    assert proj["kingman_wait_after_ms"] < 120.0
    assert proj["wait_reduction_pct"] > 98.9

    # Parallel 4-branch expectation must be strictly smaller than sum of branch means
    means = [480.0, 620.0, 540.0, 390.0]
    par_mean = fork_join_exponential_expectation_ms(means)
    assert par_mean < sum(means)


def test_gqa_kv_cache_and_prefix_retention_physics() -> None:
    b_tok = kv_cache_bytes_per_token(num_layers=64, num_kv_heads=8, head_dim=128, bytes_per_elem=2)
    assert b_tok == 262_144  # 256 KiB per token
    assert math.isclose(kv_prefix_retention_ratio(72_001, 72_000), 1.0, rel_tol=1e-12)
    assert math.isclose(kv_prefix_retention_ratio(1, 72_000), 0.0, rel_tol=1e-12)


def test_cloud_spanner_truetime_saga_commit_and_503_compensation() -> None:
    saga = CloudSpannerTrueTimeSaga(epsilon_ms=3.5)
    # 1. Fault-injected 503 on step 3 must revert Cloud Run split and BigQuery slots cleanly
    rolled = saga.execute_remediation_saga(
        incident_id="INC-503-TEST",
        burst_slots=1200,
        inject_downstream_503=True,
    )
    assert rolled["outcome"] == "ROLLED_BACK_CLEAN"
    assert rolled["compensated_steps"] == [
        "step_2_bigquery_slot_burst",
        "step_1_cloud_run_traffic_drain",
    ]
    assert rolled["cloud_run_traffic"]["checkout-edge-00048-v9x"] == 25
    assert rolled["bigquery_slots"] == 2000

    # 2. Healthy execution commits atomically after 2*epsilon = 7.0 ms TrueTime wait
    committed = saga.execute_remediation_saga(
        incident_id="INC-OK-TEST",
        burst_slots=1200,
        inject_downstream_503=False,
    )
    assert committed["outcome"] == "COMMITTED"
    assert committed["truetime_commit_wait_ms"] == 7.0
    assert committed["cloud_run_traffic"]["checkout-edge-00048-v9x"] == 0
    assert committed["bigquery_slots"] == 3200
    probe_out = cloud_monitoring_slo_probe._func({"incident_id": "INC-OK-TEST"})
    assert "[REDACTED_CLOUD_SDP:" in probe_out["service_account_private_key_id"]
