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
"""Closed-form control-theoretic, queueing, information-theoretic, and KV-cache primitives."""

from __future__ import annotations

import hashlib
import json
import math
import re
from typing import Any, Mapping, Sequence

# Restricted GCP credential & audit metadata fields zeroed out before LLM ingestion (I(S; X) = 0).
SENSITIVE_GCP_TELEMETRY_KEYS = frozenset(
    {
        "service_account_private_key_id",
        "oauth_client_secret",
        "billing_account_id",
        "caller_vpc_source_ip",
        "iap_jwt_assertion",
        "kms_raw_dek_material",
    }
)

_CREDENTIAL_REGEX_RULES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("GCP_API_KEY", re.compile(r"\bAIza[0-9A-Za-z\-_]{35}\b")),
    (
        "GCP_PRIVATE_KEY_HEADER",
        re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    ),
    (
        "BEARER_OAUTH_TOKEN",
        re.compile(r"\bya29\.[0-9A-Za-z\-_]{20,}\b"),
    ),
    (
        "BILLING_ACCOUNT_ID",
        re.compile(r"\b[0-9A-F]{6}-[0-9A-F]{6}-[0-9A-F]{6}\b"),
    ),
    (
        "PRIVATE_IPV4_CIDR",
        re.compile(r"\b10\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"),
    ),
)


def canonical_tool_digest(tool_name: str, args: Mapping[str, Any]) -> str:
    """Compute a 64-bit truncated SHA-256 fingerprint over canonical (tool_name, args)."""
    canonical = json.dumps(
        {"tool": tool_name, "args": args},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def birthday_collision_upper_bound(window_size: int, bits: int = 64) -> float:
    """Exact birthday-paradox upper bound W(W - 1) / 2^(b + 1) on hash collision in window W."""
    if window_size <= 1:
        return 0.0
    return (window_size * (window_size - 1)) / float(1 << (bits + 1))


def lyapunov_energy(pending_faults: float, consecutive_retries: int, weight_lambda: float = 0.5) -> float:
    """Quadratic discrete-time Lyapunov candidate V(e_t, r_t) = e_t^2 + lambda * r_t^2."""
    return (pending_faults ** 2) + weight_lambda * (consecutive_retries ** 2)


def expected_absorbed_retries(failure_prob_q: float, threshold_k: int = 3) -> float:
    """Closed-form E[N_k] = (1 - q^k) / (1 - q) for k-absorbing transient retry chain."""
    if threshold_k <= 0:
        return 0.0
    if abs(1.0 - failure_prob_q) < 1e-12:
        return float(threshold_k)
    return (1.0 - (failure_prob_q ** threshold_k)) / (1.0 - failure_prob_q)


def kingman_vut_wait_ms(
    utilization_rho: float,
    mean_service_ms: float,
    cv_arrival_sq: float = 1.44,
    cv_service_sq: float = 1.96,
) -> float:
    """Kingman's VUT heavy-traffic mean queue wait approximation for BigQuery slot contention."""
    if not (0.0 <= utilization_rho < 1.0):
        raise ValueError("Utilization rho must satisfy 0 <= rho < 1 for finite steady-state queue.")
    variability = (cv_arrival_sq + cv_service_sq) / 2.0
    congestion = utilization_rho / (1.0 - utilization_rho)
    return variability * congestion * mean_service_ms


def fork_join_exponential_expectation_ms(branch_means_ms: Sequence[float]) -> float:
    """Exact expected latency E[max_i L_i] for independent exponential telemetry probes via inclusion-exclusion."""
    n = len(branch_means_ms)
    if n == 0:
        return 0.0
    rates = [1.0 / m for m in branch_means_ms]
    total = 0.0
    for mask in range(1, 1 << n):
        subset_rate = 0.0
        bits_set = 0
        for idx in range(n):
            if (mask >> idx) & 1:
                subset_rate += rates[idx]
                bits_set += 1
        sign = 1.0 if (bits_set % 2 == 1) else -1.0
        total += sign / subset_rate
    return total


def kv_cache_bytes_per_token(
    num_layers: int = 64,
    num_kv_heads: int = 8,
    head_dim: int = 128,
    bytes_per_elem: int = 2,
) -> int:
    """Exact GQA KV-cache memory per token: 2 * L * H_kv * d_h * b_elem bytes."""
    return 2 * num_layers * num_kv_heads * head_dim * bytes_per_elem


def kv_prefix_retention_ratio(perturbation_index: int, prefix_tokens: int) -> float:
    """Causal Transformer KV-cache retention fraction under perturbation at 1-indexed token j_0."""
    if prefix_tokens <= 0:
        return 0.0
    if perturbation_index > prefix_tokens:
        return 1.0
    if perturbation_index <= 1:
        return 0.0
    return (perturbation_index - 1) / float(prefix_tokens)


def scrub_gcp_telemetry_payload(payload: Any, redactions: list[str] | None = None) -> Any:
    """Deterministic Cloud SDP-aligned pre-LLM redaction kernel enforcing I(S_cred; X_LLM) = 0."""
    if redactions is None:
        redactions = []

    if isinstance(payload, dict):
        sanitized: dict[str, Any] = {}
        for k, v in payload.items():
            if k in SENSITIVE_GCP_TELEMETRY_KEYS:
                redactions.append(f"FIELD:{k}")
                sanitized[k] = f"[REDACTED_CLOUD_SDP:{k.upper()}]"
            else:
                sanitized[k] = scrub_gcp_telemetry_payload(v, redactions)
        return sanitized

    if isinstance(payload, list):
        return [scrub_gcp_telemetry_payload(item, redactions) for item in payload]

    if isinstance(payload, str):
        text = payload
        for label, pattern in _CREDENTIAL_REGEX_RULES:
            if pattern.search(text):
                redactions.append(f"REGEX:{label}")
                text = pattern.sub(f"[REDACTED_{label}]", text)
        return text

    return payload
