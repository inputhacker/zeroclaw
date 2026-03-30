#!/usr/bin/env python3
"""Resolve a Context Book endpoint for external agents via mDNS/DNS-SD."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass, field


def canonical_service_type(raw: str) -> str:
    value = raw.strip()
    if not value:
        return "_contextbook._tcp.local."
    value = value.rstrip(".")
    if value.startswith("_") and value.endswith("._tcp.local"):
        return f"{value}."

    base = value.lower()
    if base.startswith("_"):
        base = base.lstrip("_")
    if base.endswith("._tcp.local"):
        return f"_{base}."
    return f"_{base}._tcp.local."


def avahi_service_type(canonical: str) -> str:
    value = canonical.rstrip(".")
    if value.endswith(".local"):
        value = value[: -len(".local")]
    return value


def split_escaped_fields(line: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    escaped = False
    for ch in line.rstrip("\n"):
        if escaped:
            current.append(ch)
            escaped = False
            continue
        if ch == "\\":
            escaped = True
            continue
        if ch == ";":
            parts.append("".join(current))
            current = []
            continue
        current.append(ch)
    parts.append("".join(current))
    return parts


def strip_quotes(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] == '"':
        return text[1:-1]
    return text


def parse_txt_fields(fields: list[str]) -> dict[str, str]:
    txt: dict[str, str] = {}
    raw_text = " ".join(field for field in fields if field.strip())
    try:
        txt_fields = shlex.split(raw_text)
    except ValueError:
        txt_fields = fields

    for raw_field in txt_fields:
        field = strip_quotes(raw_field)
        if not field:
            continue
        if "=" in field:
            key, value = field.split("=", 1)
            txt[key] = value
        else:
            txt[field] = ""
    return txt


def parse_int(value: str | None, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


@dataclass
class Candidate:
    source: str
    fullname: str
    name: str
    service_type: str
    domain: str
    host: str
    address: str
    port: int
    endpoint: str
    txt: dict[str, str] = field(default_factory=dict)
    service: str = ""
    version: int = 0
    api: list[str] = field(default_factory=list)
    env: str = ""
    bootstrap: str = ""
    features: list[str] = field(default_factory=list)
    priority: int = 100
    weight: int = 0
    instance_id: str = ""


def parse_candidate(fields: list[str]) -> Candidate | None:
    if len(fields) < 9 or fields[0] != "=":
        return None
    name = fields[3]
    service_type = fields[4]
    domain = fields[5]
    host = fields[6]
    address = fields[7]
    port = parse_int(fields[8], 0)
    txt = parse_txt_fields(fields[9:])
    endpoint_host = address or host
    endpoint = f"http://{endpoint_host}:{port}" if endpoint_host and port > 0 else ""
    fullname = f"{name}.{service_type}.{domain}.".replace("..", ".")
    return Candidate(
        source="mdns",
        fullname=fullname,
        name=name,
        service_type=service_type,
        domain=domain,
        host=host,
        address=address,
        port=port,
        endpoint=endpoint,
        txt=txt,
        service=txt.get("service", ""),
        version=parse_int(txt.get("ver"), 0),
        api=split_csv(txt.get("api")),
        env=txt.get("env", ""),
        bootstrap=txt.get("bootstrap", ""),
        features=split_csv(txt.get("features")),
        priority=parse_int(txt.get("priority"), 100),
        weight=parse_int(txt.get("weight"), 0),
        instance_id=txt.get("instance_id", ""),
    )


def discover_candidates(service_type: str, timeout_ms: int) -> list[Candidate]:
    browse_type = avahi_service_type(service_type)
    cmd = [
        "avahi-browse",
        "--parsable",
        "--resolve",
        "--terminate",
        browse_type,
    ]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=max(timeout_ms / 1000.0, 1.0) + 2.0,
            check=False,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("avahi-browse is required for this skill but is not installed") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"avahi-browse timed out for service type {browse_type}") from exc

    if proc.returncode not in (0,):
        stderr = proc.stderr.strip() or proc.stdout.strip()
        raise RuntimeError(f"avahi-browse failed for {browse_type}: {stderr}")

    candidates: list[Candidate] = []
    seen: set[tuple[str, str, int]] = set()
    for line in proc.stdout.splitlines():
        candidate = parse_candidate(split_escaped_fields(line))
        if candidate is None or not candidate.endpoint:
            continue
        key = (candidate.fullname, candidate.address, candidate.port)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(candidate)
    return candidates


def candidate_filter_reasons(
    candidate: Candidate,
    required_features: list[str],
    reject_local_trust: bool,
) -> list[str]:
    reasons: list[str] = []
    if candidate.service != "context-book":
        reasons.append("missing service=context-book")
    if candidate.version != 1:
        reasons.append("unsupported ver")
    api_set = set(candidate.api)
    if "rest" not in api_set or "sse" not in api_set:
        reasons.append("api must include rest and sse")
    if reject_local_trust and candidate.bootstrap == "trusted-network+shared-secret":
        reasons.append("bootstrap policy rejected")
    for feature in required_features:
        if feature not in candidate.features:
            reasons.append(f"missing feature {feature}")
    return reasons


def score_candidate(candidate: Candidate, preferred_env: str) -> tuple[int, int, int, int, str]:
    env_match = 1 if preferred_env and candidate.env == preferred_env else 0
    tie = candidate.instance_id or candidate.fullname
    return (-candidate.version, -env_match, candidate.priority, -candidate.weight, tie)


def join_endpoint_path(endpoint: str, path: str) -> str:
    normalized = path.strip() or "/"
    if not normalized.startswith("/"):
        normalized = f"/{normalized}"
    return f"{endpoint.rstrip('/')}{normalized}"


def preflight(candidate: Candidate) -> dict[str, object]:
    path = candidate.txt.get("health") or candidate.txt.get("path") or "/"
    url = join_endpoint_path(candidate.endpoint, path)
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return {
                "ok": True,
                "reachable": True,
                "status": response.getcode(),
                "url": url,
            }
    except urllib.error.HTTPError as exc:
        return {
            "ok": True,
            "reachable": True,
            "status": exc.code,
            "url": url,
        }
    except Exception as exc:  # pragma: no cover - network path
        return {
            "ok": False,
            "reachable": False,
            "error": str(exc),
            "url": url,
        }


def manual_candidate(url: str) -> Candidate:
    trimmed = url.strip()
    return Candidate(
        source="manual_override",
        fullname="manual-context-book",
        name="manual-context-book",
        service_type="manual",
        domain="manual",
        host="manual",
        address="manual",
        port=0,
        endpoint=trimmed.rstrip("/"),
    )


def select_candidate(
    candidates: list[Candidate],
    preferred_env: str,
    required_features: list[str],
    reject_local_trust: bool,
    do_preflight: bool,
) -> tuple[Candidate | None, list[dict[str, object]]]:
    evaluations: list[dict[str, object]] = []
    compatible: list[Candidate] = []

    for candidate in candidates:
        reasons = candidate_filter_reasons(candidate, required_features, reject_local_trust)
        entry: dict[str, object] = {
            "candidate": asdict(candidate),
            "filterReasons": reasons,
        }
        evaluations.append(entry)
        if not reasons:
            compatible.append(candidate)

    compatible.sort(key=lambda item: score_candidate(item, preferred_env))

    for candidate in compatible:
        if not do_preflight:
            return candidate, evaluations
        result = preflight(candidate)
        for entry in evaluations:
            current = entry.get("candidate")
            if isinstance(current, dict) and current.get("fullname") == candidate.fullname:
                entry["preflight"] = result
                break
        if bool(result.get("reachable")):
            return candidate, evaluations

    return None, evaluations


def print_json(payload: dict[str, object]) -> int:
    sys.stdout.write(json.dumps(payload, ensure_ascii=True))
    sys.stdout.write("\n")
    return 0 if payload.get("ok") else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Context Book discovery helper")
    parser.add_argument(
        "--service-type",
        default=os.environ.get("CONTEXT_BOOK_DISCOVERY_SERVICE_TYPE", "_contextbook._tcp.local."),
        help="DNS-SD service type",
    )
    parser.add_argument(
        "--window-ms",
        type=int,
        default=parse_int(os.environ.get("CONTEXT_BOOK_DISCOVERY_WINDOW_MS"), 3000),
        help="Discovery scan window",
    )
    parser.add_argument(
        "--preferred-env",
        default=os.environ.get("CONTEXT_BOOK_ENV", ""),
        help="Preferred env TXT value",
    )
    parser.add_argument(
        "--manual-url",
        default=os.environ.get("CONTEXT_BOOK_URL", ""),
        help="Manual endpoint override used before discovery",
    )
    parser.add_argument(
        "--require-feature",
        action="append",
        default=[],
        help="Feature token required from TXT metadata; may be repeated",
    )
    parser.add_argument(
        "--reject-local-trust",
        action="store_true",
        help="Reject bootstrap=trusted-network+shared-secret candidates",
    )
    parser.add_argument(
        "--no-preflight",
        action="store_true",
        help="Skip HTTP reachability preflight before selecting an endpoint",
    )
    parser.add_argument("command", choices=["discover", "report"])
    args = parser.parse_args()

    normalized_service_type = canonical_service_type(args.service_type)
    try:
        if args.manual_url.strip():
            manual = manual_candidate(args.manual_url)
            payload: dict[str, object] = {
                "ok": True,
                "command": args.command,
                "serviceType": normalized_service_type,
                "selected": asdict(manual),
                "override": "manual_url",
            }
            if args.command == "report":
                payload["candidates"] = []
            if not args.no_preflight:
                payload["preflight"] = preflight(manual)
            return print_json(payload)

        discovered = discover_candidates(normalized_service_type, max(args.window_ms, 500))
        selected, evaluations = select_candidate(
            discovered,
            preferred_env=args.preferred_env.strip(),
            required_features=list(args.require_feature),
            reject_local_trust=args.reject_local_trust,
            do_preflight=not args.no_preflight,
        )
        if selected is not None:
            payload: dict[str, object] = {
                "ok": True,
                "command": args.command,
                "serviceType": normalized_service_type,
                "selected": asdict(selected),
            }
            if args.command == "report":
                payload["candidates"] = evaluations
            return print_json(payload)

        return print_json(
            {
                "ok": False,
                "command": args.command,
                "serviceType": normalized_service_type,
                "error": "no compatible Context Book endpoint found",
                "candidates": evaluations,
            }
        )
    except Exception as exc:
        return print_json(
            {
                "ok": False,
                "command": args.command,
                "serviceType": normalized_service_type,
                "error": str(exc),
            }
        )


if __name__ == "__main__":
    raise SystemExit(main())
