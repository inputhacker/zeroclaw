#!/usr/bin/env python3
"""Deterministic Context Book skill CLI for event-driven entity retrieval."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any

BOOTSTRAP_SECRET_ENV = "CONTEXT_BOOK_BOOTSTRAP_SHARED_SECRET"
BOOTSTRAP_SECRET_HEADER = "X-Context-Book-Bootstrap-Secret"
BOOTSTRAP_WAIT_TOKEN_HEADER = "X-Context-Book-Bootstrap-Wait-Token"
BOOTSTRAP_STATUS_WAIT_MS = 5000
BOOTSTRAP_STATUS_ATTEMPTS = 24
BOOTSTRAP_APPROVAL_POLL_INTERVAL_S = 0.5
BOOTSTRAP_APPROVAL_POLL_ATTEMPTS = 240


def _json_request(
    base_url: str,
    method: str,
    path: str,
    token: str | None = None,
    body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 20.0,
) -> tuple[int, dict[str, Any]]:
    if path.startswith("http://") or path.startswith("https://"):
        url = path
    else:
        url = f"{base_url.rstrip('/')}{path}"
    data = None
    request_headers = {"content-type": "application/json"}
    if token:
        request_headers["authorization"] = f"Bearer {token}"
    if headers:
        request_headers.update(headers)
    if body is not None:
        data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, method=method, data=data, headers=request_headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            parsed = json.loads(raw) if raw else {}
            return resp.getcode(), parsed if isinstance(parsed, dict) else {"items": parsed}
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8")
        parsed: dict[str, Any] = {}
        if raw:
            try:
                j = json.loads(raw)
                parsed = j if isinstance(j, dict) else {"items": j}
            except json.JSONDecodeError:
                parsed = {"raw": raw}
        return e.code, parsed


def _bootstrap_secret() -> str:
    secret = os.environ.get(BOOTSTRAP_SECRET_ENV, "").strip()
    if secret:
        return secret
    raise RuntimeError(
        f"{BOOTSTRAP_SECRET_ENV} must be set when --token is not provided because bootstrap now uses connect/init/complete."
    )


def _extract_error_details(payload: dict[str, Any]) -> dict[str, Any]:
    error = payload.get("error")
    if isinstance(error, dict):
        details = error.get("details")
        if isinstance(details, dict):
            return details
    return {}


def _bootstrap_error_code(payload: dict[str, Any]) -> str:
    error = payload.get("error")
    if isinstance(error, dict):
        code = error.get("code")
        if isinstance(code, str):
            return code.strip()
    return ""


def _bootstrap_pending_info(payload: dict[str, Any]) -> dict[str, str]:
    source: Any = payload.get("request")
    if not isinstance(source, dict):
        source = _extract_error_details(payload)
    if not isinstance(source, dict):
        return {}
    request_id = str(source.get("requestId") or "").strip()
    requested_agent_id = str(source.get("requestedAgentId") or "").strip()
    wait_token = str(source.get("waitToken") or "").strip()
    status_url = str(source.get("statusUrl") or "").strip()
    complete_url = str(source.get("completeUrl") or "").strip()
    if not (request_id and requested_agent_id and wait_token and status_url and complete_url):
        return {}
    return {
        "requestId": request_id,
        "requestedAgentId": requested_agent_id,
        "waitToken": wait_token,
        "statusUrl": status_url,
        "completeUrl": complete_url,
    }


def _status_url_with_wait(status_url: str) -> str:
    separator = "&" if "?" in status_url else "?"
    return f"{status_url}{separator}waitMs={BOOTSTRAP_STATUS_WAIT_MS}"


def _connect_once(base_url: str, agent_id: str, bootstrap_secret: str) -> tuple[int, dict[str, Any]]:
    return _json_request(
        base_url,
        "POST",
        "/agents/connect",
        body={"agentId": agent_id},
        headers={BOOTSTRAP_SECRET_HEADER: bootstrap_secret},
    )


def _complete_bootstrap_request(base_url: str, bootstrap_secret: str, pending: dict[str, str]) -> str:
    code, payload = _json_request(
        base_url,
        "POST",
        pending["completeUrl"],
        body={"requestId": pending["requestId"]},
        headers={BOOTSTRAP_WAIT_TOKEN_HEADER: pending["waitToken"]},
    )
    if code in (200, 201) and payload.get("accessToken"):
        return str(payload["accessToken"])
    if code == 404:
        code, payload = _connect_once(base_url, pending["requestedAgentId"], bootstrap_secret)
        if code == 200 and payload.get("accessToken"):
            return str(payload["accessToken"])
        raise RuntimeError(
            f"bootstrap complete fallback connect failed for agent_id={pending['requestedAgentId']}: status={code}, payload={payload}"
        )
    raise RuntimeError(
        f"bootstrap complete failed for request_id={pending['requestId']}: status={code}, payload={payload}"
    )


def _wait_for_dashboard_approval(base_url: str, agent_id: str, bootstrap_secret: str) -> str:
    for _ in range(BOOTSTRAP_APPROVAL_POLL_ATTEMPTS):
        time.sleep(BOOTSTRAP_APPROVAL_POLL_INTERVAL_S)
        code, payload = _connect_once(base_url, agent_id, bootstrap_secret)
        if code == 200 and payload.get("accessToken"):
            return str(payload["accessToken"])
        error_code = _bootstrap_error_code(payload)
        if error_code == "BOOTSTRAP_APPROVAL_REQUIRED":
            continue
        if error_code in {"BOOTSTRAP_REQUEST_DENIED", "BOOTSTRAP_REQUEST_EXPIRED"}:
            raise RuntimeError(f"dashboard approval failed for agent_id={agent_id}: status={code}, payload={payload}")
        raise RuntimeError(
            f"failed while waiting for dashboard approval for agent_id={agent_id}: status={code}, payload={payload}"
        )
    raise RuntimeError(
        f"dashboard approval timed out for agent_id={agent_id} after {BOOTSTRAP_APPROVAL_POLL_ATTEMPTS} attempts"
    )


def _wait_for_dashboard_approval_with_pending(base_url: str, bootstrap_secret: str, pending: dict[str, str]) -> str:
    for _ in range(BOOTSTRAP_STATUS_ATTEMPTS):
        code, payload = _json_request(
            base_url,
            "GET",
            _status_url_with_wait(pending["statusUrl"]),
            headers={BOOTSTRAP_WAIT_TOKEN_HEADER: pending["waitToken"]},
            timeout=BOOTSTRAP_STATUS_WAIT_MS / 1000 + 10,
        )
        if code != 200:
            raise RuntimeError(
                f"bootstrap status failed for request_id={pending['requestId']}: status={code}, payload={payload}"
            )
        request = payload.get("request")
        request_body = request if isinstance(request, dict) else {}
        approval_state = str(request_body.get("approvalState") or "").strip()
        next_action = str(payload.get("nextAction") or "").strip()
        if approval_state == "Approved" or next_action == "complete":
            return _complete_bootstrap_request(base_url, bootstrap_secret, pending)
        if approval_state == "Pending" or next_action == "wait":
            continue
        raise RuntimeError(
            f"dashboard approval did not complete for request_id={pending['requestId']}: state={approval_state or 'unknown'}"
        )
    raise RuntimeError(
        f"dashboard approval timed out for request_id={pending['requestId']} after {BOOTSTRAP_STATUS_ATTEMPTS} status waits"
    )


def _ensure_token(base_url: str, agent_id: str) -> str:
    bootstrap_secret = _bootstrap_secret()
    code, payload = _connect_once(base_url, agent_id, bootstrap_secret)
    if code == 200 and payload.get("accessToken"):
        return str(payload["accessToken"])
    if _bootstrap_error_code(payload) == "BOOTSTRAP_APPROVAL_REQUIRED":
        pending = _bootstrap_pending_info(payload)
        if pending:
            return _wait_for_dashboard_approval_with_pending(base_url, bootstrap_secret, pending)
        return _wait_for_dashboard_approval(base_url, agent_id, bootstrap_secret)
    if code != 404:
        raise RuntimeError(f"connect failed for agent_id={agent_id}: status={code}, payload={payload}")

    register_body = {"agentName": agent_id, "deviceType": "unknown"}
    code, payload = _json_request(
        base_url,
        "POST",
        "/bootstrap/register/init",
        body=register_body,
        headers={BOOTSTRAP_SECRET_HEADER: bootstrap_secret},
    )
    if code == 202:
        pending = _bootstrap_pending_info(payload)
        if pending:
            return _wait_for_dashboard_approval_with_pending(base_url, bootstrap_secret, pending)
        raise RuntimeError(f"bootstrap init succeeded without wait metadata: payload={payload}")
    if code not in (200, 201, 404):
        raise RuntimeError(f"bootstrap init failed for agent_id={agent_id}: status={code}, payload={payload}")

    code, payload = _json_request(
        base_url,
        "POST",
        "/agents/register",
        body=register_body,
        headers={BOOTSTRAP_SECRET_HEADER: bootstrap_secret},
    )
    if code in (200, 201) and payload.get("accessToken"):
        return str(payload["accessToken"])
    if _bootstrap_error_code(payload) == "BOOTSTRAP_APPROVAL_REQUIRED":
        pending = _bootstrap_pending_info(payload)
        if pending:
            return _wait_for_dashboard_approval_with_pending(base_url, bootstrap_secret, pending)
        return _wait_for_dashboard_approval(base_url, agent_id, bootstrap_secret)

    code, payload = _connect_once(base_url, agent_id, bootstrap_secret)
    if code == 200 and payload.get("accessToken"):
        return str(payload["accessToken"])
    if _bootstrap_error_code(payload) == "BOOTSTRAP_APPROVAL_REQUIRED":
        pending = _bootstrap_pending_info(payload)
        if pending:
            return _wait_for_dashboard_approval_with_pending(base_url, bootstrap_secret, pending)
        return _wait_for_dashboard_approval(base_url, agent_id, bootstrap_secret)

    raise RuntimeError(f"failed to obtain token for agent_id={agent_id}: status={code}, payload={payload}")


def _activate_if_needed(base_url: str, agent_id: str, token: str) -> None:
    _json_request(
        base_url,
        "PATCH",
        f"/agents/{agent_id}/status",
        token=token,
        body={"status": "Active"},
    )


def _extract_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    items = payload.get("items", [])
    if isinstance(items, list):
        return [x for x in items if isinstance(x, dict)]
    return []


def _find_by_id(items: list[dict[str, Any]], key: str, value: str) -> dict[str, Any] | None:
    for item in items:
        if str(item.get(key, "")) == value:
            return item
    return None


def _list_contexts(base_url: str, token: str) -> list[dict[str, Any]]:
    code, payload = _json_request(base_url, "GET", "/contexts", token=token)
    if code != 200:
        raise RuntimeError(f"list contexts failed: status={code}, payload={payload}")
    return _extract_items(payload)


def _list_votes(base_url: str, token: str) -> list[dict[str, Any]]:
    code, payload = _json_request(base_url, "GET", "/votes", token=token)
    if code != 200:
        raise RuntimeError(f"list votes failed: status={code}, payload={payload}")
    return _extract_items(payload)


def _list_agents(base_url: str, token: str) -> list[dict[str, Any]]:
    code, payload = _json_request(base_url, "GET", "/agents", token=token)
    if code != 200:
        raise RuntimeError(f"list agents failed: status={code}, payload={payload}")
    return _extract_items(payload)


def _handle_event(base_url: str, agent_id: str, event: dict[str, Any]) -> dict[str, Any]:
    token = _ensure_token(base_url, agent_id)
    _activate_if_needed(base_url, agent_id, token)

    event_type = str(event.get("eventType", ""))
    entity_id = str(event.get("entityId", ""))
    result: dict[str, Any] = {
        "ok": True,
        "eventType": event_type,
        "entityId": entity_id,
        "action": "noop",
        "fetched": None,
    }

    if event_type in ("context.created", "context.updated"):
        item = _find_by_id(_list_contexts(base_url, token), "contextId", entity_id)
        result["action"] = "fetch_context"
        result["fetched"] = {"kind": "context", "item": item}
        return result

    if event_type in ("vote.created", "vote.updated"):
        item = _find_by_id(_list_votes(base_url, token), "voteId", entity_id)
        result["action"] = "fetch_vote"
        result["fetched"] = {"kind": "vote", "item": item}
        return result

    if event_type in ("agent.registered", "agent.status.changed", "agent.connection.changed"):
        item = _find_by_id(_list_agents(base_url, token), "agentId", entity_id)
        result["action"] = "fetch_agent"
        result["fetched"] = {"kind": "agent", "item": item}
        return result

    if event_type in ("context.deleted", "vote.deleted", "agent.unregistered"):
        result["action"] = "entity_deleted"
        return result

    return result


def _print_json(obj: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(obj, ensure_ascii=True))
    sys.stdout.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Context Book deterministic skill helper")
    parser.add_argument("--base-url", required=True, help="Context Book base URL")
    parser.add_argument("--agent-id", required=True, help="Agent ID used for connect/register")
    parser.add_argument(
        "--token",
        default="",
        help="Optional bearer token; if omitted, script runs bootstrap connect/init/status/complete flow",
    )

    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list-contexts")
    sub.add_parser("list-votes")
    sub.add_parser("list-agents")

    get_ctx = sub.add_parser("get-context")
    get_ctx.add_argument("--context-id", required=True)

    get_vote = sub.add_parser("get-vote")
    get_vote.add_argument("--vote-id", required=True)

    ev = sub.add_parser("handle-event")
    ev.add_argument("--event-json", default="-", help="Event JSON or '-' for stdin")

    args = parser.parse_args()

    try:
        token = args.token or _ensure_token(args.base_url, args.agent_id)
        _activate_if_needed(args.base_url, args.agent_id, token)

        if args.command == "list-contexts":
            _print_json({"ok": True, "items": _list_contexts(args.base_url, token)})
            return 0
        if args.command == "list-votes":
            _print_json({"ok": True, "items": _list_votes(args.base_url, token)})
            return 0
        if args.command == "list-agents":
            _print_json({"ok": True, "items": _list_agents(args.base_url, token)})
            return 0
        if args.command == "get-context":
            item = _find_by_id(_list_contexts(args.base_url, token), "contextId", args.context_id)
            _print_json({"ok": True, "item": item})
            return 0
        if args.command == "get-vote":
            item = _find_by_id(_list_votes(args.base_url, token), "voteId", args.vote_id)
            _print_json({"ok": True, "item": item})
            return 0
        if args.command == "handle-event":
            if args.event_json == "-":
                raw = sys.stdin.read()
            else:
                raw = args.event_json
            event = json.loads(raw)
            if not isinstance(event, dict):
                raise RuntimeError("event JSON must be an object")
            _print_json(_handle_event(args.base_url, args.agent_id, event))
            return 0

        raise RuntimeError(f"unsupported command: {args.command}")
    except Exception as e:
        _print_json({"ok": False, "error": str(e)})
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
