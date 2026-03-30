#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
BOOTSTRAP_SECRET="${CONTEXT_BOOK_BOOTSTRAP_SHARED_SECRET:-none}"
AGENT_ID="agent-smoke-$(date +%s)"

init_json=$(curl -sS -X POST "$BASE_URL/bootstrap/register/init" \
  -H 'content-type: application/json' \
  -H "X-Context-Book-Bootstrap-Secret: $BOOTSTRAP_SECRET" \
  -d "{\"agentName\":\"$AGENT_ID\",\"deviceType\":\"android_mobile\"}")

echo "bootstrap init: $init_json"

request_id=$(printf '%s' "$init_json" | sed -n 's/.*"requestId":"\([^"]*\)".*/\1/p')
wait_token=$(printf '%s' "$init_json" | sed -n 's/.*"waitToken":"\([^"]*\)".*/\1/p')
if [[ -z "$request_id" || -z "$wait_token" ]]; then
  echo "failed to parse bootstrap request id or wait token" >&2
  exit 1
fi

approve_json=$(curl -sS -X POST "$BASE_URL/dashboard/api/bootstrap/requests/$request_id/approve" \
  -H 'content-type: application/json' \
  -d '{"actor":"smoke-test","reason":"skill smoke test approval"}')

echo "approve: $approve_json"

complete_json=$(curl -sS -X POST "$BASE_URL/bootstrap/register/complete" \
  -H 'content-type: application/json' \
  -H "X-Context-Book-Bootstrap-Wait-Token: $wait_token" \
  -d "{\"requestId\":\"$request_id\"}")

echo "complete: $complete_json"

registered_agent_id=$(printf '%s' "$complete_json" | sed -n 's/.*"agentId":"\([^"]*\)".*/\1/p')
access_token=$(printf '%s' "$complete_json" | sed -n 's/.*"accessToken":"\([^"]*\)".*/\1/p')
if [[ -z "$registered_agent_id" || -z "$access_token" ]]; then
  echo "failed to parse agent id or access token from bootstrap complete" >&2
  exit 1
fi

status_json=$(curl -sS -X PATCH "$BASE_URL/agents/$registered_agent_id/status" \
  -H "authorization: Bearer $access_token" \
  -H 'content-type: application/json' \
  -d '{"status":"Active"}')

echo "status: $status_json"

echo '{"eventType":"context.updated","entityId":"android_mobile_ctx_1"}' \
  | python3 "$(dirname "$0")/context_book_skill.py" \
    --base-url "$BASE_URL" \
    --agent-id "$registered_agent_id" \
    handle-event
