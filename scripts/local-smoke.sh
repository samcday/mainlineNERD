#!/usr/bin/env bash
# mainlineNERD smoke test: the real binary against a real, isolated Synapse.
#
# Used unchanged by CI (.github/workflows/smoke.yml). Needs bash, curl, jq,
# python3, sqlite3, openssl and docker or podman (CONTAINER_RUNTIME overrides
# autodetection). The container is loopback-only, named after this run's private
# directory, uses only a synthetic throwaway account, and is removed on exit.
# Run state lives in a private out/smoke-* directory: removed on success, kept on
# failure for debugging. No pre-existing path is ever deleted.
set -euo pipefail

# Pinned official Synapse image; the workflow pins the same version.
image=${SYNAPSE_IMAGE:-ghcr.io/element-hq/synapse:v1.162.0}
ready_timeout=${SMOKE_READY_TIMEOUT:-90}
if [ -n "${CONTAINER_RUNTIME:-}" ]; then
    runtime=$CONTAINER_RUNTIME
elif command -v docker >/dev/null; then
    runtime=docker
else
    runtime=podman
fi

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
bin=${BIN:-$root/target/debug/mainlinenerd}
mkdir -p "$root/out" # gitignored runtime root
data=$(mktemp -d "$root/out/smoke-XXXXXX") # private per run, never shared
name=mainlinenerd-$(basename "$data") # container named after this run's directory
port=${SMOKE_PORT:-$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')}
hs=http://127.0.0.1:$port
hs_url=$hs/ # as the URL parser normalises it
user=@smoke:smoke.localhost
device=MAINLINENERDSMOKE
# Synthetic throwaway password, generated per run and never written outside the
# ephemeral container/db (the Synapse data directory is deleted by cleanup too).
password=$(openssl rand -hex 16)

# Run the container as the invoking user so its state stays host-deletable: no
# root-owned files are left in out/. Rootless podman needs keep-id for the
# mapping to line up.
if [ "$runtime" = podman ]; then
    as_user=(--userns=keep-id --user "$(id -u):$(id -g)")
else
    as_user=(--user "$(id -u):$(id -g)")
fi

fail() { echo "FAIL: $*" >&2; exit 1; }
log() { printf '\n== %s\n' "$*"; }
# Synapse's own access-log lines for /sync, read through the container runtime.
sync_lines() { "$runtime" logs "$name" 2>&1 | grep -F '/_matrix/client/v3/sync?' || true; }
# Remove only what this run created: its container, plus its own private
# directory on success (kept on failure so the logs and db can be inspected).
cleanup() {
    status=$?
    "$runtime" rm -f "$name" >/dev/null 2>&1 || true
    if [ "$status" -eq 0 ]; then rm -rf "$data"; else echo "kept run state in $data" >&2; fi
}
trap cleanup EXIT

for cmd in "$runtime" curl jq python3 sqlite3 openssl cargo; do
    command -v "$cmd" >/dev/null || fail "$cmd is required"
done

log "building $bin (--locked)"
cargo build --locked --manifest-path "$root/Cargo.toml"

log "generating a fresh Synapse config in $data"
mkdir -p "$data/synapse"
"$runtime" run --rm -e SYNAPSE_SERVER_NAME=smoke.localhost -e SYNAPSE_REPORT_STATS=no \
    -v "$data/synapse:/data:Z" "${as_user[@]}" "$image" generate >/dev/null

log "starting $name ($image) on $hs"
"$runtime" run -d --name "$name" -p "127.0.0.1:$port:8008" \
    -v "$data/synapse:/data:Z" "${as_user[@]}" "$image" >/dev/null

start=$SECONDS
deadline=$((start + ready_timeout))
until curl -fsS -o /dev/null "$hs/health" 2>/dev/null; do
    if [ "$SECONDS" -ge "$deadline" ]; then
        "$runtime" logs --tail 50 "$name" >&2 || true
        fail "Synapse did not become healthy within ${ready_timeout}s"
    fi
    sleep 1
done
log "Synapse is healthy after $((SECONDS - start))s"

log "provisioning synthetic account $user"
# The register helper runs inside the container, so it talks to Synapse's
# in-container port, not the loopback-published one.
"$runtime" exec "$name" register_new_matrix_user \
    -c /data/homeserver.yaml -u smoke -p "$password" --no-admin http://localhost:8008 >/dev/null

# The login body (which contains the password) is written to the ephemeral
# output directory instead of being passed on argv.
jq -cn --arg p "$password" --arg d "$device" \
    '{type: "m.login.password", identifier: {type: "m.id.user", user: "smoke"}, password: $p, device_id: $d}' \
    >"$data/login.json"
login=$(curl -fsS -X POST "$hs/_matrix/client/v3/login" -H 'Content-Type: application/json' \
    --data @"$data/login.json")
token=$(jq -er .access_token <<<"$login")
[ "$(jq -r .device_id <<<"$login")" = "$device" ] || fail "login did not return device $device"
rm -f "$data/login.json" # it held the throwaway password

log "happy path: verified /whoami, one initial sync, startup record written"
db=$data/startup.db
before_syncs=$(sync_lines | wc -l)
# Bogus proxies are scoped to this one process: the binary builds its HTTP client
# with no_proxy, so if it honoured them every request would go to a dead port and
# the check would fail. Fixture curl/container traffic is untouched.
bogus_proxy=(NO_PROXY= no_proxy=
    ALL_PROXY=http://127.0.0.1:1 all_proxy=http://127.0.0.1:1
    HTTP_PROXY=http://127.0.0.1:1 http_proxy=http://127.0.0.1:1
    HTTPS_PROXY=http://127.0.0.1:1 https_proxy=http://127.0.0.1:1)
env MATRIX_ACCESS_TOKEN="$token" "${bogus_proxy[@]}" "$bin" \
    --homeserver "$hs" --user "$user" --device "$device" --db "$db" | tee "$data/happy.log"
grep -Fq "startup-check ok homeserver=$hs_url user=$user device=$device " "$data/happy.log" \
    || fail "missing successful receipt"
[ "$(sqlite3 "$db" "select homeserver || ' ' || user_id from startup_state where id = 1")" = "$hs_url $user" ] \
    || fail "startup_state does not record the verified homeserver/user"
sqlite3 "$db" "select last_successful_startup from startup_state where id = 1" \
    | grep -Eq '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z$' || fail "startup time is not RFC 3339"
[ "$(sqlite3 "$db" "select count(*) from sqlite_master where type = 'table' and name != 'startup_state'")" = 0 ] \
    || fail "startup bookkeeping created unexpected tables"

# The receipt and the row above would also pass without a sync, so assert the
# sync itself from Synapse's access log: exactly one new, successful, initial
# /sync with presence offline (waiting briefly for the log line to be flushed).
deadline=$((SECONDS + 15))
while [ "$(sync_lines | wc -l)" -le "$before_syncs" ] && [ "$SECONDS" -lt "$deadline" ]; do sleep 1; done
mapfile -t syncs < <(sync_lines)
[ "$((${#syncs[@]} - before_syncs))" -eq 1 ] \
    || fail "expected exactly one /sync in Synapse's access log, saw $((${#syncs[@]} - before_syncs))"
sync_log=${syncs[$before_syncs]}
[[ $sync_log == *' 200 "GET /_matrix/client/v3/sync?'* ]] || fail "the initial /sync did not return 200: $sync_log"
[[ $sync_log == *set_presence=offline* ]] || fail "the initial /sync did not use presence offline: $sync_log"
[[ $sync_log != *since=* ]] || fail "the initial /sync used a since token: $sync_log"

log "negative: invalid token is rejected and leaves no startup record"
if MATRIX_ACCESS_TOKEN="syt_invalid_$(openssl rand -hex 8)" "$bin" \
    --homeserver "$hs" --user "$user" --device "$device" --db "$data/bad-token.db" \
    >"$data/bad-token.out" 2>"$data/bad-token.err"; then
    fail "an invalid token was accepted"
fi
[ -s "$data/bad-token.err" ] && [ ! -e "$data/bad-token.db" ] \
    || fail "invalid token did not fail cleanly or left a startup record"

log "negative: wrong user / wrong device are rejected and leave no startup record"
if MATRIX_ACCESS_TOKEN="$token" "$bin" \
    --homeserver "$hs" --user "@nobody:smoke.localhost" --device "$device" --db "$data/mismatch-user.db" \
    >"$data/mismatch-user.out" 2>"$data/mismatch-user.err"; then
    fail "a mismatched user was accepted"
fi
if MATRIX_ACCESS_TOKEN="$token" "$bin" \
    --homeserver "$hs" --user "$user" --device WRONGDEVICE --db "$data/mismatch-device.db" \
    >"$data/mismatch-device.out" 2>"$data/mismatch-device.err"; then
    fail "a mismatched device was accepted"
fi
grep -q 'identity mismatch' "$data/mismatch-user.err" || fail "wrong user did not report an identity mismatch"
grep -q 'device mismatch' "$data/mismatch-device.err" || fail "wrong device did not report a device mismatch"
[ ! -e "$data/mismatch-user.db" ] && [ ! -e "$data/mismatch-device.db" ] \
    || fail "a mismatched identity left a startup record"
[ "$(sync_lines | wc -l)" -eq "$((before_syncs + 1))" ] || fail "a rejected run made /sync requests"

log "ok: $image passed the startup-check smoke test"
