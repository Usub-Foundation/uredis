#!/usr/bin/env bash
# Runs tests/cluster/reconnect_test.cpp against a throw-away single-node Redis Cluster in Docker.
#   tests/cluster/run.sh <path-to-uredis_cluster_reconnect_test-binary>
# Nothing listens on the port when the test starts. The test asks for actions by writing
# req.N (start | pause | unpause) into the control directory; this script performs the action
# and answers with ack.N.
set -euo pipefail
BIN=${1:?usage: run.sh <uredis_cluster_reconnect_test binary>}
IMG=${UREDIS_TEST_REDIS_IMAGE:-redis:7.2-alpine}
PORT=${UREDIS_TEST_CLUSTER_PORT:-16390}
readonly NAME=uredis-cluster-reconnect-test
readonly CTL=/tmp/uredis-cluster-reconnect-ctl

# Fixed names only: nothing here depends on a variable that could change later.
cleanup() {
  docker unpause uredis-cluster-reconnect-test >/dev/null 2>&1 || true
  docker rm -f uredis-cluster-reconnect-test >/dev/null 2>&1 || true
  if [ -d /tmp/uredis-cluster-reconnect-ctl ]; then
    find /tmp/uredis-cluster-reconnect-ctl -mindepth 1 -delete
    rmdir /tmp/uredis-cluster-reconnect-ctl
  fi
}
trap cleanup EXIT

docker rm -f $NAME >/dev/null 2>&1 || true
if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
  echo "port $PORT is already in use; set UREDIS_TEST_CLUSTER_PORT" >&2
  exit 1
fi
mkdir -p /tmp/uredis-cluster-reconnect-ctl
find /tmp/uredis-cluster-reconnect-ctl -mindepth 1 -delete

start_redis() {
  docker rm -f $NAME >/dev/null 2>&1 || true
  # One node owning all 16384 slots; it announces the published address so CLUSTER SLOTS
  # points the client back at 127.0.0.1:$PORT.
  docker run -d --name $NAME -p "127.0.0.1:$PORT:6379" "$IMG" \
    redis-server --save "" --appendonly no \
    --cluster-enabled yes --cluster-config-file nodes.conf \
    --cluster-announce-ip 127.0.0.1 --cluster-announce-port "$PORT" >/dev/null
  for _ in $(seq 1 50); do docker exec $NAME redis-cli ping >/dev/null 2>&1 && break; sleep 0.2; done
  docker exec $NAME redis-cli cluster addslotsrange 0 16383 >/dev/null
  for _ in $(seq 1 50); do
    docker exec $NAME redis-cli cluster info 2>/dev/null | grep -q 'cluster_state:ok' && return 0
    sleep 0.2
  done
  docker logs $NAME | tail -20
  return 1
}

UREDIS_TEST_HOST=127.0.0.1 UREDIS_TEST_PORT=$PORT UREDIS_TEST_CTL=$CTL "$BIN" &
TEST_PID=$!

n=1
while kill -0 "$TEST_PID" 2>/dev/null; do
  if [ -e "$CTL/req.$n" ]; then
    action=$(cat "$CTL/req.$n")
    case "$action" in
      start) start_redis ;;
      pause) docker pause $NAME >/dev/null ;;
      unpause) docker unpause $NAME >/dev/null ;;
      *) echo "run.sh: unknown action '$action'" >&2; exit 1 ;;
    esac
    : > "$CTL/ack.$n"
    n=$((n + 1))
  else
    sleep 0.05
  fi
done
wait "$TEST_PID"
