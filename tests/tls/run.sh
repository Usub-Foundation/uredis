#!/usr/bin/env bash
# Runs tests/tls/tls_test.cpp against a throw-away Redis with TLS in Docker.
#   tests/tls/run.sh <path-to-uredis_tls_test-binary>
# Generates a CA, a server certificate for "redis.test" (+ 127.0.0.1) and a client certificate.
set -euo pipefail
BIN=${1:?usage: run.sh <uredis_tls_test binary>}
IMG=${UREDIS_TEST_REDIS_IMAGE:-redis:7.2-alpine}
NAME=uredis-tls-test
PLAIN_PORT=${UREDIS_TEST_PLAIN_PORT:-16379}
TLS_PORT=${UREDIS_TEST_TLS_PORT:-16380}
D=$(mktemp -d)
cleanup() { docker rm -f $NAME >/dev/null 2>&1 || true; find "$D" -mindepth 1 -delete; rmdir "$D"; }
trap cleanup EXIT

openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=uredis test CA" \
  -keyout "$D/ca.key" -out "$D/ca.crt" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -subj "/CN=redis.test" -keyout "$D/server.key" -out "$D/server.csr" >/dev/null 2>&1
printf 'subjectAltName=DNS:redis.test,IP:127.0.0.1\n' > "$D/san.ext"
openssl x509 -req -in "$D/server.csr" -CA "$D/ca.crt" -CAkey "$D/ca.key" -CAcreateserial -days 2 \
  -extfile "$D/san.ext" -out "$D/server.crt" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -subj "/CN=uredis client" -keyout "$D/client.key" -out "$D/client.csr" >/dev/null 2>&1
openssl x509 -req -in "$D/client.csr" -CA "$D/ca.crt" -CAkey "$D/ca.key" -CAcreateserial -days 2 \
  -out "$D/client.crt" >/dev/null 2>&1
chmod 755 "$D"; chmod 644 "$D"/*   # redis in the container runs as its own user

docker rm -f $NAME >/dev/null 2>&1 || true
docker run -d --name $NAME -p 127.0.0.1:$PLAIN_PORT:6379 -p 127.0.0.1:$TLS_PORT:6380 -v "$D:/tls:ro" "$IMG" \
  redis-server --save "" --appendonly no --port 6379 --tls-port 6380 \
  --tls-cert-file /tls/server.crt --tls-key-file /tls/server.key --tls-ca-cert-file /tls/ca.crt \
  --tls-auth-clients optional >/dev/null
for _ in $(seq 1 50); do docker exec $NAME redis-cli ping >/dev/null 2>&1 && break; sleep 0.2; done
docker exec $NAME redis-cli ping >/dev/null || { docker logs $NAME | tail -20; exit 1; }

UREDIS_TEST_HOST=127.0.0.1 UREDIS_TEST_PLAIN_PORT=$PLAIN_PORT UREDIS_TEST_TLS_PORT=$TLS_PORT \
UREDIS_TEST_CA="$D/ca.crt" UREDIS_TEST_SERVER_NAME=redis.test \
UREDIS_TEST_CLIENT_CERT="$D/client.crt" UREDIS_TEST_CLIENT_KEY="$D/client.key" \
  "$BIN"
