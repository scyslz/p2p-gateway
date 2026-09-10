#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
echo "== vet =="
go vet ./... 2>&1 | head -n 20 || true
node --check web/files/client.js && node --check web/files/p2p-sw.js && echo "syntax_ok"
echo "== build =="
go build -o /tmp/p2p-gateway-bin ./cmd/gateway && echo "BUILD_OK $(ls -lh /tmp/p2p-gateway-bin | awk '{print $5}')"
echo "== restart =="
pkill -f p2p-gateway-bin 2>/dev/null || true
sleep 1
nohup /tmp/p2p-gateway-bin ./config.yaml > /tmp/p2p-gateway.log 2>&1 & echo $!
sleep 1
cat /tmp/p2p-gateway.log | head -n 3
ss -tln 2>&1 | grep -q 62057 && echo "LISTEN_OK :62057" || echo "NOT_LISTEN"
curl -s -o /dev/null -w "%{http_code} %{redirect_url}\n" http://127.0.0.1:62057/p2p/ 2>&1 | head -n 2
echo "done"
