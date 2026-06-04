#!/bin/sh
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
#
# Entry point for the headless-Chrome workload: serve the mirrored site locally,
# launch headless Chrome against it (real renderer memory), then the control
# server. Two transports, mirroring the node workload:
#   - cloud-hypervisor (no arg): control binds a unix socket and socat bridges
#     guest vsock port 1234 to it.
#   - gVisor (arg = "tcp:PORT"): control listens on a netstack TCP socket.
set -e

export HOME=/tmp
CHROMIUM="${CHROMIUM:-chromium}"
SITE_DIR=/site
ENTRY="$(cat "${SITE_DIR}/.entry" 2>/dev/null || (cd "${SITE_DIR}" && find . -name '*.html' | head -n1 | sed 's#^\./##'))"
URL="http://127.0.0.1:8080/${ENTRY}"

# 1. Serve the mirrored site on localhost.
node /static-server.js "${SITE_DIR}" 8080 >/tmp/static.log 2>&1 &

# 2. Launch headless Chrome with the DevTools endpoint, pointed at the page.
#    --no-sandbox: running as root with no user namespaces in the guest.
#    --disable-dev-shm-usage: /dev/shm is tiny in the minimal rootfs.
"${CHROMIUM}" \
  --headless=new --no-sandbox --disable-gpu --disable-dev-shm-usage \
  --no-first-run --no-default-browser-check --disable-extensions \
  --user-data-dir=/tmp/cr --disk-cache-dir=/tmp/cr/cache \
  --remote-debugging-address=127.0.0.1 --remote-debugging-port=9222 \
  "${URL}" >/tmp/chrome.log 2>&1 &

# 3. Control server speaking the harness protocol (drives Chrome via CDP).
if [ -n "$1" ]; then
  exec node /control.js "$1"   # gVisor: "tcp:PORT" netstack socket
fi

SOCK=/run/chrome.sock
node /control.js "$SOCK" &
for _ in $(seq 1 600); do [ -S "$SOCK" ] && break; sleep 0.05; done
exec socat VSOCK-LISTEN:1234,reuseaddr,fork "UNIX-CONNECT:$SOCK"
