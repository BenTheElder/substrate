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
# Entry point for the realistic Node workload: start the Vite dev server (the app
# a Claude agent is iterating on), then the control server. Two transports:
#   - cloud-hypervisor (no arg): control binds a unix socket and socat bridges
#     guest vsock port 1234 to it (Node has no native AF_VSOCK).
#   - gVisor (arg = bind-mounted unix socket path): control binds it directly.
set -e

# Start the Vite dev server in the background (warm module graph + file watcher).
( cd /app && exec node node_modules/vite/bin/vite.js --host 127.0.0.1 --port 5173 ) >/tmp/vite.log 2>&1 &

if [ -n "$1" ]; then
  exec node /control.js "$1"   # gVisor: bind the given unix socket directly
fi

SOCK=/run/node.sock
node /control.js "$SOCK" &
for _ in $(seq 1 600); do [ -S "$SOCK" ] && break; sleep 0.05; done
exec socat VSOCK-LISTEN:1234,reuseaddr,fork "UNIX-CONNECT:$SOCK"
