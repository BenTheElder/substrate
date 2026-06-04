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

# Realistic Node workload: a Vite + React dev server (the app a Claude agent is
# iterating on) plus a control server speaking the harness protocol. socat
# bridges vsock<->unix for the cloud-hypervisor path.
FROM node:22-bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends socat \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY app/package.json /app/package.json
RUN npm install --no-audit --no-fund --loglevel=error
COPY app/ /app/

# Generate a non-trivial component graph for Vite to keep warm.
COPY gen-app.js /gen-app.js
RUN node /gen-app.js /app 30

COPY control.js /control.js
COPY workload.sh /workload
RUN chmod 0755 /workload
ENTRYPOINT ["/workload"]
