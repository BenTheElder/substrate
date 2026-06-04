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
# Realistic headless-Chrome workload: a real webpage mirrored INTO the image at
# build time (served locally inside the guest, which has no external network) plus
# a control server that drives Chrome over the DevTools Protocol while speaking the
# harness protocol. The working set is REAL renderer memory (allocated/dirtied/
# verified inside the page via CDP), on top of the loaded page's own footprint.
#
# The mirrored site lives ONLY in the built image (fetched here, on the bench host
# which has egress) — it is never committed to the repo. Override SITE_URL to taste.
FROM node:22-bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      chromium ca-certificates fonts-liberation wget socat \
    && rm -rf /var/lib/apt/lists/*

# Mirror a real page + its requisites locally at build time (bounded: the page and
# its assets, no recursive crawl). Record the entry HTML path for the server.
ARG SITE_URL=https://en.wikipedia.org/wiki/Cloud_computing
RUN set -eux; \
    wget -e robots=off -nv --tries=2 --timeout=30 \
      --page-requisites --convert-links --adjust-extension --span-hosts \
      -P /site "${SITE_URL}" || true; \
    base="$(basename "${SITE_URL%%\?*}")"; \
    entry="$(cd /site && { find . -name '*.html' | grep -F "$base" || find . -name '*.html'; } | head -n1 | sed 's#^\./##')"; \
    test -n "$entry"; \
    printf '%s' "$entry" > /site/.entry; \
    echo "mirrored entry: $entry"

COPY static-server.js /static-server.js
COPY control.js /control.js
COPY workload.sh /workload
RUN chmod 0755 /workload
ENTRYPOINT ["/workload"]
