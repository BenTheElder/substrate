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

# Build a fully static workload binary so the rootfs can be from-scratch (the
# guest boots straight into /sbin/init -> /workload with no libc present).
FROM debian:13-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*
COPY main.c /src/main.c
RUN gcc -O2 -Wall -Wextra -static -o /workload /src/main.c

# Minimal image: just the static workload at /workload. build-rootfs.sh adds the
# init shim as /sbin/init.
FROM scratch
COPY --from=build /workload /workload
ENTRYPOINT ["/workload"]
