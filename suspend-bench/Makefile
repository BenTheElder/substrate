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

# The harness targets Linux/KVM on the bench VM; build for linux/amd64 so it can
# be developed/cross-compiled from any host and scp'd to bench-host-n2.
GOOS   ?= linux
GOARCH ?= amd64
BIN    ?= bin

.PHONY: all build bench pinger fmt vet test clean smoke

all: build

build: bench pinger

bench:
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -o $(BIN)/bench ./cmd/bench

pinger:
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -o $(BIN)/pinger ./cmd/pinger

fmt:
	gofmt -s -w .

vet:
	GOOS=$(GOOS) GOARCH=$(GOARCH) go vet ./...

test:
	GOOS=$(GOOS) GOARCH=$(GOARCH) go test ./...

# Run the per-mechanism smoke checks on the bench VM (must run on Linux w/ KVM).
smoke: build
	sudo $(BIN)/bench --smoke

clean:
	rm -rf $(BIN)
