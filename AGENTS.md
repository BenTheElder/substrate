# Agent Substrate

## Rules for contributors

- Read the "Using AI" section of `COLLABORATING.md`. You are responsible for everything your agent submits.
- **Be brief.** Code comments, commit messages, PR descriptions and issues should be short, plain English, and read the way an experienced maintainer writes by hand. Reviewer time and agent context windows are limited.
  - `COLLABORATING.md` doesn't allow AI-generated commit messages, because they have usually been low quality and unreviewed. If you are asked to write one anyway, hold it to the same standard, and the person submitting it must review it.
  - Write short, direct sentences with one idea each, subject first.
  - Use the real identifiers from the code. Don't coin new names, and avoid noun stacks like "the store-wide pin-lock pre-flight".
  - Prefer verbs to abstract nouns: "evicts" instead of "performs eviction".
  - Say each thing once. Don't restate the diff, open with "This PR …" or "This commit …", or repeat the same explanation in several comments.
  - Explain why only when the code doesn't make it obvious, in a clause or a line.
  - Avoid:
    - em-dashes, and chains of semicolons
    - headings, bold, or test plans in commit messages
    - epigrams and contrast pairs, like "X, not Y" or "So treat ENOENT as what it is."
    - numbers added for color, like "measured at a flat 51ms"
    - giving code motives or editorializing about it, like "wants", "decides", "is responsible for", "lands", "owns", or "X is what makes Y". Plain active voice is fine: "the reaper collects" is how Go doc comments read.
    - filler words: load-bearing, footgun, surfaced, robust, comprehensive, seamless, leverage
    - "No behavior change" unless the diff is a pure refactor
    - long sentences that pile up clauses, or put the subject after a long lead-in
- Comment the final state of the code. Bugs that only existed while you were writing the change, and links to planning docs outside the repository, are noise to the next reader.
- Test new behavior in ways that help future changes. Update tests when you change existing behavior. Don't add tests just for coverage. Every test costs CI time on every PR.
- Rebase onto upstream `main` before opening or updating a PR. This file may have changed there. Follow that version.

## Project Overview

Agent Substrate is a system built on top of Kubernetes which manages agent-like workloads to achieve higher scale and efficiency than Kubernetes alone can offer, with lower latency.
It takes the Kubernetes control-plane out of the critical path to achieve lower latency by mapping a larger set of “actors” (applications such as agents) onto a smaller set of ready “workers” (Kubernetes Pods).
Agent Substrate relies on the fact that agent-like applications tend to be idle most of the time to achieve heavy multiplexing.

For development, it's recommended to read the `README.md` and `CONTRIBUTING.md` in the root folder.
See `hack/install-ate.sh` and `tools/setup-gcp` for provisioning and deploying clusters and GCP resources.

## Repository Layout

```
cmd/          # One subdirectory per binary (ateapi, atelet, atenet, …)
internal/     # Shared packages, internal to this module only
pkg/          # Shared packages intended for external import
docs/         # Design docs and developer guides
hack/         # Dev/CI scripts and code generators
manifests/    # Kubernetes YAML for deploying Agent Substrate
demos/        # Self-contained example applications
benchmarking/ # Load-testing tools and workloads
tools/        # Standalone Go tools (go run ./tools/<name>) for Dev/CI
```

**Where to put new Go code, quick rules:**

| Situation | Location |
|---|---|
| Only used by one binary | `cmd/<binary>/internal/<pkg>` |
| Shared across binaries, not for external import | `internal/<pkg>` |
| Public API for external consumers | `pkg/<pkg>` |
| Public proto (control-plane gRPC API) | `pkg/proto/<name>` |
| Internal proto (atelet / ateworker) | `internal/proto/<name>` |
| Dev/CI scripts | `hack/` |
| Standalone Go dev/CI tools | `tools/<name>` with its own `go.mod` |
| Self-contained plugin that nothing in the repo depends on | `internal/plugins/<name>` with its own `go.mod`, importing only `pkg/`; see `internal/plugins/README.md` |

See `docs/dev/code-layout.md` for the full rationale and per-directory details.

## Build and Test Commands

The dev entry points are the scripts under `hack/` and the `cmd/ate-setup` installer.

### Building and Installing
- **Install from source**: `go run ./cmd/ate-setup deploy ate-system` (or the `hack/install-ate.sh` shim) builds every image with ko as part of the install; see `cmd/ate-setup/README.md`
- **Binaries**: `go build -o bin/kubectl-ate ./cmd/kubectl-ate`, `go build -o bin/ate-setup ./cmd/ate-setup`
- **Release images**: `VERSION=TAG go run ./cmd/ate-setup publish release-images --ko-docker-repo REPO` (every image a pre-built install needs, all tagged `TAG`, including the docker-built `envoy-dataplane`)

### Testing and Verification
- **Run Unit Tests**: `go test -race ./...`
- **Run E2E Tests**: `hack/run-e2e.sh` (Requires a cluster with Agent Substrate installed; `hack/run-e2e-kind.sh` for kind)
- **Run Linters and Verifiers**: `hack/verify-all.sh` (formatting, golangci-lint, boilerplate headers, licenses, go modules, generated code, and the metrics registry)

## Code Style Guidelines

- **Go Formatting**: Code must be formatted with `gofmt`. Run `hack/update/gofmt.sh` to automatically format all files before submitting changes.
- **Copyright Headers**: All files must contain appropriate copyright and license headers. See templates in `hack/boilerplate/`.
- **Modularity**: Submit small, focused Pull Requests that touch a limited part of the codebase for easier reviews and rebasing.
- **Go Modules**: Ensure `go.mod` is clean. Run `go mod tidy` if adding or removing dependencies.
- **Spelling**: American English. `golangci-lint` runs `misspell` with `locale: US`, so British spellings fail lint.

## Backward Compatibility

<!-- TODO #2004: Remove this section at the v1.0.0 release. -->

Agent Substrate makes no compatibility guarantee before v1.0.0. Do not add code to stay compatible with older clients, binaries, protos, flags, or config.
Remove or rename fields, RPCs, flags, and types outright and update every caller in the same change.
Do not add `reserved` statements to protos, deprecated aliases, fallbacks for old formats, or version negotiation.

## Commit Messages

- **Describe the change and why it was needed**: The message is read on `main` long after the PR branch is gone, so it should stand on its own.
- **No issue or PR references**: Leave out `#1234`, `Fixes #1234`, and GitHub URLs. GitHub renders them as cross-references on the linked thread, and rebases or force-pushes repeat them. Put that context in the pull request description instead, where it belongs to the review rather than to the permanent history.

## Metrics

`docs/metrics/registry/` is an [OpenTelemetry Weaver](https://github.com/open-telemetry/weaver) registry. `metrics.yaml` defines every metric instrument the ate system components emit, plus every attribute any signal uses and the permitted values of each. `events.yaml` defines the log events emitted over OTLP. Read them to find an instrument, an event, or their attributes.

If you add or rename an instrument or an event, or add an attribute, follow `docs/dev/best-practices/metrics.md`, update the registry, and run `hack/verify/metrics.sh`. `hack/verify-all.sh` runs the same check. Weaver reads the whole directory, so a new file in it is checked straight away.

Some attributes are defined in the registry but barred from metric labels: actor identity is the main one. The `note` on each says so, and `cardinality_rules` in `docs/metrics/substrate.yaml` is where the bar lives.

`docs/metrics/substrate.yaml` holds the rules Weaver cannot express: the cardinality rules, the known exceptions, and the subsystems that emit no metrics. Read `blind_spots` before you attribute a fault to a component.

See the [metric registry](docs/observability.md#the-metric-registry) section of the observability guide.

## Testing Instructions

1. Write tests for new behavior. Update tests when you change existing behavior. Consider the value of tests for future changes.
2. Ensure changes do not break existing tests.
3. Run `go test -race ./...` and `hack/verify-all.sh` locally before requesting a code review to catch common issues like missed copyright headers or formatting drift.
4. For end-to-end tests involving the actual infrastructure, ensure you have a running cluster (setup via `hack/ate-dev-env.sh.example` and `go run ./tools/setup-gcp bootstrap`).
5. A test that skips when a precondition is missing (Docker, a cluster, an artifact directory) must **fail** on it in CI, because a skip and a pass are the same exit code. Check for CI with a named predicate like `cmd/ateapi/internal/store/dockerenv.Required()` instead of an inline `os.Getenv("CI")`. Make sure the CI branch actually fails before merging. See `docs/dev/best-practices/ci-fail-closed.md`.

## Security Considerations

The security story for Substrate is very early and many features are missing.
However! Take care to respect security best practices when writing code in order to improve Substrate's security over time.
The following is what Substrate currently offers.
Keep this up to date when updating AGENTS.md.

- **Workload Isolation**: The project uses `gVisor` (`runsc`) for sandboxing and security isolation of workloads on pods.
- **JWTs and JWKs**: Signing and JWK set encoding (`internal/localjwtauthority`) and verification (`internal/oidcjwt`) are built on the standard library's crypto packages. Extend those packages rather than adding go-jose or another JOSE library, so the code that decides whether a token is valid stays small and supports only the algorithms Substrate uses.
- **Ingress Authentication**: `atenet-router --ingress-auth-mode=static-mtls` requires clients to present a SPIFFE certificate signed by `--ingress-client-ca-file` whose ID is in `--ingress-allowed-spiffe-ids`; that list is the whole authorization policy, so any listed client may reach any Actor. The default, `deprecated-insecure`, checks a certificate only when a client presents one, and also serves plaintext listeners that accept any client. Install with it via `ate-setup --ingress-auth-mode=static-mtls` (`manifests/ate-install/components/router-static-mtls`); the allowlist lives in `manifests/ate-install/atenet-router.yaml`.
- **Control API Authorization**: `ate-api-server` checks every Control RPC against an embedded OpenFGA model (`cmd/ateapi/internal/authz/model.fga`), on by default. Every served unary RPC needs an entry in `defaultRPCPermissions` (`registry.go`): with enforcement on, an RPC without one is denied, and a test fails the build. Access is granted per global scope and atespace through AccessPolicy (`kubectl ate access-policy`); the install's bootstrap owner is the shared `ate-client` ServiceAccount that `kubectl ate` uses. See `docs/authorization.md` for roles and known gaps.

For future plans for security, reference `docs/roadmap.md`.
