// Standalone module for the sandbox suspend/resume benchmark spike.
//
// This is intentionally a SEPARATE Go module from the parent repo so it does
// not perturb the main module's go.mod / go.sum / vendor tree. It is throwaway
// research code (see ../docs/dev/poc2-swap-suspend-findings.md for context).
//
// It deliberately depends on nothing outside the standard library: vsock and
// cgroup/proc access are done via raw Linux syscalls, and the cloud-hypervisor
// REST API is spoken over a unix socket with net/http. That keeps the harness a
// single static binary you can scp to the bench VM with no go.sum to vendor.
module github.com/agent-substrate/substrate/suspend-bench

go 1.26
