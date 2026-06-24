// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kata

import (
	"fmt"
	"regexp"
	"strconv"
)

// ParseMemoryVCPUs extracts default_memory (MiB) and default_vcpus from a kata
// configuration.toml, returning guest RAM in bytes and the vCPU count. ateom needs
// these to build the CH VmConfig itself when it owns the cold boot (the kata shim
// would otherwise read them from the config). A non-positive vcpu count (kata's
// "use all host CPUs" sentinel) is clamped to 1.
func ParseMemoryVCPUs(base []byte) (memBytes int64, vcpus int, err error) {
	memRe := regexp.MustCompile(`(?m)^\s*default_memory\s*=\s*(\d+)`)
	vcpuRe := regexp.MustCompile(`(?m)^\s*default_vcpus\s*=\s*(\d+)`)
	m := memRe.FindSubmatch(base)
	if m == nil {
		return 0, 0, fmt.Errorf("default_memory not found in kata config")
	}
	mib, err := strconv.ParseInt(string(m[1]), 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parsing default_memory: %w", err)
	}
	v := vcpuRe.FindSubmatch(base)
	if v == nil {
		return 0, 0, fmt.Errorf("default_vcpus not found in kata config")
	}
	nv, err := strconv.Atoi(string(v[1]))
	if err != nil {
		return 0, 0, fmt.Errorf("parsing default_vcpus: %w", err)
	}
	if nv < 1 {
		nv = 1
	}
	return mib << 20, nv, nil
}

// ConfigAssets are the runtime-fetched asset paths to splice into a kata
// configuration.toml so the worker image needs no baked /opt/kata. Each is an
// absolute on-node path (content-addressed under the static-files dir, like runsc).
type ConfigAssets struct {
	// Kernel is the guest kernel (vmlinux) path.
	Kernel string
	// Image is the guest OS rootfs image path.
	Image string
	// Hypervisor is the cloud-hypervisor binary path.
	Hypervisor string
	// Virtiofsd is the virtiofsd binary path (needs find-paths migration
	// support, >= 1.13; kata bundles an older build without it).
	Virtiofsd string
}

// configField is a single TOML key whose value we rewrite to a fetched path.
type configField struct {
	key   string
	value func(ConfigAssets) string
}

// pathFields are the asset-path keys in a clh configuration.toml. We rewrite
// each (and its valid_* allowlist) to the fetched path. Empirically validated:
// a stock config with only these lines rewritten boots a VM via KATA_CONF_FILE.
var pathFields = []configField{
	{"kernel", func(a ConfigAssets) string { return quote(a.Kernel) }},
	{"image", func(a ConfigAssets) string { return quote(a.Image) }},
	{"path", func(a ConfigAssets) string { return quote(a.Hypervisor) }},
	{"valid_hypervisor_paths", func(a ConfigAssets) string { return list(a.Hypervisor) }},
	{"virtio_fs_daemon", func(a ConfigAssets) string { return quote(a.Virtiofsd) }},
	{"valid_virtio_fs_daemon_paths", func(a ConfigAssets) string { return list(a.Virtiofsd) }},
}

func quote(s string) string { return `"` + s + `"` }
func list(s string) string  { return `["` + s + `"]` }

// EnableDebug turns on kata's debug knobs in a configuration.toml: it uncomments
// every `#enable_debug = true` (hypervisor/agent/runtime) and appends
// `agent.log=debug` to the hypervisor kernel_params so the guest kata-agent emits
// debug-level logs (with the failing path on errors) over its vsock log channel,
// which the shim relays to our log fifo -> pod logs. POC diagnostic aid.
func EnableDebug(base []byte) []byte {
	out := base
	// Uncomment all `#enable_debug = true` lines.
	reDbg := regexp.MustCompile(`(?m)^(\s*)#\s*enable_debug\s*=\s*true\s*$`)
	out = reDbg.ReplaceAll(out, []byte("${1}enable_debug = true"))
	// Append agent.log=debug to kernel_params (only if not already present).
	reKP := regexp.MustCompile(`(?m)^(\s*kernel_params\s*=\s*")([^"]*)(".*)$`)
	out = reKP.ReplaceAllFunc(out, func(line []byte) []byte {
		m := reKP.FindSubmatch(line)
		if m == nil {
			return line
		}
		existing := string(m[2])
		if regexpContains(existing, "agent.log=") {
			return line
		}
		val := "agent.log=debug agent.debug_console"
		if existing != "" {
			val = existing + " " + val
		}
		return []byte(string(m[1]) + val + string(m[3]))
	})
	return out
}

func regexpContains(s, sub string) bool {
	return regexp.MustCompile(regexp.QuoteMeta(sub)).MatchString(s)
}

// EnableReclaimGuestFreedMemory sets `reclaim_guest_freed_memory = true` in the
// clh hypervisor config, which makes kata create the CH guest with a virtio-balloon
// + free-page-reporting (clh.go NewBalloonConfig(0)+SetFreePageReporting(true)).
// ateom then drives that balloon (vm.resize) before snapshot to free guest pages
// so the sparse image shrinks toward the live working set (gVisor-parity). It
// flips an existing (commented or `= false`) line, or inserts the key under
// [hypervisor.clh] if absent. Errors only if neither the key nor the section is
// found (so we fail loudly rather than silently ship a balloon-less config).
func EnableReclaimGuestFreedMemory(base []byte) ([]byte, error) {
	re := regexp.MustCompile(`(?m)^(\s*)#?\s*reclaim_guest_freed_memory\s*=\s*(?:true|false)\s*$`)
	if re.Match(base) {
		return re.ReplaceAll(base, []byte("${1}reclaim_guest_freed_memory = true")), nil
	}
	sec := regexp.MustCompile(`(?m)^(\[hypervisor\.clh\]\s*)$`)
	if sec.Match(base) {
		return sec.ReplaceAll(base, []byte("${1}\nreclaim_guest_freed_memory = true")), nil
	}
	return nil, fmt.Errorf("EnableReclaimGuestFreedMemory: neither reclaim_guest_freed_memory key nor [hypervisor.clh] section found in config")
}

// EnableDebugConsole adds the kernel params so the guest kata-agent spawns a root
// debug shell on vsock port 1026, WITHOUT the verbose debug logging EnableDebug
// also turns on. ateom dials that console (kata.DebugConsoleDump) to run the
// in-guest reset-to-golden step (wipe the overlay tmpfs upper) at checkpoint.
//
// BOTH params are required: `agent.debug_console` enables the console, and
// `agent.debug_console_vport=1026` makes the agent bind it on the vsock port
// DebugConsoleDump connects to — the agent's console only binds a vsock port when
// the vport is >0 (console.rs `if port > 0`; default 0 = no vsock listener, which
// the kata RUNTIME normally avoids by injecting the vport itself). Idempotent.
func EnableDebugConsole(base []byte) []byte {
	reKP := regexp.MustCompile(`(?m)^(\s*kernel_params\s*=\s*")([^"]*)(".*)$`)
	return reKP.ReplaceAllFunc(base, func(line []byte) []byte {
		m := reKP.FindSubmatch(line)
		if m == nil {
			return line
		}
		existing := string(m[2])
		if regexpContains(existing, "agent.debug_console_vport") {
			return line
		}
		val := "agent.debug_console agent.debug_console_vport=1026"
		if existing != "" {
			val = existing + " " + val
		}
		return []byte(string(m[1]) + val + string(m[3]))
	})
}

// RenderConfig returns base (a kata configuration.toml) with the asset-path
// fields rewritten to point at a.* . The base config carries all the
// version-matched kata settings; we only override where the assets live, so the
// config stays in sync with the kata release (the base itself is a fetched asset).
//
// Each field must already be present in base (kata's stock clh config has them);
// a missing field is an error so we fail loudly rather than boot a half-configured VM.
func RenderConfig(base []byte, a ConfigAssets) ([]byte, error) {
	if a.Kernel == "" || a.Image == "" || a.Hypervisor == "" || a.Virtiofsd == "" {
		return nil, fmt.Errorf("RenderConfig: all of Kernel/Image/Hypervisor/Virtiofsd are required, got %+v", a)
	}
	out := base
	for _, f := range pathFields {
		// Match a top-level `key = <value>` line (TOML), preserving leading
		// whitespace. Anchored to line start in multiline mode.
		re := regexp.MustCompile(`(?m)^(\s*` + regexp.QuoteMeta(f.key) + `\s*=\s*).*$`)
		if !re.Match(out) {
			return nil, fmt.Errorf("RenderConfig: key %q not found in base config", f.key)
		}
		out = re.ReplaceAll(out, []byte("${1}"+f.value(a)))
	}
	return out, nil
}
