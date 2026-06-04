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
//
// Minimal static PID1 for the benchmark guest. It exists so the guest boots
// straight into the workload with no init system:
//   - mounts /proc, /sys, /dev (devtmpfs, which creates /dev/vsock), /tmp
//   - fork+exec the workload (default /workload, or `workload=PATH` on the kernel
//     cmdline)
//   - reaps zombies; if the workload itself exits, power the VM off so the harness
//     (which is driving cloud-hypervisor) sees the sandbox die instead of hanging.
//
// Build static so it runs in a from-scratch rootfs: see ../Makefile.

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mount.h>
#include <sys/wait.h>
#include <sys/reboot.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <net/if.h>
#include <linux/reboot.h>

// ifup_lo brings the loopback interface up. Without it, 127.0.0.1 is
// unreachable, so workloads that talk over localhost (e.g. the Node control
// server <-> Vite dev server) cannot connect. (The C workload uses vsock and
// doesn't need this, but it's harmless there.)
static void ifup_lo(void) {
  int fd = socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) return;
  struct ifreq ifr;
  memset(&ifr, 0, sizeof(ifr));
  strncpy(ifr.ifr_name, "lo", IFNAMSIZ - 1);
  if (ioctl(fd, SIOCGIFFLAGS, &ifr) == 0) {
    ifr.ifr_flags |= IFF_UP | IFF_RUNNING;
    if (ioctl(fd, SIOCSIFFLAGS, &ifr) != 0)
      fprintf(stderr, "[init] warn: bringing lo up failed: %s\n", strerror(errno));
  }
  close(fd);
}

static void mount_one(const char *src, const char *tgt, const char *fs,
                      unsigned long flags) {
  mkdir(tgt, 0755);
  if (mount(src, tgt, fs, flags, NULL) != 0) {
    // Non-fatal: the device may already be mounted, or the fs unavailable.
    fprintf(stderr, "[init] warn: mount %s on %s failed: %s\n", fs, tgt,
            strerror(errno));
  }
}

// workload_path returns the workload binary to exec, honoring `workload=PATH` on
// /proc/cmdline; defaults to "/workload".
static const char *workload_path(char *buf, size_t n) {
  const char *def = "/workload";
  int fd = open("/proc/cmdline", O_RDONLY);
  if (fd < 0) return def;
  char cmd[4096];
  ssize_t r = read(fd, cmd, sizeof(cmd) - 1);
  close(fd);
  if (r <= 0) return def;
  cmd[r] = '\0';
  char *p = strstr(cmd, "workload=");
  if (!p) return def;
  p += strlen("workload=");
  size_t i = 0;
  while (p[i] && p[i] != ' ' && p[i] != '\n' && i < n - 1) {
    buf[i] = p[i];
    i++;
  }
  if (i == 0) return def;
  buf[i] = '\0';
  return buf;
}

int main(void) {
  mount_one("proc", "/proc", "proc", 0);
  mount_one("sysfs", "/sys", "sysfs", 0);
  mount_one("devtmpfs", "/dev", "devtmpfs", 0);  // creates /dev/vsock
  mount_one("tmpfs", "/tmp", "tmpfs", 0);
  ifup_lo();  // bring up loopback so localhost works (Node/Vite path)

  char pathbuf[256];
  const char *wl = workload_path(pathbuf, sizeof(pathbuf));

  pid_t child = fork();
  if (child == 0) {
    char *argv[] = {(char *)wl, NULL};
    execv(wl, argv);
    fprintf(stderr, "[init] FATAL: execv(%s) failed: %s\n", wl, strerror(errno));
    _exit(127);
  }

  // PID1 reap loop. Power off when the tracked workload exits (clean signal to
  // the host that the sandbox is done / crashed).
  for (;;) {
    int status;
    pid_t done = waitpid(-1, &status, 0);
    if (done == child) {
      fprintf(stderr, "[init] workload exited (status=%d); powering off\n",
              status);
      sync();
      reboot(LINUX_REBOOT_CMD_POWER_OFF);
      _exit(0);
    }
    if (done < 0 && errno == ECHILD) {
      // No children left at all — shouldn't happen before the workload exits.
      pause();
    }
  }
}
