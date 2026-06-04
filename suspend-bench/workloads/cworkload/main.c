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
// C micro-workload for the suspend/resume benchmark. It gives the harness exact
// control over the guest's dirty working set, and a cheap liveness probe plus a
// verifiable state token so we can prove memory survived suspend/resume.
//
// It listens on AF_VSOCK port 1234 and speaks a newline-delimited text protocol.
// The harness connects through cloud-hypervisor's vsock unix socket.
//
// Commands (one per line):
//   PING            -> "PONG seed=<hex16> counter=<dec>\n"   (cheap; the timed probe)
//   SETWS <bytes>   -> (re)allocate the region to <bytes>, fill from PRNG(seed),
//                      touch every page -> "OK\n"            (set the working set)
//   DIRTY <bytes>   -> write into the first <bytes> (one store per page),
//                      counter++ -> "OK\n"                   (make pages dirty again)
//   WALK            -> read one word of every page (forces fault-in under ondemand
//                      restore / swap-in) -> "OK <sum_hex>\n"
//   HASH            -> "HASH <hex16>\n"  FNV-1a over the whole region (correctness)
//   READY           -> "READY\n"                            (ready-to-suspend marker)
//   QUIT            -> close + exit
//
// After a disconnect it loops back to accept(), so the harness can reconnect
// after a restore (a fresh VMM) or after resume — which is exactly what
// resume-to-first-ping measures.
//
// Transport: with no args it binds AF_VSOCK port 1234 (the cloud-hypervisor path).
// With one arg it binds an AF_UNIX stream socket at that path instead — used for
// the gVisor/runsc path, where the bundle bind-mounts the socket dir to the host
// and gVisor does not provide AF_VSOCK.

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <linux/vm_sockets.h>

#define PORT 1234
#define PAGE 4096

// PRNG: splitmix64, seeded once at startup. The seed lives in guest memory, so it
// (and everything derived from it) must survive suspend/resume unchanged.
static uint64_t g_seed = 0x0123456789ABCDEFULL;
static uint64_t g_counter = 0;
static uint8_t *g_region = NULL;
static size_t g_region_len = 0;

static uint64_t splitmix64(uint64_t *s) {
  uint64_t z = (*s += 0x9E3779B97F4A7C15ULL);
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
  return z ^ (z >> 31);
}

// set_ws (re)sizes the region and fills it from PRNG(seed), touching every page.
static int set_ws(size_t bytes) {
  if (g_region) {
    munmap(g_region, g_region_len);
    g_region = NULL;
    g_region_len = 0;
  }
  if (bytes == 0) return 0;
  void *p = mmap(NULL, bytes, PROT_READ | PROT_WRITE,
                 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (p == MAP_FAILED) return -1;
  g_region = (uint8_t *)p;
  g_region_len = bytes;
  uint64_t s = g_seed;
  uint64_t *w = (uint64_t *)g_region;
  size_t words = bytes / sizeof(uint64_t);
  for (size_t i = 0; i < words; i++) w[i] = splitmix64(&s);
  return 0;
}

// dirty writes one store into the first word of each page across the first
// `bytes` of the region, marking those pages dirty, and bumps the counter.
static void dirty(size_t bytes) {
  if (!g_region) return;
  if (bytes > g_region_len) bytes = g_region_len;
  g_counter++;
  for (size_t off = 0; off < bytes; off += PAGE) {
    *(volatile uint64_t *)(g_region + off) = g_counter;
  }
}

// walk reads one word of every page, returning a checksum; this forces every
// page to be present (the key cost ondemand restore / swap defer).
static uint64_t walk(void) {
  uint64_t sum = 0;
  for (size_t off = 0; off < g_region_len; off += PAGE) {
    sum += *(volatile uint64_t *)(g_region + off);
  }
  return sum;
}

static uint64_t fnv1a(void) {
  uint64_t h = 1469598103934665603ULL;
  for (size_t i = 0; i < g_region_len; i++) {
    h ^= g_region[i];
    h *= 1099511628211ULL;
  }
  return h;
}

// handle processes one command line and writes a response to fd.
static void handle(int fd, char *line) {
  char out[128];
  if (strncmp(line, "PING", 4) == 0) {
    int n = snprintf(out, sizeof(out), "PONG seed=%016llx counter=%llu\n",
                     (unsigned long long)g_seed, (unsigned long long)g_counter);
    write(fd, out, n);
  } else if (strncmp(line, "SETWS ", 6) == 0) {
    size_t bytes = strtoull(line + 6, NULL, 10);
    const char *r = set_ws(bytes) == 0 ? "OK\n" : "ERR setws\n";
    write(fd, r, strlen(r));
  } else if (strncmp(line, "DIRTY ", 6) == 0) {
    dirty(strtoull(line + 6, NULL, 10));
    write(fd, "OK\n", 3);
  } else if (strncmp(line, "WALK", 4) == 0) {
    int n = snprintf(out, sizeof(out), "OK %016llx\n",
                     (unsigned long long)walk());
    write(fd, out, n);
  } else if (strncmp(line, "HASH", 4) == 0) {
    int n = snprintf(out, sizeof(out), "HASH %016llx\n",
                     (unsigned long long)fnv1a());
    write(fd, out, n);
  } else if (strncmp(line, "READY", 5) == 0) {
    write(fd, "READY\n", 6);
  } else if (strncmp(line, "QUIT", 4) == 0) {
    write(fd, "BYE\n", 4);
    close(fd);
    exit(0);
  } else {
    write(fd, "ERR unknown\n", 12);
  }
}

// serve reads newline-delimited commands from one connection until it closes.
static void serve(int fd) {
  char buf[256];
  size_t len = 0;
  for (;;) {
    ssize_t r = read(fd, buf + len, sizeof(buf) - len - 1);
    if (r <= 0) return;
    len += (size_t)r;
    buf[len] = '\0';
    char *nl;
    while ((nl = memchr(buf, '\n', len)) != NULL) {
      *nl = '\0';
      handle(fd, buf);
      size_t consumed = (size_t)(nl - buf) + 1;
      memmove(buf, nl + 1, len - consumed);
      len -= consumed;
    }
    if (len >= sizeof(buf) - 1) len = 0;  // overlong line: drop
  }
}

// listen_vsock binds AF_VSOCK port PORT (cloud-hypervisor path).
static int listen_vsock(void) {
  int s = socket(AF_VSOCK, SOCK_STREAM, 0);
  if (s < 0) {
    perror("socket(AF_VSOCK)");
    return -1;
  }
  struct sockaddr_vm addr;
  memset(&addr, 0, sizeof(addr));
  addr.svm_family = AF_VSOCK;
  addr.svm_cid = VMADDR_CID_ANY;
  addr.svm_port = PORT;
  if (bind(s, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
    perror("bind(AF_VSOCK)");
    return -1;
  }
  if (listen(s, 4) != 0) {
    perror("listen");
    return -1;
  }
  fprintf(stderr, "[cworkload] listening on vsock port %d\n", PORT);
  return s;
}

// listen_unix binds an AF_UNIX stream socket at path (gVisor path).
static int listen_unix(const char *path) {
  int s = socket(AF_UNIX, SOCK_STREAM, 0);
  if (s < 0) {
    perror("socket(AF_UNIX)");
    return -1;
  }
  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);
  unlink(path);
  if (bind(s, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
    perror("bind(AF_UNIX)");
    return -1;
  }
  if (listen(s, 4) != 0) {
    perror("listen");
    return -1;
  }
  fprintf(stderr, "[cworkload] listening on unix socket %s\n", path);
  return s;
}

int main(int argc, char **argv) {
  setvbuf(stdout, NULL, _IONBF, 0);
  int s = (argc > 1) ? listen_unix(argv[1]) : listen_vsock();
  if (s < 0) return 1;
  for (;;) {
    int c = accept(s, NULL, NULL);
    if (c < 0) {
      if (errno == EINTR) continue;
      perror("accept");
      return 1;
    }
    serve(c);
    close(c);
  }
}
