//go:build linux

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

package ateworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/compute/metadata"
	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/ateworker/actornet"
	"github.com/agent-substrate/substrate/internal/ateworker/cgroup"
	"github.com/agent-substrate/substrate/internal/ateworker/pid1"
	"github.com/agent-substrate/substrate/internal/ateworker/register"
	"github.com/agent-substrate/substrate/internal/ateworker/tunnel"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/otlprelay"
	"github.com/agent-substrate/substrate/internal/preview"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/version"
	"github.com/agent-substrate/substrate/internal/workerstats"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// minUsageSampleInterval is the floor of --usage-sample-interval, the same as
// atelet's poll interval floor. It keeps statsSweepBudget inside one interval.
const minUsageSampleInterval = 50 * time.Second

// flags are the flags every worker takes.
type flags struct {
	podUID                 *string
	tunnel                 *tunnel.Config
	readinessListenAddress *string
	maxActors              *int
	usageSampleInterval    *time.Duration
	showVersion            *bool
	logLevel               *string
	preview                *[]string
	otlpRelaySocket        *string
}

func registerFlags(fs *pflag.FlagSet) *flags {
	return &flags{
		podUID:                 fs.String("pod-uid", "", "The UID of the current pod"),
		tunnel:                 tunnel.RegisterFlags(fs),
		readinessListenAddress: fs.String("readiness-listen-address", "0.0.0.0:8080", "Address for the HTTP /readyz and /healthz probes"),
		maxActors:              fs.Int("max-actors", 1000, "How many actors this worker will host at once"),
		usageSampleInterval:    fs.Duration("usage-sample-interval", time.Minute, "How often the ateworker samples the resource usage of its actors. Each sample writes an ate.actor.usage_sampled record per actor, and GetActiveWorkloadStats serves the latest."),
		showVersion:            fs.Bool("version", false, "Print version and exit."),
		logLevel:               fs.String("log-level", "info", "Minimum log level: debug, info, warn, or error."),
		preview:                fs.StringSlice("preview", nil, "Preview gates to enable."),
		otlpRelaySocket: fs.String("otlp-relay-socket", nodepath.AteletOTLPSocketPath(),
			"Unix socket of atelet's OTLP relay to export telemetry through, keeping it off the pod network. Empty, or absent at startup, exports directly to OTEL_EXPORTER_OTLP_ENDPOINT instead."),
	}
}

// Main runs a worker with the runtime newRuntime returns, and exits. The
// caller registers any flags of its own on pflag.CommandLine first.
func Main(cfg Config, newRuntime func(context.Context, Env) (Runtime, error)) {
	pid1.RunIfPID1()
	f := registerFlags(pflag.CommandLine)
	pflag.Parse()
	if *f.showVersion {
		fmt.Println(version.String())
		return
	}

	ctx := context.Background()
	if err := run(ctx, cfg, f, newRuntime); err != nil {
		slog.ErrorContext(ctx, "Error while executing", slog.Any("err", err))
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg Config, f *flags, newRuntime func(context.Context, Env) (Runtime, error)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The runtime logger and actor output share one writer, so their lines to
	// the pod's stdout don't interleave.
	logWriter := actorlog.NewSyncedWriter(os.Stdout)
	serverboot.InitLoggerWithWriter(logWriter)
	if err := serverboot.SetLogLevel(*f.logLevel); err != nil {
		return err
	}
	if err := preview.Init(*f.preview...); err != nil {
		return fmt.Errorf("invalid --preview: %w", err)
	}

	slog.InfoContext(ctx, "ateworker booting", slog.String("service", cfg.ServiceName), slog.String("version", version.Version))
	if *f.maxActors < 0 {
		return fmt.Errorf("--max-actors must not be negative, got %d", *f.maxActors)
	}
	if *f.usageSampleInterval < minUsageSampleInterval {
		return fmt.Errorf("--usage-sample-interval must be at least %v, got %v", minUsageSampleInterval, *f.usageSampleInterval)
	}

	// Export through atelet's node-local relay when it is there, so telemetry
	// never touches the worker pod's network. A nil conn means it is not, and
	// the providers dial the collector directly. A relay that cannot be dialed
	// is logged and not fatal: failing the worker over its telemetry route
	// would turn a misconfigured flag into an outage.
	relayConn, err := otlprelay.Dial(ctx, *f.otlpRelaySocket)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to connect to the OTLP relay; exporting telemetry directly over the pod network",
			slog.String("socket", *f.otlpRelaySocket), slog.Any("err", err))
	}
	if relayConn != nil {
		defer relayConn.Close()
	}

	tp, err := serverboot.InitTracing(ctx, serverboot.TracingOptions{
		ServiceName:  cfg.ServiceName,
		Sampling:     serverboot.ResolveTraceSampling(ctx, serverboot.ParentRatioSampling(serverboot.ControlPlaneTraceRatio)),
		ExporterConn: relayConn,
		// So the spans say which path they took, including when relayConn is nil
		// because the dial above failed.
		RelayCapable: true,
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize tracing", err)
	}
	defer serverboot.ShutdownProvider("TracerProvider", tp.Shutdown)

	mp, err := serverboot.InitMetricsPushOnlyVia(ctx, cfg.ServiceName, relayConn)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize metrics", err)
	}
	defer serverboot.ShutdownProvider("MeterProvider", mp.Shutdown)

	lp, err := serverboot.InitLogging(ctx, serverboot.LoggingOptions{
		ServiceName:  cfg.ServiceName,
		Exporter:     serverboot.ResolveLogsExporter(ctx),
		ExporterConn: relayConn,
		RelayCapable: true,
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize logging", err)
	}
	// Nil when the exporter does not include otlp.
	if lp != nil {
		defer serverboot.ShutdownProvider("LoggerProvider", lp.Shutdown)
	}

	podUID := *f.podUID
	if err := resources.ValidateWorkerPodUID(podUID); err != nil {
		return fmt.Errorf("in resources.ValidateWorkerPodUID: %w", err)
	}
	// atelet owns the directory and the sandbox versions file in it; only the
	// socket below is this process's. Both outlive a container restart.
	workerDir := nodepath.WorkerPath(podUID)
	if err := os.MkdirAll(workerDir, 0o700); err != nil {
		return fmt.Errorf("in os.MkdirAll(%q): %w", workerDir, err)
	}

	// Prepare the cgroup so the runtime can create a leaf per actor.
	actorCgroups, err := cgroup.Delegate(ctx)
	if err != nil {
		return fmt.Errorf("while delegating the worker cgroup: %w", err)
	}

	actorLogger := actorlog.NewActorLogger(logWriter, metadata.OnGCE())
	runtime, err := newRuntime(ctx, Env{PodUID: podUID, ActorLogger: actorLogger, ActorCgroups: actorCgroups})
	if err != nil {
		return err
	}

	sockPath := nodepath.WorkerSocketPath(podUID)
	if err := os.RemoveAll(sockPath); err != nil {
		return fmt.Errorf("while removing %q: %w", sockPath, err)
	}
	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		return fmt.Errorf("while opening unix socket: %w", err)
	}
	defer func() {
		if err := os.Remove(sockPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			slog.ErrorContext(ctx, "Failed to remove the ateworker socket on shutdown", slog.Any("err", err))
		}
	}()

	tunnel, err := tunnel.Start(ctx, *f.tunnel, actornet.ActorHTTPUpstream)
	if err != nil {
		return err
	}
	svc := newService(cfg, runtime, tunnel, actorLogger, *f.maxActors)
	// The controller sets both from the downward API.
	pool := workerstats.Pool{Namespace: os.Getenv("POD_NAMESPACE"), Name: os.Getenv("WORKER_POOL_NAME")}
	if pool.Namespace == "" || pool.Name == "" {
		slog.WarnContext(ctx, "Worker pool unknown; usage records will name no pool", slog.Any("pool", pool))
	}
	usageStdout := workerstats.NewStdoutHandler(logWriter)
	defer usageStdout.Close()
	svc.usage = workerstats.NewUsageEmitter(lp, usageStdout, pool)
	defer workerstats.StartSampler(ctx, *f.usageSampleInterval, svc.sweepUsage)()

	svr := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(ateinterceptors.InternalServerUnaryInterceptor),
	)
	ateworkerpb.RegisterWorkerServer(svr, svc)
	reflection.Register(svr)
	readiness := &serverboot.Readiness{}

	// Trap SIGTERM, sent by the kubelet at the start of the pod's termination
	// grace period, and propagate it into the sandboxes so actors can save
	// their state. The server keeps serving through the drain: new workloads
	// are rejected, while a suspend arriving mid-drain is still honored.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		slog.InfoContext(ctx, "Received signal; beginning graceful shutdown", slog.String("signal", sig.String()))
		readiness.MarkNotReady()
		// A fresh context: run's is torn down on return, and the drain must
		// outlive it.
		svc.gracefulShutdown(context.Background())
		// Blocks until in-flight RPCs, notably a checkpoint, complete.
		svr.GracefulStop()
	}()

	// Nothing else tells the control plane what this worker can supply, so a
	// worker that cannot report sits idle forever. Report retries every
	// failure it can outlast; anything that reaches here is a misconfiguration
	// no restart in place will fix.
	go func() {
		err := register.Report(ctx, register.ReportConfig{
			SocketPath:           nodepath.WorkerSupportSocket,
			CredentialBundlePath: f.tunnel.CredentialBundle,
			TrustBundlePath:      f.tunnel.TrustBundle,
			AteletSPIFFEID:       f.tunnel.BrokerIdentity,
			Actors:               *f.maxActors,
			SandboxClass:         cfg.SandboxClass,
		})
		if err != nil && ctx.Err() == nil {
			serverboot.Fatal(ctx, "Failed to report worker capacity", err)
		}
	}()

	go serverboot.StartHealthServer(ctx, *f.readinessListenAddress, readiness)

	slog.InfoContext(ctx, "ateworker serving", slog.String("socket", sockPath))
	if err := svr.Serve(lis); err != nil {
		return fmt.Errorf("while serving: %w", err)
	}
	return nil
}
