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

// scaledriver packs actors onto one worker by driving atelet's AteomHerder
// directly, with ateapi out of the loop.
//
// The shell harness (hack/scale-density.sh) measures the whole system, which is
// the right thing to measure -- but once ateapi's per-activation cost dominates,
// it stops being able to say anything about the sandbox underneath. Every
// activation there is two kubectl-ate processes, a scheduling decision, and a
// read-modify-write of a worker record that grows with the actor count.
//
// This drives the next layer down: resolve everything ONCE at startup, hold one
// gRPC connection, and issue nothing but Restore in the loop. What is left is
// the ateom's own cost, which is the number needed to decide whether the
// control plane or the sandbox is the thing to fix.
//
// It runs in-cluster because atelet requires a client certificate chaining to
// the podidentity CA, which only a pod can project.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/atelet"
	"github.com/agent-substrate/substrate/internal/credbundle"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	trustDomain     = "cluster.local"
	ateletNamespace = "ate-system"
	ateletSA        = "atelet"
	workerPoolLabel = "ate.dev/worker-pool"
)

var (
	mode = pflag.String("mode", "restore", "restore (create and activate), create (create only, leaving actors suspended), resume (activate actors that exist, no create), or terminate (tear them back down).")
	// The whole point of having both: the difference between them IS the control
	// plane's share of an activation, and nothing else measures it. Same process,
	// same concurrency, same clock -- only the layer being dialed changes.
	via = pflag.String("via", "atelet", "atelet (dial the node's AteomHerder, control plane removed) or ateapi (create+resume through the control plane, as any client would).")

	poolNamespace     = pflag.String("pool-namespace", "ate-scale-tiny", "Namespace of the WorkerPool to pack.")
	poolName          = pflag.String("pool-name", "tiny", "Name of the WorkerPool to pack.")
	templateNamespace = pflag.String("template-namespace", "ate-scale-tiny", "Namespace of the ActorTemplate to instantiate.")
	templateName      = pflag.String("template-name", "tiny", "Name of the ActorTemplate to instantiate.")
	atespace          = pflag.String("atespace", "scale", "Atespace recorded on each actor.")
	goldenURI         = pflag.String("golden-snapshot-uri", "", "gs:// URI of the template's golden snapshot. Required: this drives the same restore ateapi would, and the URI lives in ateapi's store, not the Kubernetes API.")

	// Separates "slow" from "broken". The ateom fails an activation whose readyz
	// does not answer in time, so under concurrency the default 30s turns queueing
	// delay into reported failures -- a 1000-actor run reported 649 failures whose
	// actors were up and answering minutes later.
	readyzTimeout = pflag.Int32("readyz-timeout-seconds", 0, "Override the template's readyz timeout. Zero keeps the template's (or the ateom's default).")

	count    = pflag.Int("count", 100, "How many actors to activate.")
	parallel = pflag.Int("parallel", 16, "How many activations to keep in flight.")
	prefix   = pflag.String("name-prefix", "d", "Actor name prefix. Scope it per run: a name left behind by a previous run restores again rather than failing, which quietly measures the wrong thing.")
	timeout  = pflag.Duration("timeout", 5*time.Minute, "Per-activation deadline.")
	// A fleet of shards scheduling onto a few nodes starts over a minute, and
	// summing their per-shard rates then overstates the fleet's. Holding every
	// shard until one wall-clock instant makes since_start_ms comparable across
	// shards, and the aggregate rate a real one.
	startAt = pflag.String("start-at", "", "RFC3339 instant to hold at before issuing. Empty starts immediately.")

	credBundle = pflag.String("cred-bundle", "/run/podidentity.podcert.ate.dev/credential-bundle.pem", "Client credential bundle presented to atelet.")
	caCerts    = pflag.String("ca-certs", "/run/podidentity.podcert.ate.dev/trust-bundle.pem", "CA bundle used to verify atelet.")
)

func main() {
	pflag.Parse()
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "scaledriver: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *mode == "restore" && *via == "atelet" && *goldenURI == "" {
		return fmt.Errorf("--golden-snapshot-uri is required for --mode=restore --via=atelet")
	}
	if *via != "atelet" && *via != "ateapi" {
		return fmt.Errorf("--via must be atelet or ateapi, got %q", *via)
	}
	if *via == "ateapi" {
		return driveViaAteapi(ctx)
	}

	cfg, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("in-cluster config: %w", err)
	}
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("kube client: %w", err)
	}
	ateapi, err := ateclient.NewClient(ctx, "", "", *ateapiEndpoint, *ateapiToken, false)
	if err != nil {
		return fmt.Errorf("ate client: %w", err)
	}
	defer ateapi.Close()

	tmpl, err := ateapi.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{
		ActorTemplate: &ateapipb.ObjectRef{Atespace: *templateNamespace, Name: *templateName},
	})
	if err != nil {
		return fmt.Errorf("get ActorTemplate %s/%s: %w", *templateNamespace, *templateName, err)
	}
	spec, err := workloadSpec(tmpl)
	if err != nil {
		return err
	}
	cpuMilli, memBytes, err := actorLimits(tmpl)
	if err != nil {
		return fmt.Errorf("actor limits: %w", err)
	}

	workerUID, node, err := workerPod(ctx, kube)
	if err != nil {
		return err
	}
	ateletIP, ateletUID, err := ateletOnNode(ctx, kube, node)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "worker %s on %s, atelet %s (%s)\n", workerUID, node, ateletIP, ateletUID)

	creds, err := dialCredentials(ateletUID)
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(net.JoinHostPort(ateletIP, strconv.Itoa(atelet.DefaultPort)), grpc.WithTransportCredentials(creds))
	if err != nil {
		return fmt.Errorf("dial atelet: %w", err)
	}
	defer conn.Close()
	client := ateletpb.NewAteomHerderClient(conn)

	call := restoreCall(client, workerUID, spec, cpuMilli, memBytes)
	if *mode == "terminate" {
		call = terminateCall(client, workerUID, spec)
	}
	return drive(ctx, call)
}

// actorName is the name AND the UID of the nth actor. atelet only needs the UID
// to be a unique DNS-1123 label -- it keys on-node state by it -- so a
// synthesized one keeps ateapi out of the loop entirely.
func actorName(n int) string { return fmt.Sprintf("%s-%06d", *prefix, n) }

func restoreCall(client ateletpb.AteomHerderClient, workerUID string, spec *ateletpb.WorkloadSpec, cpuMilli, memBytes int64) func(context.Context, int) error {
	return func(ctx context.Context, n int) error {
		name := actorName(n)
		// The same request ateapi's resume workflow sends for an actor with no
		// snapshot of its own but a golden one on its template: a full external
		// restore of the golden. No egress gateway, because tunneled egress would
		// pull atunnel and ateapi's credential broker back into the measurement.
		_, err := client.Restore(ctx, &ateletpb.RestoreRequest{
			TargetAteomUid:        workerUID,
			Atespace:              *atespace,
			ActorName:             name,
			ActorUid:              name,
			ActorTemplateAtespace: *templateNamespace,
			ActorTemplateName:     *templateName,
			Spec:                  spec,
			Type:                  ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
			Config: &ateletpb.RestoreRequest_ExternalConfig{
				ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{SnapshotUri: *goldenURI},
			},
			Scope:       ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
			CpuMilli:    cpuMilli,
			MemoryBytes: memBytes,
		})
		return err
	}
}

func terminateCall(client ateletpb.AteomHerderClient, workerUID string, spec *ateletpb.WorkloadSpec) func(context.Context, int) error {
	return func(ctx context.Context, n int) error {
		name := actorName(n)
		_, err := client.Terminate(ctx, &ateletpb.TerminateRequest{
			TargetAteomUid:        workerUID,
			Atespace:              *atespace,
			ActorName:             name,
			ActorUid:              name,
			ActorTemplateAtespace: *templateNamespace,
			ActorTemplateName:     *templateName,
			Spec:                  spec,
		})
		return err
	}
}

// drive issues call for actors 0..count with at most parallel in flight, and
// writes one CSV row per activation.
//
// The row carries the completion ORDER and the wall clock at completion, not
// just the duration, because the interesting failure is a curve: activation N
// costing more than activation 0 is what says the worker is degrading, and that
// is invisible in an average.
func drive(ctx context.Context, call func(context.Context, int) error) error {
	if *startAt != "" {
		at, err := time.Parse(time.RFC3339, *startAt)
		if err != nil {
			return fmt.Errorf("parsing --start-at: %w", err)
		}
		if late := time.Since(at); late > 0 {
			fmt.Fprintf(os.Stderr, "start barrier already passed by %s; starting now\n", late.Round(time.Millisecond))
		} else {
			select {
			case <-time.After(-late):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	fmt.Println("n,done,elapsed_ms,since_start_ms,ok,error")

	start := time.Now()
	fmt.Fprintf(os.Stderr, "started_at_epoch_ms=%d\n", start.UnixMilli())
	var wg sync.WaitGroup
	sem := make(chan struct{}, *parallel)
	var mu sync.Mutex
	var done, failed atomic.Int64
	var durations []time.Duration

	for n := range *count {
		if ctx.Err() != nil {
			break
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
		}
		wg.Go(func() {
			defer func() { <-sem }()

			callCtx, cancel := context.WithTimeout(ctx, *timeout)
			defer cancel()
			t := time.Now()
			err := call(callCtx, n)
			elapsed := time.Since(t)

			d := done.Add(1)
			msg := ""
			if err != nil {
				failed.Add(1)
				msg = strconv.Quote(err.Error())
			} else {
				mu.Lock()
				durations = append(durations, elapsed)
				mu.Unlock()
			}
			mu.Lock()
			fmt.Printf("%d,%d,%d,%d,%t,%s\n", n, d, elapsed.Milliseconds(), time.Since(start).Milliseconds(), err == nil, msg)
			mu.Unlock()
		})
	}
	wg.Wait()

	total := time.Since(start)
	ok := int64(len(durations))
	fmt.Fprintf(os.Stderr, "%d ok, %d failed in %s (%.1f/s)\n", ok, failed.Load(), total.Round(time.Millisecond), float64(ok)/total.Seconds())
	if ok > 0 {
		slices.Sort(durations)
		fmt.Fprintf(os.Stderr, "per-activation p50=%s p90=%s p99=%s max=%s\n",
			durations[ok*50/100].Round(time.Millisecond),
			durations[min(ok*90/100, ok-1)].Round(time.Millisecond),
			durations[min(ok*99/100, ok-1)].Round(time.Millisecond),
			durations[ok-1].Round(time.Millisecond))
	}
	if failed.Load() > 0 {
		return fmt.Errorf("%d of %d activations failed", failed.Load(), *count)
	}
	return nil
}

func workerPod(ctx context.Context, kube kubernetes.Interface) (uid, node string, err error) {
	pods, err := kube.CoreV1().Pods(*poolNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: workerPoolLabel + "=" + *poolName,
		FieldSelector: "status.phase=Running",
	})
	if err != nil {
		return "", "", fmt.Errorf("list worker pods: %w", err)
	}
	if len(pods.Items) != 1 {
		return "", "", fmt.Errorf("found %d running worker pods for %s/%s, want exactly 1: density is per worker, so a second one would split the population", len(pods.Items), *poolNamespace, *poolName)
	}
	pod := pods.Items[0]
	return string(pod.UID), pod.Spec.NodeName, nil
}

func ateletOnNode(ctx context.Context, kube kubernetes.Interface, node string) (ip, uid string, err error) {
	pods, err := kube.CoreV1().Pods(ateletNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + ateletSA,
		FieldSelector: "spec.nodeName=" + node,
	})
	if err != nil {
		return "", "", fmt.Errorf("list atelet pods: %w", err)
	}
	if len(pods.Items) != 1 {
		return "", "", fmt.Errorf("found %d atelet pods on node %q, want exactly 1", len(pods.Items), node)
	}
	pod := pods.Items[0]
	if pod.Status.PodIP == "" {
		return "", "", fmt.Errorf("atelet %s has no pod IP", pod.Name)
	}
	return pod.Status.PodIP, string(pod.UID), nil
}

// dialCredentials mirrors ateapi's atelet dialer: present the podidentity
// client bundle, and verify the peer is the atelet whose pod UID we resolved
// (the peer is dialed by IP and its certificate carries no IP SAN, so the
// SPIFFE identity does the work the hostname normally would).
func dialCredentials(ateletPodUID string) (credentials.TransportCredentials, error) {
	td, err := spiffeid.TrustDomainFromString(trustDomain)
	if err != nil {
		return nil, err
	}
	bundle, err := x509bundle.Load(td, *caCerts)
	if err != nil {
		return nil, fmt.Errorf("load CA bundle %s: %w", *caCerts, err)
	}
	expected, err := spiffeid.FromSegments(td, "ns", ateletNamespace, "sa", ateletSA)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		MinVersion:           tls.VersionTLS13,
		GetClientCertificate: credbundle.ClientLoader(*credBundle),
		InsecureSkipVerify:   true,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return fmt.Errorf("atelet presented no certificate")
			}
			id, _, err := x509svid.Verify(cs.PeerCertificates, bundle)
			if err != nil {
				return fmt.Errorf("verify atelet certificate: %w", err)
			}
			if id != expected {
				return fmt.Errorf("atelet SPIFFE ID %q, want %q", id, expected)
			}
			leaf := cs.PeerCertificates[0]
			if !slices.Contains(leaf.ExtKeyUsage, x509.ExtKeyUsageServerAuth) {
				return fmt.Errorf("atelet certificate lacks serverAuth")
			}
			identity, err := substratex509.PodIdentityFromCertificate(leaf)
			if err != nil {
				return fmt.Errorf("parse PodIdentity extension: %w", err)
			}
			if identity == nil || identity.PodUID != ateletPodUID {
				return fmt.Errorf("atelet pod UID %v, want %q", identity, ateletPodUID)
			}
			return nil
		},
	}), nil
}

// workloadSpec projects the ActorTemplate onto atelet's wire spec, covering
// only what a density fixture declares. Volumes are rejected rather than
// dropped: a silently missing mount would restore into a subtly different actor
// than the one whose golden snapshot is being restored.
func workloadSpec(tmpl *ateapipb.ActorTemplate) (*ateletpb.WorkloadSpec, error) {
	if len(tmpl.GetVolumes()) > 0 {
		return nil, fmt.Errorf("ActorTemplate %s/%s declares volumes; scaledriver only handles volume-free templates",
			tmpl.GetMetadata().GetAtespace(), tmpl.GetMetadata().GetName())
	}
	spec := &ateletpb.WorkloadSpec{}
	for _, ctr := range tmpl.GetContainers() {
		out := &ateletpb.Container{
			Name:    ctr.GetName(),
			Image:   ctr.GetImage(),
			Command: ctr.GetCommand(),
			Args:    ctr.GetArgs(),
		}
		for _, env := range ctr.GetEnv() {
			out.Env = append(out.Env, &ateletpb.EnvEntry{Name: env.GetName(), Value: env.GetValue()})
		}
		if r := ctr.GetReadyz(); r != nil {
			out.Readyz = &ateletpb.Readyz{TimeoutSeconds: r.GetTimeoutSeconds()}
			if *readyzTimeout > 0 {
				out.Readyz.TimeoutSeconds = *readyzTimeout
			}
			if g := r.GetHttpGet(); g != nil {
				out.Readyz.HttpGet = &ateletpb.HTTPGetAction{Path: g.GetPath(), Port: g.GetPort()}
			}
		}
		spec.Containers = append(spec.Containers, out)
	}
	return spec, nil
}

func actorLimits(tmpl *ateapipb.ActorTemplate) (cpuMilli, memBytes int64, err error) {
	limits, err := resources.ParseQuantities(tmpl.GetResources())
	if err != nil {
		return 0, 0, err
	}
	if c, ok := limits[resources.ResourceCPU]; ok {
		cpuMilli = c.MilliValue()
	}
	if m, ok := limits[resources.ResourceMemory]; ok {
		memBytes = m.Value()
	}
	return cpuMilli, memBytes, nil
}
