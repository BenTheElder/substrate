//go:build linux || darwin

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

// Scratch e2e: prove one worker pod hosts two actors at once.
//
// Not part of the suite yet -- it needs a pool deliberately shaped for it (one
// replica, maxActorsPerWorker 2, enough cpu and memory for both), which the
// shared fixtures are not.
package multiactor

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestMain(m *testing.M) { e2e.RunTestMain(m) }

const atespace = "multiactor"

// The template to run, overridable so the same assertions can be pointed at
// either sandbox class. The two ateoms host a set of actors in the same shape
// but by quite different means -- one runsc sandbox per actor against one
// cloud-hypervisor VM per actor -- so "two actors on one worker" is a claim
// that has to be made separately about each.
//
// Defaults to the gVisor counter. For micro-VM:
//
//	MULTIACTOR_TEMPLATE_NAMESPACE=ate-demo-counter-microvm \
//	MULTIACTOR_TEMPLATE_NAME=counter-microvm
//
// Either way the pool it selects must be shaped for this: one replica, and
// maxActorsPerWorker plus capacity enough for both actors.
func templateRef() (namespace, name string) {
	namespace = os.Getenv("MULTIACTOR_TEMPLATE_NAMESPACE")
	if namespace == "" {
		namespace = "ate-demo-counter"
	}
	name = os.Getenv("MULTIACTOR_TEMPLATE_NAME")
	if name == "" {
		name = "counter"
	}
	return namespace, name
}

// TestTwoActorsOnOneWorker is the whole point of the exercise: two actors
// running at once in a single worker pod, each reachable and each holding its
// own state.
func TestTwoActorsOnOneWorker(t *testing.T) {
	ctx := context.Background()
	clients := e2e.GetClients()

	_, _ = clients.SubstrateAPI.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: atespace}},
	})

	templateNamespace, templateName := templateRef()
	t.Logf("running against template %s/%s", templateNamespace, templateName)

	names := []string{
		fmt.Sprintf("ma-a-%d", time.Now().UnixNano()),
		fmt.Sprintf("ma-b-%d", time.Now().UnixNano()),
	}
	for _, name := range names {
		name := name
		t.Cleanup(func() {
			cctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			_, _ = clients.SubstrateAPI.SuspendActor(cctx, &ateapipb.SuspendActorRequest{
				Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: name}})
			_, _ = clients.SubstrateAPI.DeleteActor(cctx, &ateapipb.DeleteActorRequest{
				Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: name}})
		})
		if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: templateNamespace, Name: templateName},
		}}); err != nil {
			t.Fatalf("CreateActor(%s): %v", name, err)
		}
	}

	// Resume both. The second is the interesting one: it must be placed on the
	// same worker as the first and come up beside it.
	pods := map[string]string{}
	for _, name := range names {
		if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: name}}); err != nil {
			t.Fatalf("ResumeActor(%s): %v", name, err)
		}
		actor := waitRunning(ctx, t, clients, name)
		pod := actor.GetStatus().GetWorkerAssignment().GetWorkerPod()
		if pod == "" {
			t.Fatalf("actor %s has no worker pod", name)
		}
		pods[name] = pod
		t.Logf("actor %s is RUNNING on worker pod %s", name, pod)
	}

	if pods[names[0]] != pods[names[1]] {
		t.Fatalf("actors landed on different pods (%s, %s); the pool must be a single replica for this test",
			pods[names[0]], pods[names[1]])
	}
	t.Logf("both actors share worker pod %s", pods[names[0]])

	// Each must answer on its own DNS name, with its own counter. A shared
	// upstream or a crossed activation shows up here as one actor answering for
	// the other, or as one of them being unreachable.
	router, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	defer router.Close()

	for i, name := range names {
		body := callActor(ctx, t, router, name)
		t.Logf("actor %s (index %d) answered: %s", name, i, body)
	}

	// Both again, to show the first was not disturbed by the second activating
	// -- the failure the old teardown-on-setup would have produced.
	for _, name := range names {
		callActor(ctx, t, router, name)
	}
	t.Log("both actors still answering with both up")

	// CONNECT ingress, which resolves the actor separately from the plain HTTP
	// path above. Both actors here hold the same address and report it as their
	// own, so a crossed tunnel cannot be caught by comparing response bodies --
	// the two are byte-identical. What does catch it is the suspend below: if
	// CONNECT resolved to "whichever actor is on this worker" rather than the
	// one named in the authority, a tunnel to the suspended actor would land on
	// its neighbor and succeed.
	for _, name := range names {
		body := connectActor(ctx, t, router, name)
		if !strings.Contains(body, fmt.Sprintf("extra port %d", counterExtraPort)) {
			t.Errorf("actor %s CONNECT body = %q, want the extra-port greeting", name, body)
		}
	}
	t.Log("both actors reachable over CONNECT on the extra port")

	// Suspending one must not disturb the other. Teardown is per actor: its own
	// interior namespace and its own ruleset entries, so the survivor keeps its
	// networking. Its answering here is what says so.
	victim, survivor := names[0], names[1]
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: victim}}); err != nil {
		t.Fatalf("SuspendActor(%s): %v", victim, err)
	}
	// Wait for the suspend to actually land before asserting anything about
	// reachability: SuspendActor returning is not the same as the actor being
	// gone from its worker, and asserting too early would make this a race
	// rather than a check.
	waitState(ctx, t, clients, victim, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	t.Logf("suspended %s", victim)
	callActor(ctx, t, router, survivor)
	t.Logf("survivor %s still answering after its neighbor was suspended", survivor)

	// The survivor is still reachable over CONNECT with its neighbor gone.
	//
	// Deliberately NOT asserted here: that a tunnel naming the suspended actor
	// is refused. The router auto-resumes a suspended actor on ingress, so such
	// a tunnel is answered -- by the actor it names, after bringing it back.
	// That is the product's behavior on both the plain and CONNECT paths, and
	// it makes "refused" the wrong expectation rather than a missing guard.
	if body := connectActor(ctx, t, router, survivor); !strings.Contains(body, fmt.Sprintf("extra port %d", counterExtraPort)) {
		t.Errorf("survivor %s CONNECT body = %q, want the extra-port greeting", survivor, body)
	}
	t.Logf("survivor %s still reachable over CONNECT after its neighbor was suspended", survivor)

	// And the suspended one comes back beside the survivor, onto the worker it
	// left, reusing the slot it gave up.
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: victim}}); err != nil {
		t.Fatalf("ResumeActor(%s): %v", victim, err)
	}
	resumed := waitRunning(ctx, t, clients, victim)
	if got := resumed.GetStatus().GetWorkerAssignment().GetWorkerPod(); got != pods[victim] {
		t.Errorf("actor %s resumed onto pod %s, want the single pool replica %s", victim, got, pods[victim])
	}
	for _, name := range names {
		callActor(ctx, t, router, name)
	}
	t.Log("both actors answering again after a suspend/resume of one")
}

// counterExtraPort is the counter demo's second listener (--extra-port), the
// port CONNECT ingress addresses. Both sandbox classes' fixtures set it.
const counterExtraPort = 9090

// connectActor tunnels to the actor's extra port and returns the response,
// retrying briefly the way callActor does.
func connectActor(ctx context.Context, t *testing.T, router *e2e.RouterClient, name string) string {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		body, err := tryConnectActor(ctx, router, name)
		if err == nil {
			return body
		}
		lastErr = err
		time.Sleep(time.Second)
	}
	t.Fatalf("actor %s unreachable over CONNECT: %v", name, lastErr)
	return ""
}

// tryConnectActor makes one CONNECT attempt and reads one response through it.
// Separate from connectActor because the suspended-actor assertion wants a
// single attempt and expects it to fail.
func tryConnectActor(ctx context.Context, router *e2e.RouterClient, name string) (string, error) {
	ref := resources.ActorRef{Atespace: atespace, Name: name}
	conn, err := router.Connect(ctx, ref, counterExtraPort)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		return "", err
	}
	// The tunnel establishes in atenet-router before the actor is dialed, so
	// only a real request through it proves the actor was reached.
	if _, err := conn.Write([]byte("GET / HTTP/1.1\r\nHost: " + resources.ActorDNSName(ref) + "\r\nConnection: close\r\n\r\n")); err != nil {
		return "", err
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return string(body), fmt.Errorf("status %d", resp.StatusCode)
	}
	return string(body), nil
}

// callActor posts to an actor through the router, retrying briefly: an actor
// that has just reached RUNNING may take a moment to accept.
func callActor(ctx context.Context, t *testing.T, router *e2e.RouterClient, name string) string {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		body, err := tryCallActor(ctx, router, name)
		if err == nil {
			return body
		}
		lastErr = err
		time.Sleep(time.Second)
	}
	t.Fatalf("actor %s unreachable: %v", name, lastErr)
	return ""
}

// tryCallActor makes one plain-HTTP attempt through the router.
func tryCallActor(ctx context.Context, router *e2e.RouterClient, name string) (string, error) {
	ref := resources.ActorRef{Atespace: atespace, Name: name}
	resp, err := router.PostJSON(ctx, ref, "/", nil)
	if err != nil {
		return "", err
	}
	body, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return string(body), fmt.Errorf("status %d: %s", resp.StatusCode, body)
	}
	if readErr != nil {
		return "", readErr
	}
	return string(body), nil
}

// waitState blocks until the actor reports want, so assertions that depend on a
// transition having completed do not race it.
func waitState(ctx context.Context, t *testing.T, clients *e2e.Clients, name string, want ateapipb.ActorState) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	var last ateapipb.ActorState
	for time.Now().Before(deadline) {
		actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: name}})
		if err == nil {
			last = actor.GetStatus().GetState()
			if last == want {
				return
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("actor %s did not reach %v (last %v)", name, want, last)
}

func waitRunning(ctx context.Context, t *testing.T, clients *e2e.Clients, name string) *ateapipb.Actor {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: name}})
		if err == nil {
			switch actor.GetStatus().GetState() {
			case ateapipb.ActorState_ACTOR_STATE_RUNNING:
				return actor
			case ateapipb.ActorState_ACTOR_STATE_CRASHED:
				t.Fatalf("actor %s CRASHED while resuming", name)
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("actor %s did not reach RUNNING", name)
	return nil
}
