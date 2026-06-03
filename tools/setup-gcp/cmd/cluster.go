// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	container "cloud.google.com/go/container/apiv1"
	"cloud.google.com/go/container/apiv1/containerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/wait"
)

var requiredBetaAPIs = []string{
	"certificates.k8s.io/v1beta1/podcertificaterequests",
	"certificates.k8s.io/v1beta1/clustertrustbundles",
}

func deleteCluster(ctx context.Context, env *Environment) error {
	client, err := container.NewClusterManagerClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()
	name := fmt.Sprintf("projects/%s/locations/%s/clusters/%s", env.ProjectID, env.ClusterLocation, env.ClusterName)
	slog.Info("Deleting cluster", slog.String("cluster", env.ClusterName))
	op, err := client.DeleteCluster(ctx, &containerpb.DeleteClusterRequest{Name: name})
	if err != nil {
		return fmt.Errorf("delete cluster: %w", err)
	}
	return waitContainerOperation(ctx, client, op.Name, env)
}

func createClusterInternal(ctx context.Context, env *Environment, client *container.ClusterManagerClient, parent string) error {
	slog.Info("Cluster does not exist. Creating...", slog.String("cluster", env.ClusterName))
	req := &containerpb.CreateClusterRequest{
		Parent: parent,
		Cluster: &containerpb.Cluster{
			Name:                  env.ClusterName,
			InitialClusterVersion: env.ClusterVersion,
			NodePools: []*containerpb.NodePool{
				{
					Name:             "substrate-node-pool",
					InitialNodeCount: 2,
					Config: &containerpb.NodeConfig{
						MachineType: env.GVisorNodeMachineType,
						ImageType:   "cos_containerd",
						LinuxNodeConfig: &containerpb.LinuxNodeConfig{
							SwapConfig: &containerpb.LinuxNodeConfig_SwapConfig{
								Enabled: proto.Bool(true),
								PerformanceProfile: &containerpb.LinuxNodeConfig_SwapConfig_BootDiskProfile_{
									BootDiskProfile: &containerpb.LinuxNodeConfig_SwapConfig_BootDiskProfile{
										SwapSize: &containerpb.LinuxNodeConfig_SwapConfig_BootDiskProfile_SwapSizeGib{
											SwapSizeGib: 8,
										},
									},
								},
							},
						},
					},
				},
			},
			EnableK8SBetaApis: &containerpb.K8SBetaAPIConfig{
				EnabledApis: requiredBetaAPIs,
			},
			WorkloadIdentityConfig: &containerpb.WorkloadIdentityConfig{
				WorkloadPool: fmt.Sprintf("%s.svc.id.goog", env.ProjectID),
			},
			Network:    env.Network,
			Subnetwork: env.Subnetwork,
		},
	}
	op, err := client.CreateCluster(ctx, req)
	if err != nil {
		return fmt.Errorf("create cluster: %w", err)
	}
	return waitContainerOperation(ctx, client, op.Name, env)
}

func createClusterIdempotent(ctx context.Context, env *Environment) error {
	client, err := container.NewClusterManagerClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	parent := fmt.Sprintf("projects/%s/locations/%s", env.ProjectID, env.ClusterLocation)
	clusterName := fmt.Sprintf("projects/%s/locations/%s/clusters/%s", env.ProjectID, env.ClusterLocation, env.ClusterName)

	slog.Info("Checking if cluster exists", slog.String("cluster", env.ClusterName), slog.String("location", env.ClusterLocation))
	cluster, err := client.GetCluster(ctx, &containerpb.GetClusterRequest{Name: clusterName})
	if err != nil {
		if status.Code(err) != codes.NotFound {
			return fmt.Errorf("getting cluster: %w", err)
		}

		return createClusterInternal(ctx, env, client, parent)
	}

	slog.Info("Cluster exists. Checking attributes...", slog.String("cluster", env.ClusterName))

	// Recreate cluster if network configuration mismatches.
	expectedNetwork := fmt.Sprintf("projects/%s/global/networks/%s", env.ProjectID, env.Network)
	if cluster.NetworkConfig != nil && cluster.NetworkConfig.Network != "" && !strings.HasSuffix(cluster.NetworkConfig.Network, expectedNetwork) {
		slog.Info("Mismatch in network", slog.String("current", cluster.NetworkConfig.Network), slog.String("expected", expectedNetwork))
		if err := deleteCluster(ctx, env); err != nil {
			return err
		}
		return createClusterInternal(ctx, env, client, parent)
	}

	// Recreate cluster if subnet configuration mismatches.
	expectedSubnetwork := fmt.Sprintf("projects/%s/regions/%s/subnetworks/%s", env.ProjectID, env.GCERegion, env.Subnetwork)
	if cluster.NetworkConfig != nil && cluster.NetworkConfig.Subnetwork != "" && !strings.HasSuffix(cluster.NetworkConfig.Subnetwork, expectedSubnetwork) {
		slog.Info("Mismatch in subnetwork", slog.String("current", cluster.NetworkConfig.Subnetwork), slog.String("expected", expectedSubnetwork))
		if err := deleteCluster(ctx, env); err != nil {
			return err
		}
		return createClusterInternal(ctx, env, client, parent)
	}

	expectedWorkloadPool := fmt.Sprintf("%s.svc.id.goog", env.ProjectID)
	currentWorkloadPool := ""
	if cluster.WorkloadIdentityConfig != nil {
		currentWorkloadPool = cluster.WorkloadIdentityConfig.WorkloadPool
	}
	if currentWorkloadPool != expectedWorkloadPool {
		slog.Info("Mismatch in workload pool", slog.String("current", currentWorkloadPool), slog.String("expected", expectedWorkloadPool))
		slog.Info("Updating cluster WorkloadIdentityConfig...")
		op, err := client.UpdateCluster(ctx, &containerpb.UpdateClusterRequest{
			Name: clusterName,
			Update: &containerpb.ClusterUpdate{
				DesiredWorkloadIdentityConfig: &containerpb.WorkloadIdentityConfig{
					WorkloadPool: expectedWorkloadPool,
				},
			},
		})
		if err != nil {
			return fmt.Errorf("update cluster workload identity: %w", err)
		}
		if err := waitContainerOperation(ctx, client, op.Name, env); err != nil {
			return err
		}
	} else {
		slog.Info("Cluster WorkloadIdentityConfig match perfectly.", slog.String("cluster", env.ClusterName))
	}

	if cluster.EnableK8SBetaApis == nil ||
		len(cluster.EnableK8SBetaApis.EnabledApis) == 0 ||
		!containsAll(cluster.EnableK8SBetaApis.EnabledApis, requiredBetaAPIs) {

		clusterEnabledAPIs := []string{}
		if cluster.EnableK8SBetaApis != nil && len(cluster.EnableK8SBetaApis.EnabledApis) > 0 {
			clusterEnabledAPIs = cluster.EnableK8SBetaApis.EnabledApis
		}
		slog.Info("Mismatch in EnableK8SBetaApis", slog.String("current", strings.Join(clusterEnabledAPIs, ",")), slog.String("expected", strings.Join(requiredBetaAPIs, ",")))

		var combinedAPIs []string
		for _, api := range append(requiredBetaAPIs, clusterEnabledAPIs...) {
			if !slices.Contains(combinedAPIs, api) {
				combinedAPIs = append(combinedAPIs, api)
			}
		}

		op, err := client.UpdateCluster(ctx, &containerpb.UpdateClusterRequest{
			Name: clusterName,
			Update: &containerpb.ClusterUpdate{
				DesiredK8SBetaApis: &containerpb.K8SBetaAPIConfig{
					EnabledApis: combinedAPIs,
				},
			},
		})
		if err != nil {
			return fmt.Errorf("update cluster beta apis: %w", err)
		}
		if err := waitContainerOperation(ctx, client, op.Name, env); err != nil {
			return err
		}
	} else {
		slog.Info("Cluster EnableK8SBetaApis match perfectly.", slog.String("cluster", env.ClusterName))
	}

	if err := reconcileClusterVersion(ctx, env, client, clusterName, cluster.GetCurrentMasterVersion()); err != nil {
		return fmt.Errorf("reconciling cluster version: %w", err)
	}

	if err := reconcileNodePools(ctx, env, client, clusterName); err != nil {
		return fmt.Errorf("reconciling node pools: %w", err)
	}

	return nil
}

// reconcileClusterVersion upgrades the cluster control plane to env.ClusterVersion
// when the current version is older. GKE does not support downgrades, so a
// current version that is newer than (or equal to) the desired version is left
// untouched.
func reconcileClusterVersion(ctx context.Context, env *Environment, client *container.ClusterManagerClient, clusterName, currentVersion string) error {
	desired := env.ClusterVersion
	if desired == "" {
		slog.Info("CLUSTER_VERSION not set; skipping control plane version reconciliation")
		return nil
	}
	if !gkeVersionLess(currentVersion, desired) {
		slog.Info("Cluster control plane version is up to date", slog.String("current", currentVersion), slog.String("desired", desired))
		return nil
	}

	slog.Info("Upgrading cluster control plane...", slog.String("current", currentVersion), slog.String("desired", desired))
	op, err := client.UpdateCluster(ctx, &containerpb.UpdateClusterRequest{
		Name: clusterName,
		Update: &containerpb.ClusterUpdate{
			DesiredMasterVersion: desired,
		},
	})
	if err != nil {
		return fmt.Errorf("upgrade control plane to %s: %w", desired, err)
	}
	if err := waitContainerOperation(ctx, client, op.Name, env); err != nil {
		return err
	}
	slog.Info("Cluster control plane upgraded successfully", slog.String("version", desired))
	return nil
}

// desiredNodePoolVersion returns the version node pools should run. It prefers
// NODE_POOL_VERSION and falls back to CLUSTER_VERSION. Node pool versions may
// not exceed the control plane version, which reconcileClusterVersion upgrades
// first.
func desiredNodePoolVersion(env *Environment) string {
	if env.NodePoolVersion != "" {
		return env.NodePoolVersion
	}
	return env.ClusterVersion
}

// parseGKEVersion splits a GKE version like "1.36.0-gke.2459000" into comparable
// integer components [major, minor, patch, gkeBuild]. Missing or non-numeric
// components are treated as 0 so comparisons degrade gracefully.
func parseGKEVersion(v string) [4]int {
	var out [4]int
	v = strings.TrimPrefix(v, "v")
	semver := v
	if i := strings.Index(v, "-gke."); i >= 0 {
		semver = v[:i]
		gke := strings.SplitN(v[i+len("-gke."):], ".", 2)[0]
		out[3] = atoiSafe(gke)
	}
	for i, p := range strings.Split(semver, ".") {
		if i > 2 {
			break
		}
		out[i] = atoiSafe(p)
	}
	return out
}

func atoiSafe(s string) int {
	n, _ := strconv.Atoi(strings.TrimFunc(s, func(r rune) bool { return r < '0' || r > '9' }))
	return n
}

// gkeVersionLess reports whether GKE version a is strictly older than b.
func gkeVersionLess(a, b string) bool {
	pa, pb := parseGKEVersion(a), parseGKEVersion(b)
	for i := range pa {
		if pa[i] != pb[i] {
			return pa[i] < pb[i]
		}
	}
	return false
}

func reconcileNodePools(ctx context.Context, env *Environment, client *container.ClusterManagerClient, clusterName string) error {
	slog.Info("Reconciling node pools...", slog.String("cluster", clusterName))

	// List node pools of the cluster
	resp, err := client.ListNodePools(ctx, &containerpb.ListNodePoolsRequest{
		Parent: clusterName,
	})
	if err != nil {
		return fmt.Errorf("list node pools: %w", err)
	}

	desiredNodePoolName := env.NodePoolName
	if desiredNodePoolName == "" {
		desiredNodePoolName = "substrate-node-pool"
	}

	var matchingPool *containerpb.NodePool
	var mismatchingPools []*containerpb.NodePool
	var unrelatedPools []*containerpb.NodePool

	for _, np := range resp.NodePools {
		isOurPool := np.Name == desiredNodePoolName ||
			strings.HasPrefix(np.Name, desiredNodePoolName+"-")

		if isOurPool {
			// Check if configuration matches
			matches := true
			if np.Config == nil {
				matches = false
			} else {
				if np.Config.ImageType != "cos_containerd" {
					matches = false
				}
				if np.Config.LinuxNodeConfig == nil ||
					np.Config.LinuxNodeConfig.SwapConfig == nil ||
					np.Config.LinuxNodeConfig.SwapConfig.Enabled == nil ||
					!*np.Config.LinuxNodeConfig.SwapConfig.Enabled {
					matches = false
				}
			}
			if matches {
				matchingPool = np
			} else {
				slog.Info("Found mismatching node pool configuration", slog.String("nodePool", np.Name))
				mismatchingPools = append(mismatchingPools, np)
			}
		} else {
			unrelatedPools = append(unrelatedPools, np)
		}
	}

	// Determine new pool name if we need to create it
	var activePoolName string
	if matchingPool != nil {
		activePoolName = matchingPool.Name
		slog.Info("Matching node pool already exists", slog.String("nodePool", activePoolName))

		// Upgrade the node pool in place if it is older than the desired version.
		desiredNPV := desiredNodePoolVersion(env)
		if desiredNPV != "" && gkeVersionLess(matchingPool.GetVersion(), desiredNPV) {
			slog.Info("Upgrading node pool version...",
				slog.String("nodePool", activePoolName),
				slog.String("current", matchingPool.GetVersion()),
				slog.String("desired", desiredNPV))
			op, err := client.UpdateNodePool(ctx, &containerpb.UpdateNodePoolRequest{
				Name:        fmt.Sprintf("%s/nodePools/%s", clusterName, matchingPool.Name),
				NodeVersion: desiredNPV,
				// ImageType is required by UpdateNodePool; keep it on COS.
				ImageType: "cos_containerd",
			})
			if err != nil {
				return fmt.Errorf("upgrade node pool %s: %w", matchingPool.Name, err)
			}
			if err := waitContainerOperation(ctx, client, op.Name, env); err != nil {
				return err
			}
			slog.Info("Node pool upgraded successfully", slog.String("nodePool", activePoolName))
		} else {
			slog.Info("Node pool version is up to date", slog.String("nodePool", activePoolName), slog.String("version", matchingPool.GetVersion()))
		}
	} else {
		// Determine which name is free among the blue-green candidates
		if !isNameInUse(resp.NodePools, desiredNodePoolName) {
			activePoolName = desiredNodePoolName
		} else if !isNameInUse(resp.NodePools, desiredNodePoolName+"-a") {
			activePoolName = desiredNodePoolName + "-a"
		} else {
			activePoolName = desiredNodePoolName + "-b"
		}

		slog.Info("Creating node pool with COS and Swap configuration...", slog.String("nodePool", activePoolName), slog.String("version", desiredNodePoolVersion(env)))
		req := &containerpb.CreateNodePoolRequest{
			Parent: clusterName,
			NodePool: &containerpb.NodePool{
				Name:             activePoolName,
				InitialNodeCount: 2,
				Version:          desiredNodePoolVersion(env),
				Config: &containerpb.NodeConfig{
					MachineType: env.GVisorNodeMachineType,
					ImageType:   "cos_containerd",
					LinuxNodeConfig: &containerpb.LinuxNodeConfig{
						SwapConfig: &containerpb.LinuxNodeConfig_SwapConfig{
							Enabled: proto.Bool(true),
							PerformanceProfile: &containerpb.LinuxNodeConfig_SwapConfig_BootDiskProfile_{
								BootDiskProfile: &containerpb.LinuxNodeConfig_SwapConfig_BootDiskProfile{
									SwapSize: &containerpb.LinuxNodeConfig_SwapConfig_BootDiskProfile_SwapSizeGib{
										SwapSizeGib: 8,
									},
								},
							},
						},
					},
				},
			},
		}
		op, err := client.CreateNodePool(ctx, req)
		if err != nil {
			return fmt.Errorf("create node pool %s: %w", activePoolName, err)
		}
		if err := waitContainerOperation(ctx, client, op.Name, env); err != nil {
			return err
		}
		slog.Info("Node pool created successfully", slog.String("nodePool", activePoolName))
	}

	// Delete mismatching pools and unrelated pools (like default-pool)
	poolsToDelete := append(mismatchingPools, unrelatedPools...)
	for _, np := range poolsToDelete {
		slog.Info("Deleting old/unwanted node pool...", slog.String("nodePool", np.Name))
		op, err := client.DeleteNodePool(ctx, &containerpb.DeleteNodePoolRequest{
			Name: fmt.Sprintf("%s/nodePools/%s", clusterName, np.Name),
		})
		if err != nil {
			slog.ErrorContext(ctx, "failed to delete node pool", slog.String("nodePool", np.Name), slog.Any("error", err))
			continue
		}
		if err := waitContainerOperation(ctx, client, op.Name, env); err != nil {
			slog.ErrorContext(ctx, "failed waiting for node pool deletion", slog.String("nodePool", np.Name), slog.Any("error", err))
			continue
		}
		slog.Info("Node pool deleted successfully", slog.String("nodePool", np.Name))
	}

	return nil
}

func isNameInUse(pools []*containerpb.NodePool, name string) bool {
	for _, p := range pools {
		if p.Name == name {
			return true
		}
	}
	return false
}

func waitContainerOperation(ctx context.Context, client *container.ClusterManagerClient, opName string, env *Environment) error {
	slog.Info("Waiting for operation to complete...", slog.String("operation", opName))

	fullName := opName
	if !strings.HasPrefix(opName, "projects/") {
		fullName = fmt.Sprintf("projects/%s/locations/%s/operations/%s", env.ProjectID, env.ClusterLocation, opName)
	}

	err := wait.PollUntilContextTimeout(ctx, 10*time.Second, 30*time.Minute, true, func(pollCtx context.Context) (bool, error) {
		op, err := client.GetOperation(pollCtx, &containerpb.GetOperationRequest{
			Name: fullName,
		})
		if err != nil {
			return false, fmt.Errorf("failed to get operation status: %w", err)
		}
		if op.Status == containerpb.Operation_DONE {
			if op.Error != nil {
				return true, fmt.Errorf("operation failed: %v", op.Error)
			}
			slog.Info("Operation completed successfully.", slog.String("operation", opName))
			return true, nil
		}
		if op.Status == containerpb.Operation_ABORTING {
			return true, fmt.Errorf("operation %s is aborting", opName)
		}
		return false, nil
	})

	if err != nil {
		return fmt.Errorf("wait for operation %s: %w", opName, err)
	}

	return nil
}

func containsAll(clusterAPIs []string, requiredAPIs []string) bool {
	for _, s := range requiredAPIs {
		if !slices.Contains(clusterAPIs, s) {
			return false
		}
	}
	return true
}
