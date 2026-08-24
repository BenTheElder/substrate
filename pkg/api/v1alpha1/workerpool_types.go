// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WorkerPoolLabelValue is a Kubernetes label value for generated worker
// workloads.
//
// +kubebuilder:validation:MaxLength=63
// +kubebuilder:validation:Pattern=`^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$`
type WorkerPoolLabelValue string

// WorkerPoolPodTemplate defines optional metadata, scheduling, and resource
// settings for worker workloads. NodeAffinity is mapped to
// spec.affinity.nodeAffinity on the pod.
type WorkerPoolPodTemplate struct {
	// Labels are added to the generated Deployment and worker pods. Keys in
	// the ate.dev domain and its subdomains are reserved for controllers.
	//
	// +optional
	// +kubebuilder:validation:MaxProperties=64
	// +kubebuilder:validation:XValidation:rule="self.all(key, !key.startsWith('ate.dev/') && !key.contains('.ate.dev/'))",message="ate.dev and its subdomains are reserved"
	// +kubebuilder:validation:XValidation:rule="self.all(key, !format.qualifiedName().validate(key).hasValue())",message="label keys must be valid Kubernetes qualified names"
	Labels map[string]WorkerPoolLabelValue `json:"labels,omitempty"`

	// Annotations are added to the generated Deployment and worker pods. Keys
	// in the ate.dev domain and its subdomains are reserved for controllers.
	//
	// +optional
	// +kubebuilder:validation:MaxProperties=64
	// +kubebuilder:validation:XValidation:rule="self.all(key, !key.startsWith('ate.dev/') && !key.contains('.ate.dev/'))",message="ate.dev and its subdomains are reserved"
	// +kubebuilder:validation:XValidation:rule="self.all(key, !format.qualifiedName().validate(key).hasValue())",message="annotation keys must be valid Kubernetes qualified names"
	Annotations map[string]string `json:"annotations,omitempty"`

	// NodeSelector is a selector which must be true for the pod to fit on a node.
	//
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations for the worker pods.
	//
	// +optional
	// +kubebuilder:validation:MaxItems=16
	// +listType=atomic
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// PriorityClassName for the worker pods.
	//
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`

	// NodeAffinity scheduling rules for the worker pods. Mapped to
	// spec.affinity.nodeAffinity on the pod.
	//
	// +optional
	NodeAffinity *corev1.NodeAffinity `json:"nodeAffinity,omitempty"`

	// Resources are the compute resources allocated for each worker pod.
	//
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="!has(self.sandboxClass) || self.sandboxClass == 'gvisor' || !has(self.template) || !has(self.template.resources) || !((has(self.template.resources.limits) && 'nvidia.com/gpu' in self.template.resources.limits) || (has(self.template.resources.requests) && 'nvidia.com/gpu' in self.template.resources.requests))",message="nvidia.com/gpu is only supported when sandboxClass is 'gvisor'"
// +kubebuilder:validation:XValidation:rule="!has(self.template) || !has(self.template.resources) || !has(self.template.resources.requests) || !('nvidia.com/gpu' in self.template.resources.requests) || (has(self.template.resources.limits) && 'nvidia.com/gpu' in self.template.resources.limits)",message="nvidia.com/gpu must be set in limits: Kubernetes does not admit a request for an extended resource without a matching limit"
type WorkerPoolSpec struct {
	// Replicas is the number of worker pods to run.
	// +required
	// +kubebuilder:validation:Minimum=0
	Replicas int32 `json:"replicas"`

	// AteomImage is the ateom container image to deploy as workers.
	// +kubebuilder:validation:MinLength=1
	// +required
	AteomImage string `json:"ateomImage"`

	// Template holds optional metadata, scheduling, and resource settings for worker workloads.
	//
	// +optional
	Template *WorkerPoolPodTemplate `json:"template,omitempty"`

	// SandboxClass selects the sandbox runtime family for this pool, which drives
	// the worker pod shape (KVM/vhost device mounts and node placement) and which
	// SandboxConfigs are eligible. The concrete binary is still selected by
	// AteomImage. Defaults to gvisor.
	//
	// See Also: TODOs in ActorTemplate SandboxClass
	//
	// +optional
	// +kubebuilder:validation:Enum=gvisor;microvm
	// +kubebuilder:default=gvisor
	SandboxClass SandboxClass `json:"sandboxClass,omitempty"`

	// SandboxConfigName names a cluster-scoped SandboxConfig to use for fetching
	// sandbox binaries. It overrides the cluster-wide default SandboxConfig for
	// this pool's SandboxClass. The referenced config's SandboxClass must match
	// this pool's SandboxClass. If empty, the default SandboxConfig for the
	// SandboxClass is used.
	// +optional
	SandboxConfigName string `json:"sandboxConfigName,omitempty"`

	// MaxActorsPerWorker is how many Actors may be bound to one worker Pod at
	// once. It bounds the per-Actor costs that CPU and memory limits do not --
	// network namespaces, mounts, file descriptors, and how much is lost with
	// the pod -- and is enforced as one dimension of the worker's capacity,
	// alongside the compute limits taken from the ateom container. An Actor is
	// placed only where every dimension still has room.
	//
	// The ceiling is the worker's pod-side address space: actornet gives each
	// Actor an address out of a /20, so 4094 is what the addressing supports
	// and the API refuses to promise more than the implementation can number.
	// The practical limit is far lower and is set by the pod's memory: a
	// worker cannot host more Actors than it can hold guests for.
	//
	// It defaults to 1, which behaves exactly as single-Actor workers always
	// have. Raising it is supported on both sandbox classes -- each Actor gets
	// its own network namespace, sandbox, and atunnel activation -- but has
	// been exercised end to end at 2, not at hundreds. Treat large values as
	// unproven rather than unsupported: what is known to hold is the design,
	// not the constant.
	//
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=4094
	// +kubebuilder:default=1
	MaxActorsPerWorker int32 `json:"maxActorsPerWorker,omitempty"`
}

// MaxActors returns how many Actors each worker Pod in this pool may host,
// treating the zero value as 1 so a pool stored before the field existed, or
// built by a client that does not set it, keeps single-Actor behavior.
func (s *WorkerPoolSpec) MaxActors() int32 {
	if s.MaxActorsPerWorker < 1 {
		return 1
	}
	return s.MaxActorsPerWorker
}

type WorkerPoolStatus struct {
	// Replicas is the total number of worker pods.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Replicas int32 `json:"replicas"`

	// ReadyReplicas is the number of ready worker pods.
	// +kubebuilder:validation:Minimum=0
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// Selector is the label selector for the worker pods.
	// +optional
	Selector string `json:"selector,omitempty"`
}

// WorkerPool is the Schema for the workerpools API
// +genclient
// +kubebuilder:object:generate=true
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=workerpool
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.status.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type WorkerPool struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of WorkerPool
	// +required
	Spec WorkerPoolSpec `json:"spec"`

	// status is the observed state of WorkerPool
	// +optional
	Status WorkerPoolStatus `json:"status,omitempty"`
}

// WorkerPoolList contains a list of WorkerPools.
// +kubebuilder:object:generate=true
// +kubebuilder:object:root=true
type WorkerPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WorkerPool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&WorkerPool{}, &WorkerPoolList{})
}
