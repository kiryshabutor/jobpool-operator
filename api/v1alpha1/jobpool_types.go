/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// JobPoolSpec defines the desired state of JobPool.
type JobPoolSpec struct {
	// maxConcurrentJobs is the maximum number of active Jobs in the namespace.
	// Jobs created above this limit are queued (spec.suspend=true) and started
	// in FIFO order when a slot frees up. Running Jobs are never suspended,
	// even if the limit is lowered.
	// +kubebuilder:validation:Minimum=1
	// +required
	MaxConcurrentJobs int32 `json:"maxConcurrentJobs"`
}

// JobPoolStatus defines the observed state of JobPool.
type JobPoolStatus struct {
	// activeJobs is the number of Jobs in the namespace that are neither
	// suspended nor finished, including Jobs not queued by this JobPool.
	// +optional
	ActiveJobs int32 `json:"activeJobs"`

	// queuedJobs is the number of Jobs waiting in this JobPool's queue.
	// +optional
	QueuedJobs int32 `json:"queuedJobs"`

	// observedGeneration is the .metadata.generation last processed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// conditions represent the current state of the JobPool resource.
	// The "Ready" condition is True when this JobPool manages the queue of its
	// namespace; reasons: Active, Terminating, Conflict.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=jp
// +kubebuilder:printcolumn:name="Max",type=integer,JSONPath=`.spec.maxConcurrentJobs`
// +kubebuilder:printcolumn:name="Active",type=integer,JSONPath=`.status.activeJobs`
// +kubebuilder:printcolumn:name="Queued",type=integer,JSONPath=`.status.queuedJobs`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].reason`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// JobPool limits the number of concurrently active Jobs in its namespace.
// Only one JobPool per namespace is allowed: the validating webhook reserves the
// namespace for the first one and rejects the others (see PoolAnnotation).
type JobPool struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of JobPool
	// +required
	Spec JobPoolSpec `json:"spec"`

	// status defines the observed state of JobPool
	// +optional
	Status JobPoolStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// JobPoolList contains a list of JobPool
type JobPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []JobPool `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &JobPool{}, &JobPoolList{})
		return nil
	})
}
