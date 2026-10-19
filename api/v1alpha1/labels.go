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

// Names shared by the Job webhook and the JobPool controller.
const (
	// QueuedLabel marks a Job that was suspended by the webhook and waits in the queue.
	// Suspended Jobs without this label belong to the user and are never started.
	QueuedLabel = "jobpool.kiribu.dev/queued"

	// EnabledNamespaceLabel is set by the controller on namespaces that have a JobPool
	// and removed from namespaces that have none. The Job webhook is only called for
	// namespaces with this label. The label belongs to the controller: manual changes
	// are reverted.
	EnabledNamespaceLabel = "jobpool.kiribu.dev/enabled"

	// LabelTrue is the value of QueuedLabel and EnabledNamespaceLabel.
	LabelTrue = "true"

	// Finalizer releases the queue before a JobPool is deleted.
	Finalizer = "jobpool.kiribu.dev/release-queue"

	// PoolAnnotation on a Namespace reserves it for the JobPool with this name.
	// Only one JobPool per namespace is allowed: the JobPool webhook sets the
	// reservation with an optimistic lock on the Namespace, so of two concurrent
	// JobPools only one gets it. The finalizer of the JobPool removes it.
	PoolAnnotation = "jobpool.kiribu.dev/pool"

	// PoolReservedAtAnnotation is the time (RFC 3339) PoolAnnotation was set. A
	// reservation of a JobPool that does not exist is taken over only when it is
	// old enough: the create request of that JobPool may still be in flight.
	PoolReservedAtAnnotation = "jobpool.kiribu.dev/pool-reserved-at"
)

// Condition types and reasons of JobPool.
const (
	// ConditionReady is True when the JobPool manages the queue of its namespace.
	ConditionReady = "Ready"

	// ReasonActive: the JobPool manages the queue.
	ReasonActive = "Active"
	// ReasonTerminating: the JobPool is being deleted and releases its queue.
	ReasonTerminating = "Terminating"
	// ReasonConflict: the namespace is reserved for another JobPool. Possible only
	// if the JobPool webhook was bypassed; this JobPool does nothing.
	ReasonConflict = "Conflict"
)
