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
	// PoolName is the only allowed name of a JobPool (CEL rule in the CRD).
	PoolName = "default"

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
)

// Condition types and reasons of JobPool.
const (
	// ConditionReady is True when the JobPool manages the queue of its namespace.
	ConditionReady = "Ready"

	// ReasonActive: the JobPool manages the queue.
	ReasonActive = "Active"
	// ReasonTerminating: the JobPool is being deleted and releases its queue.
	ReasonTerminating = "Terminating"
)
