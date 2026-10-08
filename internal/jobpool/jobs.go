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

// Package jobpool contains the queueing rules of the operator as pure functions.
// It does not talk to the API server, so every rule is covered by plain unit tests.
package jobpool

import (
	"cmp"
	"slices"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	schedulingv1alpha1 "github.com/kiryl-b-inno/jobpool-operator/api/v1alpha1"
)

// IsFinished reports whether the Job has a Complete or Failed condition set to True.
func IsFinished(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// IsSuspended reports whether spec.suspend is set to true.
func IsSuspended(job *batchv1.Job) bool {
	return job.Spec.Suspend != nil && *job.Spec.Suspend
}

// IsActive reports whether the Job occupies a slot of the pool: it is neither
// suspended nor finished. Every Job of the namespace counts, including Jobs
// that were never queued by the operator.
func IsActive(job *batchv1.Job) bool {
	return !IsSuspended(job) && !IsFinished(job)
}

// IsQueued reports whether the Job waits in the operator's queue: it was
// suspended by the webhook (has QueuedLabel), is not finished and not being deleted.
// Jobs suspended by the user have no label and are never started by the operator.
func IsQueued(job *batchv1.Job) bool {
	return IsSuspended(job) &&
		job.Labels[schedulingv1alpha1.QueuedLabel] == schedulingv1alpha1.LabelTrue &&
		!IsFinished(job) &&
		job.DeletionTimestamp == nil
}

// olderFirst orders objects by creationTimestamp and then by name.
// creationTimestamp has a one-second resolution, so the name breaks ties
// and keeps the order stable between reconciles.
func olderFirst(a, b metav1.Object) int {
	if c := a.GetCreationTimestamp().Compare(b.GetCreationTimestamp().Time); c != 0 {
		return c
	}
	return cmp.Compare(a.GetName(), b.GetName())
}

// SortFIFO sorts Jobs from the oldest to the newest.
func SortFIFO(jobs []*batchv1.Job) {
	slices.SortFunc(jobs, func(a, b *batchv1.Job) int { return olderFirst(a, b) })
}

// QueuePlan is the decision of a single reconcile.
type QueuePlan struct {
	// Active is the number of active Jobs before ToStart are started.
	Active int32
	// ToStart are queued Jobs to resume now, oldest first.
	ToStart []*batchv1.Job
	// Waiting are queued Jobs that stay suspended, oldest first.
	Waiting []*batchv1.Job
}

// Plan decides which queued Jobs can be started without exceeding maxActive.
// Running Jobs are never suspended: if there are already more active Jobs than
// maxActive (the limit was lowered or the Jobs ran before the pool existed),
// nothing is started until enough of them finish.
func Plan(jobs []batchv1.Job, maxActive int32) QueuePlan {
	var plan QueuePlan
	var queued []*batchv1.Job
	for i := range jobs {
		job := &jobs[i]
		switch {
		case IsActive(job):
			plan.Active++
		case IsQueued(job):
			queued = append(queued, job)
		}
	}
	SortFIFO(queued)

	free := max(0, int(maxActive)-int(plan.Active))
	n := min(free, len(queued))
	plan.ToStart = queued[:n]
	plan.Waiting = queued[n:]
	return plan
}
