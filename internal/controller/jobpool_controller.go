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

package controller

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/recorder"

	schedulingv1alpha1 "github.com/kiryl-b-inno/jobpool-operator/api/v1alpha1"
	"github.com/kiryl-b-inno/jobpool-operator/internal/jobpool"
)

// releaseRecheckDelay is how long the finalizer waits before checking the queue
// again when a Job was queued while the queue was being released.
const releaseRecheckDelay = time.Second

// JobPoolReconciler starts queued Jobs of a namespace without exceeding
// spec.maxConcurrentJobs and releases the queue when the JobPool is deleted.
type JobPoolReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// APIReader reads Jobs directly from the API server, bypassing the cache.
	// The cache may not yet contain a Job the previous reconcile has just started,
	// and counting it as queued would exceed the limit (task1.md, section 16).
	APIReader client.Reader
	Recorder  recorder.EventRecorder
}

// +kubebuilder:rbac:groups=scheduling.kiribu.dev,resources=jobpools,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=scheduling.kiribu.dev,resources=jobpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=scheduling.kiribu.dev,resources=jobpools/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile recomputes the queue of the namespace from scratch on every call,
// so it does not matter which event triggered it (level-triggered).
func (r *JobPoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var pool schedulingv1alpha1.JobPool
	if err := r.Get(ctx, req.NamespacedName, &pool); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !pool.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, &pool)
	}

	if !controllerutil.ContainsFinalizer(&pool, schedulingv1alpha1.Finalizer) {
		base := pool.DeepCopy()
		controllerutil.AddFinalizer(&pool, schedulingv1alpha1.Finalizer)
		if err := r.Patch(ctx, &pool, client.MergeFrom(base)); err != nil {
			return ctrl.Result{}, err
		}
	}

	var pools schedulingv1alpha1.JobPoolList
	if err := r.List(ctx, &pools, client.InNamespace(pool.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	if primary := jobpool.Primary(pools.Items); primary == nil || primary.Name != pool.Name {
		// Safety net for duplicates that slipped past the validating webhook.
		// No requeue: deleting the primary triggers a reconcile of this pool (see SetupWithManager).
		msg := "Namespace is already managed by another JobPool"
		if primary != nil {
			msg = fmt.Sprintf("Namespace is already managed by JobPool %q", primary.Name)
		}
		return ctrl.Result{}, r.updateStatus(ctx, &pool, 0, 0, metav1.ConditionFalse, schedulingv1alpha1.ReasonDuplicate, msg)
	}

	if err := r.setNamespaceLabel(ctx, pool.Namespace, true); err != nil {
		return ctrl.Result{}, err
	}

	var jobs batchv1.JobList
	if err := r.APIReader.List(ctx, &jobs, client.InNamespace(pool.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	plan := jobpool.Plan(jobs.Items, pool.Spec.MaxConcurrentJobs)

	for _, job := range plan.ToStart {
		if err := r.startJob(ctx, job); err != nil {
			// A conflict means the Job changed after we read it; the retry
			// recomputes the plan from fresh data.
			return ctrl.Result{}, err
		}
		log.Info("Started Job from queue", "job", job.Name)
		r.Recorder.Eventf(&pool, job, corev1.EventTypeNormal, "Started", "StartJob", "Started Job %s from queue", job.Name)
	}

	active := plan.Active + int32(len(plan.ToStart))
	return ctrl.Result{}, r.updateStatus(ctx, &pool, active, int32(len(plan.Waiting)),
		metav1.ConditionTrue, schedulingv1alpha1.ReasonActive, "")
}

// finalize releases the queue of a deleted JobPool and removes its finalizer.
// If another JobPool remains in the namespace, it takes over the queue as is.
func (r *JobPoolReconciler) finalize(ctx context.Context, pool *schedulingv1alpha1.JobPool) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	if !controllerutil.ContainsFinalizer(pool, schedulingv1alpha1.Finalizer) {
		return ctrl.Result{}, nil
	}

	if err := r.updateStatus(ctx, pool, pool.Status.ActiveJobs, pool.Status.QueuedJobs,
		metav1.ConditionFalse, schedulingv1alpha1.ReasonTerminating, "JobPool is being deleted"); err != nil {
		return ctrl.Result{}, err
	}

	// Read pools from the API server: a pool created a moment ago must take over the queue.
	var pools schedulingv1alpha1.JobPoolList
	if err := r.APIReader.List(ctx, &pools, client.InNamespace(pool.Namespace)); err != nil {
		return ctrl.Result{}, err
	}

	if successor := jobpool.Primary(pools.Items); successor != nil {
		log.Info("Queue is handed over to another JobPool", "successor", successor.Name)
	} else {
		// Stop the webhook first, so no new Jobs are queued while the queue is released.
		if err := r.setNamespaceLabel(ctx, pool.Namespace, false); err != nil {
			return ctrl.Result{}, err
		}
		released, err := r.releaseQueue(ctx, pool)
		if err != nil {
			return ctrl.Result{}, err
		}
		if released > 0 {
			// A Job may have been queued by a webhook that had not seen the deletion yet.
			// Check the queue again before letting the JobPool go.
			return ctrl.Result{RequeueAfter: releaseRecheckDelay}, nil
		}
	}

	base := pool.DeepCopy()
	controllerutil.RemoveFinalizer(pool, schedulingv1alpha1.Finalizer)
	return ctrl.Result{}, client.IgnoreNotFound(r.Patch(ctx, pool, client.MergeFrom(base)))
}

// releaseQueue starts every Job queued by the operator in the namespace and
// returns how many Jobs it touched. Jobs suspended by the user have no queued
// label and stay suspended.
func (r *JobPoolReconciler) releaseQueue(ctx context.Context, pool *schedulingv1alpha1.JobPool) (int, error) {
	var jobs batchv1.JobList
	if err := r.APIReader.List(ctx, &jobs, client.InNamespace(pool.Namespace),
		client.MatchingLabels{schedulingv1alpha1.QueuedLabel: schedulingv1alpha1.LabelTrue}); err != nil {
		return 0, err
	}
	for i := range jobs.Items {
		job := &jobs.Items[i]
		if err := r.startJob(ctx, job); err != nil {
			return 0, err
		}
		logf.FromContext(ctx).Info("Released Job from queue", "job", job.Name)
		r.Recorder.Eventf(pool, job, corev1.EventTypeNormal, "Released", "ReleaseJob",
			"Released Job %s from queue: JobPool is being deleted", job.Name)
	}
	return len(jobs.Items), nil
}

// startJob resumes a queued Job: spec.suspend=false and no queued label.
// The optimistic lock makes the patch fail if the Job changed since it was read.
func (r *JobPoolReconciler) startJob(ctx context.Context, job *batchv1.Job) error {
	base := job.DeepCopy()
	if jobpool.IsSuspended(job) {
		job.Spec.Suspend = new(false)
	}
	delete(job.Labels, schedulingv1alpha1.QueuedLabel)
	err := r.Patch(ctx, job, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	return client.IgnoreNotFound(err)
}

// setNamespaceLabel adds or removes EnabledNamespaceLabel. The Job webhook is
// called only for namespaces with this label (namespaceSelector).
func (r *JobPoolReconciler) setNamespaceLabel(ctx context.Context, name string, enabled bool) error {
	var ns corev1.Namespace
	if err := r.Get(ctx, client.ObjectKey{Name: name}, &ns); err != nil {
		return client.IgnoreNotFound(err)
	}
	_, has := ns.Labels[schedulingv1alpha1.EnabledNamespaceLabel]
	if has == enabled {
		return nil
	}
	base := ns.DeepCopy()
	if enabled {
		if ns.Labels == nil {
			ns.Labels = map[string]string{}
		}
		ns.Labels[schedulingv1alpha1.EnabledNamespaceLabel] = schedulingv1alpha1.LabelTrue
	} else {
		delete(ns.Labels, schedulingv1alpha1.EnabledNamespaceLabel)
	}
	return client.IgnoreNotFound(r.Patch(ctx, &ns, client.MergeFrom(base)))
}

// updateStatus writes counters and the Ready condition; it skips the API call if nothing changed.
func (r *JobPoolReconciler) updateStatus(ctx context.Context, pool *schedulingv1alpha1.JobPool,
	active, queued int32, ready metav1.ConditionStatus, reason, message string) error {
	if message == "" {
		message = fmt.Sprintf("Managing the Job queue of namespace %s", pool.Namespace)
	}
	base := pool.DeepCopy()
	pool.Status.ActiveJobs = active
	pool.Status.QueuedJobs = queued
	pool.Status.ObservedGeneration = pool.Generation
	meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
		Type:               schedulingv1alpha1.ConditionReady,
		Status:             ready,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: pool.Generation,
	})
	if equality.Semantic.DeepEqual(base.Status, pool.Status) {
		return nil
	}
	return client.IgnoreNotFound(r.Status().Patch(ctx, pool, client.MergeFrom(base)))
}

// SetupWithManager sets up the controller with the Manager.
//
// Jobs are watched, not owned: Jobs are created by users, and an ownerReference
// to the JobPool would make the garbage collector delete them with the pool.
func (r *JobPoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&schedulingv1alpha1.JobPool{}).
		// Any Job change (created, finished, deleted, user suspend) may free or take a slot.
		Watches(&batchv1.Job{}, handler.EnqueueRequestsFromMapFunc(r.poolsInNamespace)).
		// When a JobPool goes away, a duplicate in the same namespace becomes primary.
		Watches(&schedulingv1alpha1.JobPool{}, handler.EnqueueRequestsFromMapFunc(r.poolsInNamespace)).
		Named("jobpool").
		Complete(r)
}

// poolsInNamespace enqueues every JobPool of the object's namespace;
// Reconcile itself decides which of them is primary.
func (r *JobPoolReconciler) poolsInNamespace(ctx context.Context, obj client.Object) []reconcile.Request {
	var pools schedulingv1alpha1.JobPoolList
	if err := r.List(ctx, &pools, client.InNamespace(obj.GetNamespace())); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list JobPools", "namespace", obj.GetNamespace())
		return nil
	}
	requests := make([]reconcile.Request, 0, len(pools.Items))
	for _, pool := range pools.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&pool)})
	}
	return requests
}
