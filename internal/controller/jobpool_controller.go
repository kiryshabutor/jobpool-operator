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

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	"github.com/kiryl-b-inno/jobpool-operator/internal/reservation"
)

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
// A request with an empty name stands for a namespace without a JobPool that still
// has our traces (see poolRequests).
func (r *JobPoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if req.Name == "" {
		return ctrl.Result{}, r.cleanupNamespace(ctx, req.Namespace)
	}
	var pool schedulingv1alpha1.JobPool
	if err := r.Get(ctx, req.NamespacedName, &pool); apierrors.IsNotFound(err) {
		return ctrl.Result{}, r.cleanupNamespace(ctx, req.Namespace)
	} else if err != nil {
		return ctrl.Result{}, err
	}

	if !pool.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, &pool)
	}

	if !controllerutil.ContainsFinalizer(&pool, schedulingv1alpha1.Finalizer) {
		base := pool.DeepCopy()
		controllerutil.AddFinalizer(&pool, schedulingv1alpha1.Finalizer)
		if err := r.Patch(ctx, &pool, finalizerPatch(base)); err != nil {
			return ctrl.Result{}, err
		}
	}

	if owner, result, err := r.reserve(ctx, &pool); err != nil || !owner {
		return result, err
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

// reserve makes sure the namespace is reserved for the JobPool and reports whether
// it is. Normally the JobPool webhook has reserved it before the JobPool was
// stored, and the cached Namespace confirms that without an API call. The
// controller reserves the namespace itself if the webhook was bypassed (webhooks
// disabled, or a JobPool created before the webhook was installed).
//
// If the namespace is reserved for another JobPool, this one does nothing and
// reports Conflict. It is reconciled again when the reservation changes (the
// Namespace is watched), and in any case after a while: an owner that disappears
// without its finalizer (removed by hand) leaves the Namespace unchanged, and only
// a periodic check notices that its reservation is dead.
func (r *JobPoolReconciler) reserve(ctx context.Context, pool *schedulingv1alpha1.JobPool) (bool, ctrl.Result, error) {
	var ns corev1.Namespace
	if err := r.Get(ctx, client.ObjectKey{Name: pool.Namespace}, &ns); err != nil {
		// A missing namespace takes the JobPool with it.
		return false, ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if owner, _ := reservation.Owner(&ns); owner == pool.Name {
		return true, ctrl.Result{}, nil
	}

	d, err := reservation.Reserve(ctx, r.Client, r.APIReader, pool.Namespace, pool.Name, false)
	if err != nil {
		return false, ctrl.Result{}, err
	}
	if d.Verdict == reservation.Granted {
		logf.FromContext(ctx).Info("Reserved namespace for JobPool", "previousOwner", d.Owner)
		return true, ctrl.Result{}, nil
	}

	message := fmt.Sprintf("Namespace %s is reserved for JobPool %s; only one JobPool per namespace is allowed",
		pool.Namespace, d.Owner)
	if cond := meta.FindStatusCondition(pool.Status.Conditions, schedulingv1alpha1.ConditionReady); cond == nil ||
		cond.Reason != schedulingv1alpha1.ReasonConflict {
		r.Recorder.Eventf(pool, nil, corev1.EventTypeWarning, schedulingv1alpha1.ReasonConflict, "Reserve", "%s", message)
	}
	if err := r.updateStatus(ctx, pool, 0, 0, metav1.ConditionFalse, schedulingv1alpha1.ReasonConflict, message); err != nil {
		return false, ctrl.Result{}, err
	}
	retryAfter := d.RetryAfter
	if d.Verdict == reservation.Taken {
		retryAfter = reservation.TTL
	}
	return false, ctrl.Result{RequeueAfter: retryAfter}, nil
}

// finalize releases the queue of a deleted JobPool, then the reservation of the
// namespace, and removes its finalizer. No new JobPool can appear before the
// reservation is released: the webhook rejects it while this one exists.
//
// A JobPool the namespace is not reserved for has nothing to release: it never
// managed the queue (Conflict), or a previous call has already released the queue
// and the reservation, and a new JobPool may have taken over the queue since.
func (r *JobPoolReconciler) finalize(ctx context.Context, pool *schedulingv1alpha1.JobPool) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(pool, schedulingv1alpha1.Finalizer) {
		return ctrl.Result{}, nil
	}

	if err := r.updateStatus(ctx, pool, pool.Status.ActiveJobs, pool.Status.QueuedJobs,
		metav1.ConditionFalse, schedulingv1alpha1.ReasonTerminating, "JobPool is being deleted"); err != nil {
		return ctrl.Result{}, err
	}
	var ns corev1.Namespace
	if err := r.APIReader.Get(ctx, client.ObjectKey{Name: pool.Namespace}, &ns); client.IgnoreNotFound(err) != nil {
		return ctrl.Result{}, err
	}
	if owner, _ := reservation.Owner(&ns); owner == pool.Name {
		if err := r.releaseQueue(ctx, pool); err != nil {
			return ctrl.Result{}, err
		}
		if err := reservation.Release(ctx, r.Client, r.APIReader, pool.Namespace, pool.Name); err != nil {
			return ctrl.Result{}, err
		}
	}

	base := pool.DeepCopy()
	controllerutil.RemoveFinalizer(pool, schedulingv1alpha1.Finalizer)
	return ctrl.Result{}, client.IgnoreNotFound(r.Patch(ctx, pool, finalizerPatch(base)))
}

// finalizerPatch patches metadata.finalizers with an optimistic lock. A merge patch
// replaces the whole list, so without the lock a finalizer added by someone else
// after our read would be lost; with it the patch fails and the reconcile retries.
func finalizerPatch(base *schedulingv1alpha1.JobPool) client.Patch {
	return client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})
}

// releaseQueue releases the queue of a JobPool being deleted, unless another JobPool
// of the namespace waits to take it over (it got Conflict because the webhook was
// bypassed): then the queue stays, and so does the limit.
func (r *JobPoolReconciler) releaseQueue(ctx context.Context, pool *schedulingv1alpha1.JobPool) error {
	var pools schedulingv1alpha1.JobPoolList
	if err := r.APIReader.List(ctx, &pools, client.InNamespace(pool.Namespace)); err != nil {
		return err
	}
	for _, other := range pools.Items {
		if other.Name != pool.Name && other.DeletionTimestamp.IsZero() {
			logf.FromContext(ctx).Info("Handed queue over to another JobPool", "successor", other.Name)
			return nil
		}
	}
	return r.releaseNamespace(ctx, pool, pool.Namespace)
}

// cleanupNamespace handles a namespace without a JobPool that still has our traces:
// the enabled label, or queued Jobs. A Job can be queued after the finalizer has
// released the queue: its admission request passed the webhook while the JobPool
// still existed, and was stored later. Such a Job triggers a reconcile (see
// SetupWithManager) and is started here.
func (r *JobPoolReconciler) cleanupNamespace(ctx context.Context, namespace string) error {
	// The cache may lag behind a JobPool created a moment ago. Its own create event
	// will reconcile the namespace, so there is nothing to clean up.
	var pools schedulingv1alpha1.JobPoolList
	if err := r.APIReader.List(ctx, &pools, client.InNamespace(namespace)); err != nil {
		return err
	}
	if len(pools.Items) > 0 {
		return nil
	}
	return r.releaseNamespace(ctx, nil, namespace)
}

// releaseNamespace detaches the namespace from the webhook and starts every queued Job.
// The label goes first, so no new Jobs are queued while the queue is released.
// pool is the JobPool being deleted, used for events; nil if it is already gone.
func (r *JobPoolReconciler) releaseNamespace(ctx context.Context, pool *schedulingv1alpha1.JobPool, namespace string) error {
	if err := r.setNamespaceLabel(ctx, namespace, false); err != nil {
		return err
	}

	var jobs batchv1.JobList
	if err := r.APIReader.List(ctx, &jobs, client.InNamespace(namespace),
		client.MatchingLabels{schedulingv1alpha1.QueuedLabel: schedulingv1alpha1.LabelTrue}); err != nil {
		return err
	}
	for i := range jobs.Items {
		job := &jobs.Items[i]
		if err := r.startJob(ctx, job); err != nil {
			return err
		}
		logf.FromContext(ctx).Info("Released Job from queue", "job", job.Name)
		if pool != nil {
			r.Recorder.Eventf(pool, job, corev1.EventTypeNormal, "Released", "ReleaseJob",
				"Released Job %s from queue: JobPool is being deleted", job.Name)
		}
	}
	return nil
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
	value, has := ns.Labels[schedulingv1alpha1.EnabledNamespaceLabel]
	if enabled && value == schedulingv1alpha1.LabelTrue || !enabled && !has {
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
		Watches(&batchv1.Job{}, handler.EnqueueRequestsFromMapFunc(r.jobToPool)).
		// The enabled label belongs to the controller: a manual change is reverted at once.
		// A released or removed reservation lets a JobPool in Conflict take over.
		Watches(&corev1.Namespace{}, handler.EnqueueRequestsFromMapFunc(r.namespaceToPool)).
		Named("jobpool").
		Complete(r)
}

// jobToPool maps a Job to the JobPool of its namespace.
// A queued Job is mapped even without a JobPool: it must not stay queued forever.
func (r *JobPoolReconciler) jobToPool(ctx context.Context, obj client.Object) []reconcile.Request {
	queued := obj.GetLabels()[schedulingv1alpha1.QueuedLabel] == schedulingv1alpha1.LabelTrue
	return r.poolRequests(ctx, obj.GetNamespace(), queued)
}

// namespaceToPool maps a Namespace to its JobPool.
// A labelled namespace is mapped even without a JobPool: the label must be removed.
func (r *JobPoolReconciler) namespaceToPool(ctx context.Context, obj client.Object) []reconcile.Request {
	_, labelled := obj.GetLabels()[schedulingv1alpha1.EnabledNamespaceLabel]
	return r.poolRequests(ctx, obj.GetName(), labelled)
}

// poolRequests returns requests for the JobPools of the namespace in the cache:
// normally one, more only if the webhook was bypassed. Without a JobPool it returns
// nothing, so that every Job of the cluster does not trigger a reconcile with API
// server reads, unless force is set: then it returns a request with an empty name,
// and the reconcile cleans the namespace up.
func (r *JobPoolReconciler) poolRequests(ctx context.Context, namespace string, force bool) []reconcile.Request {
	var pools schedulingv1alpha1.JobPoolList
	if err := r.List(ctx, &pools, client.InNamespace(namespace)); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list JobPools", "namespace", namespace)
	}
	requests := make([]reconcile.Request, 0, len(pools.Items))
	for _, pool := range pools.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&pool)})
	}
	if len(requests) == 0 && force {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKey{Namespace: namespace}})
	}
	return requests
}
