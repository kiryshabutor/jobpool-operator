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
// The only allowed JobPool name is "default", so every request is <namespace>/default.
func (r *JobPoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

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
// No new JobPool can appear meanwhile: it would have the same name as the one
// being deleted, and the API server rejects it until the finalizer is gone.
func (r *JobPoolReconciler) finalize(ctx context.Context, pool *schedulingv1alpha1.JobPool) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(pool, schedulingv1alpha1.Finalizer) {
		return ctrl.Result{}, nil
	}

	if err := r.updateStatus(ctx, pool, pool.Status.ActiveJobs, pool.Status.QueuedJobs,
		metav1.ConditionFalse, schedulingv1alpha1.ReasonTerminating, "JobPool is being deleted"); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.releaseNamespace(ctx, pool, pool.Namespace); err != nil {
		return ctrl.Result{}, err
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

// cleanupNamespace handles a namespace without a JobPool that still has our traces:
// the enabled label, or queued Jobs. A Job can be queued after the finalizer has
// released the queue: its admission request passed the webhook while the JobPool
// still existed, and was stored later. Such a Job triggers a reconcile (see
// SetupWithManager) and is started here.
func (r *JobPoolReconciler) cleanupNamespace(ctx context.Context, namespace string) error {
	// The cache may lag behind a JobPool created a moment ago. Its own create event
	// will reconcile the namespace, so there is nothing to clean up.
	var pool schedulingv1alpha1.JobPool
	err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: schedulingv1alpha1.PoolName}, &pool)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
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
		Watches(&corev1.Namespace{}, handler.EnqueueRequestsFromMapFunc(r.namespaceToPool)).
		Named("jobpool").
		Complete(r)
}

// jobToPool maps a Job to the JobPool of its namespace.
// A queued Job is mapped even without a JobPool: it must not stay queued forever.
func (r *JobPoolReconciler) jobToPool(ctx context.Context, obj client.Object) []reconcile.Request {
	queued := obj.GetLabels()[schedulingv1alpha1.QueuedLabel] == schedulingv1alpha1.LabelTrue
	return r.poolRequest(ctx, obj.GetNamespace(), queued)
}

// namespaceToPool maps a Namespace to its JobPool.
// A labelled namespace is mapped even without a JobPool: the label must be removed.
func (r *JobPoolReconciler) namespaceToPool(ctx context.Context, obj client.Object) []reconcile.Request {
	_, labelled := obj.GetLabels()[schedulingv1alpha1.EnabledNamespaceLabel]
	return r.poolRequest(ctx, obj.GetName(), labelled)
}

// poolRequest returns the request for the JobPool of the namespace if it exists in
// the cache, or if force is set. Without the check every Job of the cluster would
// trigger a reconcile with API server reads.
func (r *JobPoolReconciler) poolRequest(ctx context.Context, namespace string, force bool) []reconcile.Request {
	key := client.ObjectKey{Namespace: namespace, Name: schedulingv1alpha1.PoolName}
	if !force {
		err := r.Get(ctx, key, &schedulingv1alpha1.JobPool{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			logf.FromContext(ctx).Error(err, "Failed to get JobPool", "namespace", namespace)
		}
	}
	return []reconcile.Request{{NamespacedName: key}}
}
