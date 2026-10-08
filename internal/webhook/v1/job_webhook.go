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

package v1

import (
	"context"
	"encoding/json"
	"fmt"

	admissionv1 "k8s.io/api/admission/v1"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	schedulingv1alpha1 "github.com/kiryl-b-inno/jobpool-operator/api/v1alpha1"
	"github.com/kiryl-b-inno/jobpool-operator/internal/jobpool"
)

// SetupJobWebhookWithManager registers the webhook for Job in the manager.
// operatorUser is the username of the operator itself: only it may resume queued Jobs.
func SetupJobWebhookWithManager(mgr ctrl.Manager, operatorUser string) error {
	return ctrl.NewWebhookManagedBy(mgr, &batchv1.Job{}).
		WithDefaulter(&JobDefaulter{
			Cache:        mgr.GetClient(),
			APIReader:    mgr.GetAPIReader(),
			OperatorUser: operatorUser,
		}).
		Complete()
}

// On UPDATE the webhook only re-queues a Job that someone else resumes; it never
// suspends a running Job, since that would kill its pods.
// namespaceSelector (namespaces with a JobPool only) and matchConditions (only
// CREATE and resuming UPDATE) cannot be set by a marker and are added by
// config/default/job_webhook_patch.yaml.
// +kubebuilder:webhook:path=/mutate-batch-v1-job,mutating=true,failurePolicy=fail,sideEffects=None,groups=batch,resources=jobs,verbs=create;update,versions=v1,name=mjob.kiribu.dev,admissionReviewVersions=v1,timeoutSeconds=5

// JobDefaulter puts Jobs of a namespace with a JobPool into the queue
// (spec.suspend=true and the queued label):
//   - every new Job, unless the user creates it suspended;
//   - every Job resumed by anyone but the operator, so nobody can start a Job past the limit.
//
// It never decides which Job runs; the JobPool controller starts queued Jobs
// one by one within the limit.
type JobDefaulter struct {
	// Cache is the manager's cache: the webhook is on the hot path of every Job
	// creation and must answer fast.
	Cache client.Reader
	// APIReader reads the JobPool from the API server when the cache does not have it yet.
	APIReader client.Reader
	// OperatorUser is the username the operator talks to the API server with.
	OperatorUser string
}

// Default implements admission.Defaulter so a webhook will be registered for the Kind Job.
func (d *JobDefaulter) Default(ctx context.Context, job *batchv1.Job) error {
	req, err := admission.RequestFromContext(ctx)
	if err != nil {
		return err
	}
	namespace := requestNamespace(req, job)
	log := logf.FromContext(ctx).WithValues("namespace", namespace, "name", job.Name, "generateName", job.GenerateName)

	switch req.Operation {
	case admissionv1.Create:
		if jobpool.IsSuspended(job) {
			log.V(1).Info("Job is suspended by the user, not queueing")
			return nil
		}
	case admissionv1.Update:
		if req.UserInfo.Username == d.OperatorUser {
			return nil
		}
		var oldJob batchv1.Job
		if err := json.Unmarshal(req.OldObject.Raw, &oldJob); err != nil {
			return fmt.Errorf("decoding the old Job: %w", err)
		}
		if !jobpool.IsResumed(&oldJob, job) {
			return nil
		}
	default:
		return nil
	}

	pool, err := d.getPool(ctx, namespace)
	if err != nil {
		return err
	}
	if pool == nil || !pool.DeletionTimestamp.IsZero() {
		log.V(1).Info("No active JobPool in namespace, not queueing")
		return nil
	}

	jobpool.Enqueue(job)
	log.V(1).Info("Job queued", "operation", req.Operation, "user", req.UserInfo.Username)
	return nil
}

// getPool returns the JobPool of the namespace, or nil if there is none.
// The cache is trusted only when it has a live JobPool. Otherwise this replica's
// cache may lag behind, and the API server is asked:
//   - no JobPool in the cache: it may have been created a moment ago;
//   - a JobPool being deleted: a new "default" may have replaced it already.
//
// Both happen only while a JobPool is created or deleted, so the extra read is rare.
func (d *JobDefaulter) getPool(ctx context.Context, namespace string) (*schedulingv1alpha1.JobPool, error) {
	key := client.ObjectKey{Namespace: namespace, Name: schedulingv1alpha1.PoolName}
	var pool schedulingv1alpha1.JobPool
	err := d.Cache.Get(ctx, key, &pool)
	if apierrors.IsNotFound(err) || err == nil && !pool.DeletionTimestamp.IsZero() {
		pool = schedulingv1alpha1.JobPool{}
		err = d.APIReader.Get(ctx, key, &pool)
	}
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &pool, nil
}

// requestNamespace returns the namespace the Job is created in. The object in an
// admission request may have an empty metadata.namespace (e.g. a manifest without
// it, applied with -n); the namespace of the request itself is always set.
func requestNamespace(req admission.Request, job *batchv1.Job) string {
	if req.Namespace != "" {
		return req.Namespace
	}
	return job.Namespace
}
