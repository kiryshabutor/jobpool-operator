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

	batchv1 "k8s.io/api/batch/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	schedulingv1alpha1 "github.com/kiryl-b-inno/jobpool-operator/api/v1alpha1"
	"github.com/kiryl-b-inno/jobpool-operator/internal/jobpool"
)

// SetupJobWebhookWithManager registers the webhook for Job in the manager.
// JobPools are read from the manager's cache: the webhook is on the hot path of
// every Job creation and must answer fast.
func SetupJobWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &batchv1.Job{}).
		WithDefaulter(&JobDefaulter{Reader: mgr.GetClient()}).
		Complete()
}

// The webhook only handles CREATE: suspending a running Job on UPDATE would kill its pods.
// namespaceSelector (namespaces with a JobPool only) cannot be set by a marker and is
// added by config/default/job_webhook_namespace_selector_patch.yaml.
// +kubebuilder:webhook:path=/mutate-batch-v1-job,mutating=true,failurePolicy=fail,sideEffects=None,groups=batch,resources=jobs,verbs=create,versions=v1,name=mjob.kiribu.dev,admissionReviewVersions=v1,timeoutSeconds=5

// JobDefaulter puts every new Job of a namespace with a JobPool into the queue:
// it sets spec.suspend=true and the queued label. It never decides which Job
// runs; the JobPool controller starts queued Jobs one by one within the limit.
type JobDefaulter struct {
	// Reader lists JobPools of the Job's namespace.
	Reader client.Reader
}

// Default implements admission.Defaulter so a webhook will be registered for the Kind Job.
func (d *JobDefaulter) Default(ctx context.Context, job *batchv1.Job) error {
	namespace := requestNamespace(ctx, job)
	log := logf.FromContext(ctx).WithValues("namespace", namespace, "name", job.Name, "generateName", job.GenerateName)

	if jobpool.IsSuspended(job) {
		log.V(1).Info("Job is suspended by the user, not queueing")
		return nil
	}

	var pools schedulingv1alpha1.JobPoolList
	if err := d.Reader.List(ctx, &pools, client.InNamespace(namespace)); err != nil {
		return err
	}
	pool := jobpool.Primary(pools.Items)
	if pool == nil {
		log.V(1).Info("No active JobPool in namespace, not queueing")
		return nil
	}

	job.Spec.Suspend = new(true)
	if job.Labels == nil {
		job.Labels = map[string]string{}
	}
	job.Labels[schedulingv1alpha1.QueuedLabel] = schedulingv1alpha1.LabelTrue
	log.V(1).Info("Job queued", "jobPool", pool.Name)
	return nil
}

// requestNamespace returns the namespace the Job is created in. The object in an
// admission request may have an empty metadata.namespace (e.g. a manifest without
// it, applied with -n); the namespace of the request itself is always set.
func requestNamespace(ctx context.Context, job *batchv1.Job) string {
	if req, err := admission.RequestFromContext(ctx); err == nil && req.Namespace != "" {
		return req.Namespace
	}
	return job.Namespace
}
