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
	"context"
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	schedulingv1alpha1 "github.com/kiryl-b-inno/jobpool-operator/api/v1alpha1"
)

// SetupJobPoolWebhookWithManager registers the webhook for JobPool in the manager.
// JobPools are listed directly from the API server, not from the cache: JobPools
// are created rarely, and a pool created a moment ago may not be cached yet.
func SetupJobPoolWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &schedulingv1alpha1.JobPool{}).
		WithValidator(&JobPoolValidator{Reader: mgr.GetAPIReader()}).
		Complete()
}

// Only CREATE is validated: the namespace of an existing object cannot change.
// +kubebuilder:webhook:path=/validate-scheduling-kiribu-dev-v1alpha1-jobpool,mutating=false,failurePolicy=fail,sideEffects=None,groups=scheduling.kiribu.dev,resources=jobpools,verbs=create,versions=v1alpha1,name=vjobpool.kiribu.dev,admissionReviewVersions=v1

// JobPoolValidator allows only one JobPool per namespace.
// Two JobPools created at the same moment can both pass; the controller then
// treats the older one as primary and marks the other as Duplicate.
type JobPoolValidator struct {
	// Reader lists JobPools of the namespace.
	Reader client.Reader
}

// ValidateCreate rejects a JobPool if the namespace already has a JobPool that is not being deleted.
// A JobPool being deleted does not block a new one: the new pool takes over its queue.
func (v *JobPoolValidator) ValidateCreate(ctx context.Context, obj *schedulingv1alpha1.JobPool) (admission.Warnings, error) {
	namespace := obj.Namespace
	if req, err := admission.RequestFromContext(ctx); err == nil && req.Namespace != "" {
		namespace = req.Namespace
	}

	var pools schedulingv1alpha1.JobPoolList
	if err := v.Reader.List(ctx, &pools, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	for _, pool := range pools.Items {
		if pool.DeletionTimestamp == nil && pool.Name != obj.Name {
			logf.FromContext(ctx).V(1).Info("Rejecting duplicate JobPool",
				"namespace", namespace, "name", obj.Name, "existing", pool.Name)
			return nil, fmt.Errorf("JobPool %q already exists in namespace %q; only one JobPool per namespace is allowed",
				pool.Name, namespace)
		}
	}
	return nil, nil
}

// ValidateUpdate implements admission.Validator. Updates are not validated.
func (v *JobPoolValidator) ValidateUpdate(_ context.Context, _, _ *schedulingv1alpha1.JobPool) (admission.Warnings, error) {
	return nil, nil
}

// ValidateDelete implements admission.Validator. Deletion is not validated.
func (v *JobPoolValidator) ValidateDelete(_ context.Context, _ *schedulingv1alpha1.JobPool) (admission.Warnings, error) {
	return nil, nil
}
