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
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	schedulingv1alpha1 "github.com/kiryl-b-inno/jobpool-operator/api/v1alpha1"
	"github.com/kiryl-b-inno/jobpool-operator/internal/reservation"
)

// SetupJobPoolWebhookWithManager registers the webhook for JobPool in the manager.
func SetupJobPoolWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &schedulingv1alpha1.JobPool{}).
		WithValidator(&JobPoolValidator{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader()}).
		Complete()
}

// Only CREATE is validated: the name and namespace of an existing JobPool cannot change.
// sideEffects=NoneOnDryRun: the webhook writes the reservation to the Namespace, but not on dry run.
// +kubebuilder:webhook:path=/validate-scheduling-kiribu-dev-v1alpha1-jobpool,mutating=false,failurePolicy=fail,sideEffects=NoneOnDryRun,groups=scheduling.kiribu.dev,resources=jobpools,verbs=create,versions=v1alpha1,timeoutSeconds=5,name=vjobpool.kiribu.dev,admissionReviewVersions=v1

// JobPoolValidator allows only one JobPool per namespace. It reserves the namespace
// for the new JobPool (see package reservation) and rejects the JobPool if the
// namespace is reserved for another one.
//
// Listing the JobPools of the namespace is not enough: two JobPools created at the
// same moment would both see an empty namespace and both pass. The reservation is
// a write with an optimistic lock on the Namespace, so only one of them wins.
type JobPoolValidator struct {
	// Client writes the reservation to the Namespace.
	Client client.Client
	// APIReader reads the Namespace and JobPools from the API server, bypassing the cache.
	APIReader client.Reader
}

// ValidateCreate reserves the namespace for the JobPool or rejects the JobPool.
func (v *JobPoolValidator) ValidateCreate(ctx context.Context, pool *schedulingv1alpha1.JobPool) (admission.Warnings, error) {
	req, err := admission.RequestFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// The object may have no metadata.namespace (a manifest applied with -n);
	// the namespace of the request itself is always set.
	namespace := req.Namespace
	dryRun := req.DryRun != nil && *req.DryRun

	d, err := reservation.Reserve(ctx, v.Client, v.APIReader, namespace, pool.Name, dryRun)
	if err != nil {
		return nil, err
	}
	log := logf.FromContext(ctx).WithValues("namespace", namespace, "name", pool.Name, "owner", d.Owner)
	switch d.Verdict {
	case reservation.Taken:
		log.V(1).Info("Rejected JobPool: namespace is reserved for another JobPool")
		return nil, fmt.Errorf("JobPool %q already exists in namespace %q; only one JobPool per namespace is allowed",
			d.Owner, namespace)
	case reservation.Pending:
		log.V(1).Info("Rejected JobPool: namespace is being reserved for another JobPool")
		return nil, fmt.Errorf("namespace %q is being reserved for JobPool %q; only one JobPool per namespace "+
			"is allowed. If JobPool %q does not appear, retry in %s", namespace, d.Owner, d.Owner,
			d.RetryAfter.Round(time.Second))
	}
	if dryRun {
		log.V(1).Info("Admitted JobPool on dry run without reserving the namespace")
	} else {
		log.V(1).Info("Reserved namespace for JobPool")
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
