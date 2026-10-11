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

// Package reservation keeps at most one JobPool per namespace.
//
// A namespace is reserved for a JobPool by two annotations on the Namespace: the
// name of the JobPool and the time of the reservation. The JobPool webhook
// reserves the namespace before the JobPool is stored, and the finalizer of the
// JobPool removes the reservation. The Namespace is patched with an optimistic lock
// (resourceVersion), so of two JobPools created at the same moment only one gets
// the reservation: the API server rejects the second patch with a conflict.
package reservation

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	schedulingv1alpha1 "github.com/kiryl-b-inno/jobpool-operator/api/v1alpha1"
)

// TTL is how long a reservation of a JobPool that does not exist is kept. The
// webhook reserves the namespace before the JobPool is stored, so "the JobPool
// does not exist" may mean that its create request is still in flight. Such a
// request is stored or fails well within a minute: validating webhooks run in
// parallel with timeouts of at most 30 s, and the API server ends a request after
// --request-timeout (60 s by default). An older reservation is dead: the create
// request failed after the webhook, or the JobPool was removed past its finalizer.
const TTL = time.Minute

// Verdict says whether a JobPool may hold the namespace.
type Verdict int

const (
	// Granted: the namespace is free, already reserved for this JobPool, or
	// reserved for a JobPool that will never appear.
	Granted Verdict = iota
	// Taken: the namespace is reserved for another JobPool that exists (a JobPool
	// being deleted counts: it still releases its queue).
	Taken
	// Pending: the namespace is reserved for another JobPool that does not exist
	// yet; its create request may still be in flight.
	Pending
)

// Decision is the outcome of Decide.
type Decision struct {
	Verdict Verdict
	// Owner is the JobPool the namespace was reserved for before the decision, "" if none.
	Owner string
	// RetryAfter is set for Pending: the time left until the reservation is dead.
	RetryAfter time.Duration
}

// Owner returns the JobPool the namespace is reserved for and when it was reserved.
// A missing or malformed time is returned as zero, so such a reservation counts as old.
func Owner(ns *corev1.Namespace) (string, time.Time) {
	owner := ns.Annotations[schedulingv1alpha1.PoolAnnotation]
	reservedAt, err := time.Parse(time.RFC3339, ns.Annotations[schedulingv1alpha1.PoolReservedAtAnnotation])
	if err != nil {
		return owner, time.Time{}
	}
	return owner, reservedAt
}

// Decide decides whether the JobPool pool may hold a namespace reserved for owner
// at reservedAt. ownerExists tells whether the JobPool owner exists; it matters
// only when the namespace is reserved for another JobPool.
func Decide(owner string, reservedAt time.Time, pool string, ownerExists bool, now time.Time) Decision {
	d := Decision{Verdict: Granted, Owner: owner}
	switch {
	case owner == "" || owner == pool:
	case ownerExists:
		d.Verdict = Taken
	case now.Sub(reservedAt) < TTL:
		d.Verdict = Pending
		d.RetryAfter = reservedAt.Add(TTL).Sub(now)
	}
	return d
}

// Reserve reserves the namespace for the JobPool pool unless another JobPool holds it.
//
// The Namespace and the owner JobPool are read from reader, which must be the API
// server and not the cache: the optimistic lock needs the latest resourceVersion,
// and a JobPool created a moment ago may not be cached yet. On a conflict (someone
// changed the Namespace after it was read) the decision is made again from fresh
// data. With dryRun the decision is made but nothing is written.
//
// A granted reservation is always written, even when it is already this pool's:
// then its time is renewed. Otherwise a retried CREATE of a pool whose reservation
// is older than TTL would not change the Namespace, and a concurrent CREATE of
// another pool could take the reservation over with the same resourceVersion while
// the first one is still in flight: both would pass. The renewed reservation is
// fresh, so the other pool waits (Pending) instead. A renewal within the same second
// writes nothing, but a reservation that young is not taken over anyway.
func Reserve(ctx context.Context, c client.Client, reader client.Reader, namespace, pool string, dryRun bool) (Decision, error) {
	var d Decision
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		now := time.Now()
		var ns corev1.Namespace
		if err := reader.Get(ctx, client.ObjectKey{Name: namespace}, &ns); err != nil {
			return err
		}
		owner, reservedAt := Owner(&ns)
		ownerExists := false
		if owner != "" && owner != pool {
			var err error
			if ownerExists, err = poolExists(ctx, reader, namespace, owner); err != nil {
				return err
			}
		}

		d = Decide(owner, reservedAt, pool, ownerExists, now)
		if d.Verdict != Granted || dryRun {
			return nil
		}
		base := ns.DeepCopy()
		if ns.Annotations == nil {
			ns.Annotations = map[string]string{}
		}
		ns.Annotations[schedulingv1alpha1.PoolAnnotation] = pool
		ns.Annotations[schedulingv1alpha1.PoolReservedAtAnnotation] = now.UTC().Format(time.RFC3339)
		return c.Patch(ctx, &ns, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	})
	return d, err
}

// Release removes the reservation of the namespace if it belongs to the JobPool pool.
// A reservation of another JobPool is kept: pool has already lost it.
func Release(ctx context.Context, c client.Client, reader client.Reader, namespace, pool string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var ns corev1.Namespace
		if err := reader.Get(ctx, client.ObjectKey{Name: namespace}, &ns); err != nil {
			return client.IgnoreNotFound(err)
		}
		if owner, _ := Owner(&ns); owner != pool {
			return nil
		}
		base := ns.DeepCopy()
		delete(ns.Annotations, schedulingv1alpha1.PoolAnnotation)
		delete(ns.Annotations, schedulingv1alpha1.PoolReservedAtAnnotation)
		err := c.Patch(ctx, &ns, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
		return client.IgnoreNotFound(err)
	})
}

// poolExists reports whether the JobPool exists, including one being deleted.
func poolExists(ctx context.Context, reader client.Reader, namespace, name string) (bool, error) {
	err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &schedulingv1alpha1.JobPool{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	return err == nil, err
}
