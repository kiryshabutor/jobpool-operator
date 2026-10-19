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
	"fmt"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	schedulingv1alpha1 "github.com/kiryl-b-inno/jobpool-operator/api/v1alpha1"
	"github.com/kiryl-b-inno/jobpool-operator/internal/reservation"
)

func newNamespace() string {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "jobpool-webhook-"}}
	Expect(k8sClient.Create(ctx, ns)).To(Succeed())
	return ns.Name
}

func newPool(namespace, name string) *schedulingv1alpha1.JobPool {
	return &schedulingv1alpha1.JobPool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec:       schedulingv1alpha1.JobPoolSpec{MaxConcurrentJobs: 1},
	}
}

// createPool creates a JobPool through the API server, which calls the webhook.
func createPool(namespace, name string, opts ...client.CreateOption) error {
	return k8sClient.Create(ctx, newPool(namespace, name), opts...)
}

// owner returns the JobPool the namespace is reserved for.
func owner(namespace string) string {
	ns := &corev1.Namespace{}
	ExpectWithOffset(1, k8sClient.Get(ctx, client.ObjectKey{Name: namespace}, ns)).To(Succeed())
	name, _ := reservation.Owner(ns)
	return name
}

// reservedAt returns when the namespace was reserved.
func reservedAt(namespace string) time.Time {
	ns := &corev1.Namespace{}
	ExpectWithOffset(1, k8sClient.Get(ctx, client.ObjectKey{Name: namespace}, ns)).To(Succeed())
	_, at := reservation.Owner(ns)
	return at
}

// reserve writes a reservation as the webhook would have done `age` ago.
func reserve(namespace, pool string, age time.Duration) {
	ns := &corev1.Namespace{}
	ExpectWithOffset(1, k8sClient.Get(ctx, client.ObjectKey{Name: namespace}, ns)).To(Succeed())
	ns.Annotations = map[string]string{
		schedulingv1alpha1.PoolAnnotation:           pool,
		schedulingv1alpha1.PoolReservedAtAnnotation: time.Now().Add(-age).UTC().Format(time.RFC3339),
	}
	ExpectWithOffset(1, k8sClient.Update(ctx, ns)).To(Succeed())
}

var _ = Describe("JobPool Webhook", func() {
	var namespace string

	BeforeEach(func() {
		namespace = newNamespace()
	})

	Context("When going through the API server", func() {
		It("reserves the namespace for the first JobPool", func() {
			Expect(createPool(namespace, "a")).To(Succeed())

			ns := &corev1.Namespace{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: namespace}, ns)).To(Succeed())
			name, reservedAt := reservation.Owner(ns)
			Expect(name).To(Equal("a"))
			Expect(reservedAt).To(BeTemporally("~", time.Now(), time.Minute))
		})

		It("rejects a second JobPool", func() {
			Expect(createPool(namespace, "a")).To(Succeed())

			err := createPool(namespace, "b")
			Expect(err).To(MatchError(ContainSubstring(
				fmt.Sprintf("JobPool %q already exists in namespace %q; only one JobPool per namespace is allowed",
					"a", namespace))))
			Expect(owner(namespace)).To(Equal("a"))
		})

		It("rejects a new JobPool while the old one is being deleted", func() {
			pool := newPool(namespace, "a")
			// A finalizer keeps the deleted JobPool around: it still releases its queue.
			controllerutil.AddFinalizer(pool, schedulingv1alpha1.Finalizer)
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())
			Expect(k8sClient.Delete(ctx, pool)).To(Succeed())
			DeferCleanup(func() {
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pool), pool)).To(Succeed())
				controllerutil.RemoveFinalizer(pool, schedulingv1alpha1.Finalizer)
				Expect(k8sClient.Update(ctx, pool)).To(Succeed())
			})

			Expect(createPool(namespace, "b")).To(MatchError(ContainSubstring(`JobPool "a" already exists`)))
		})

		It("lets exactly one of concurrent JobPools in", func() {
			const n = 10
			errs := make([]error, n)
			var wg sync.WaitGroup
			for i := range n {
				wg.Go(func() {
					errs[i] = createPool(namespace, fmt.Sprintf("pool-%d", i))
				})
			}
			wg.Wait()

			var created []string
			for i, err := range errs {
				if err == nil {
					created = append(created, fmt.Sprintf("pool-%d", i))
				} else {
					Expect(err).To(MatchError(ContainSubstring("only one JobPool per namespace is allowed")))
				}
			}
			Expect(created).To(HaveLen(1))
			var pools schedulingv1alpha1.JobPoolList
			Expect(k8sClient.List(ctx, &pools, client.InNamespace(namespace))).To(Succeed())
			Expect(pools.Items).To(HaveLen(1))
			Expect(owner(namespace)).To(Equal(created[0]))
		})

		It("lets a JobPool in again when its earlier create request failed after the webhook", func() {
			reserve(namespace, "a", time.Second)

			Expect(createPool(namespace, "a")).To(Succeed())
			Expect(owner(namespace)).To(Equal("a"))
		})

		It("renews an old reservation when the same JobPool is created again", func() {
			reserve(namespace, "a", reservation.TTL+time.Minute)

			Expect(createPool(namespace, "a")).To(Succeed())
			Expect(owner(namespace)).To(Equal("a"))
			Expect(reservedAt(namespace)).To(BeTemporally("~", time.Now(), 5*time.Second))
		})

		It("lets one JobPool in when a retried CREATE races another JobPool for an old reservation", func() {
			// Without the renewal the retried CREATE of "a" would not change the Namespace,
			// and "b" would take the old reservation over while "a" is not stored yet.
			const rounds = 5
			for range rounds {
				ns := newNamespace()
				reserve(ns, "a", reservation.TTL+time.Minute)

				var wg sync.WaitGroup
				for _, name := range []string{"a", "b"} {
					wg.Go(func() {
						defer GinkgoRecover()
						err := createPool(ns, name)
						if err != nil {
							Expect(err).To(MatchError(ContainSubstring("only one JobPool per namespace is allowed")))
						}
					})
				}
				wg.Wait()

				var pools schedulingv1alpha1.JobPoolList
				Expect(k8sClient.List(ctx, &pools, client.InNamespace(ns))).To(Succeed())
				Expect(pools.Items).To(HaveLen(1))
				Expect(owner(ns)).To(Equal(pools.Items[0].Name))
			}
		})

		It("rejects a JobPool while the namespace is being reserved for another one", func() {
			// The create request of "a" passed the webhook and is not stored yet.
			reserve(namespace, "a", time.Second)

			Expect(createPool(namespace, "b")).To(MatchError(ContainSubstring(
				fmt.Sprintf("namespace %q is being reserved for JobPool %q", namespace, "a"))))
			Expect(owner(namespace)).To(Equal("a"))
		})

		It("takes over a dead reservation of a JobPool that never appeared", func() {
			reserve(namespace, "ghost", reservation.TTL+time.Minute)

			Expect(createPool(namespace, "b")).To(Succeed())
			Expect(owner(namespace)).To(Equal("b"))
		})

		It("does not write the reservation on dry run", func() {
			Expect(createPool(namespace, "a", client.DryRunAll)).To(Succeed())
			Expect(owner(namespace)).To(BeEmpty())

			Expect(createPool(namespace, "b")).To(Succeed())
			Expect(owner(namespace)).To(Equal("b"))
		})

		It("rejects a JobPool on dry run when the namespace is taken", func() {
			Expect(createPool(namespace, "a")).To(Succeed())

			Expect(createPool(namespace, "b", client.DryRunAll)).To(
				MatchError(ContainSubstring(`JobPool "a" already exists`)))
		})

		It("does not check updates", func() {
			pool := newPool(namespace, "a")
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())

			pool.Spec.MaxConcurrentJobs = 5
			Expect(k8sClient.Update(ctx, pool)).To(Succeed())
		})
	})

	Context("When calling ValidateCreate", func() {
		var validator JobPoolValidator

		BeforeEach(func() {
			validator = JobPoolValidator{Client: k8sClient, APIReader: k8sClient}
		})

		It("uses the namespace of the admission request when the object has none", func() {
			ctx := admission.NewContextWithRequest(ctx, admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{Operation: admissionv1.Create, Namespace: namespace},
			})

			_, err := validator.ValidateCreate(ctx, newPool("", "a"))
			Expect(err).NotTo(HaveOccurred())
			Expect(owner(namespace)).To(Equal("a"))
		})

		It("fails without an admission request", func() {
			_, err := validator.ValidateCreate(ctx, newPool(namespace, "a"))
			Expect(err).To(HaveOccurred())
		})
	})
})
