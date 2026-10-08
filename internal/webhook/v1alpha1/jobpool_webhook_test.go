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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	schedulingv1alpha1 "github.com/kiryl-b-inno/jobpool-operator/api/v1alpha1"
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

// These tests create JobPools through the API server, so the request goes through
// the real admission chain and the webhook served by the test manager.
var _ = Describe("JobPool Webhook", func() {
	var namespace string

	BeforeEach(func() {
		namespace = newNamespace()
	})

	It("allows the first JobPool in a namespace", func() {
		Expect(k8sClient.Create(ctx, newPool(namespace, "first"))).To(Succeed())
	})

	It("rejects a second JobPool in the same namespace", func() {
		Expect(k8sClient.Create(ctx, newPool(namespace, "first"))).To(Succeed())

		err := k8sClient.Create(ctx, newPool(namespace, "second"))

		Expect(err).To(MatchError(ContainSubstring(
			`JobPool "first" already exists in namespace "` + namespace + `"; only one JobPool per namespace is allowed`)))
	})

	It("allows JobPools in different namespaces", func() {
		Expect(k8sClient.Create(ctx, newPool(namespace, "pool"))).To(Succeed())
		Expect(k8sClient.Create(ctx, newPool(newNamespace(), "pool"))).To(Succeed())
	})

	It("allows a new JobPool while the old one is being deleted", func() {
		old := newPool(namespace, "old")
		// A finalizer keeps the deleted JobPool around with a deletionTimestamp.
		controllerutil.AddFinalizer(old, schedulingv1alpha1.Finalizer)
		Expect(k8sClient.Create(ctx, old)).To(Succeed())
		Expect(k8sClient.Delete(ctx, old)).To(Succeed())
		DeferCleanup(func() {
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(old), old)).To(Succeed())
			controllerutil.RemoveFinalizer(old, schedulingv1alpha1.Finalizer)
			Expect(k8sClient.Update(ctx, old)).To(Succeed())
		})

		Expect(k8sClient.Create(ctx, newPool(namespace, "new"))).To(Succeed())
	})

	It("does not validate updates and deletion", func() {
		pool := newPool(namespace, "pool")
		validator := JobPoolValidator{}
		Expect(validator.ValidateUpdate(ctx, pool, pool)).Error().NotTo(HaveOccurred())
		Expect(validator.ValidateDelete(ctx, pool)).Error().NotTo(HaveOccurred())
	})
})
