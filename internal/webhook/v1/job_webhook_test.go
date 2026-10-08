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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	admissionv1 "k8s.io/api/admission/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	schedulingv1alpha1 "github.com/kiryl-b-inno/jobpool-operator/api/v1alpha1"
)

func newNamespace() string {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "job-webhook-"}}
	Expect(k8sClient.Create(ctx, ns)).To(Succeed())
	return ns.Name
}

func newPool(namespace string) *schedulingv1alpha1.JobPool {
	pool := &schedulingv1alpha1.JobPool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool", Namespace: namespace},
		Spec:       schedulingv1alpha1.JobPoolSpec{MaxConcurrentJobs: 1},
	}
	Expect(k8sClient.Create(ctx, pool)).To(Succeed())
	return pool
}

func newJob(namespace string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "job-", Namespace: namespace},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers:    []corev1.Container{{Name: "main", Image: "busybox"}},
				},
			},
		},
	}
}

func expectQueued(job *batchv1.Job) {
	ExpectWithOffset(1, job.Spec.Suspend).To(HaveValue(BeTrue()))
	ExpectWithOffset(1, job.Labels).To(HaveKeyWithValue(schedulingv1alpha1.QueuedLabel, schedulingv1alpha1.LabelTrue))
}

func expectUntouched(job *batchv1.Job) {
	ExpectWithOffset(1, job.Spec.Suspend).To(BeNil())
	ExpectWithOffset(1, job.Labels).NotTo(HaveKey(schedulingv1alpha1.QueuedLabel))
}

var _ = Describe("Job Webhook", func() {
	var (
		namespace string
		defaulter JobDefaulter
	)

	BeforeEach(func() {
		namespace = newNamespace()
		// Read directly from the API server so a freshly created JobPool is visible at once.
		defaulter = JobDefaulter{Reader: k8sClient}
	})

	Context("When calling Default", func() {
		It("does not touch a Job in a namespace without JobPool", func() {
			job := newJob(namespace)
			Expect(defaulter.Default(ctx, job)).To(Succeed())
			expectUntouched(job)
		})

		It("queues a Job in a namespace with JobPool", func() {
			newPool(namespace)
			job := newJob(namespace)
			job.Labels = map[string]string{"app": "demo"}

			Expect(defaulter.Default(ctx, job)).To(Succeed())

			expectQueued(job)
			Expect(job.Labels).To(HaveKeyWithValue("app", "demo"), "user labels are kept")
		})

		It("does not queue a Job suspended by the user", func() {
			newPool(namespace)
			job := newJob(namespace)
			job.Spec.Suspend = new(true)

			Expect(defaulter.Default(ctx, job)).To(Succeed())

			Expect(job.Spec.Suspend).To(HaveValue(BeTrue()))
			Expect(job.Labels).NotTo(HaveKey(schedulingv1alpha1.QueuedLabel))
		})

		It("does not queue a Job when the JobPool is being deleted", func() {
			pool := newPool(namespace)
			// A finalizer keeps the deleted JobPool around with a deletionTimestamp.
			controllerutil.AddFinalizer(pool, schedulingv1alpha1.Finalizer)
			Expect(k8sClient.Update(ctx, pool)).To(Succeed())
			Expect(k8sClient.Delete(ctx, pool)).To(Succeed())
			DeferCleanup(func() {
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pool), pool)).To(Succeed())
				controllerutil.RemoveFinalizer(pool, schedulingv1alpha1.Finalizer)
				Expect(k8sClient.Update(ctx, pool)).To(Succeed())
			})

			job := newJob(namespace)
			Expect(defaulter.Default(ctx, job)).To(Succeed())
			expectUntouched(job)
		})

		It("uses the namespace of the admission request when the object has none", func() {
			newPool(namespace)
			job := newJob("")
			reqCtx := admission.NewContextWithRequest(context.Background(), admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{Namespace: namespace},
			})

			Expect(defaulter.Default(reqCtx, job)).To(Succeed())
			expectQueued(job)
		})
	})

	Context("When creating a Job through the API server", func() {
		It("stores the Job already suspended", func() {
			newPool(namespace)

			// The webhook server reads JobPools from the manager's cache, which may lag a moment.
			Eventually(func(g Gomega) {
				job := newJob(namespace)
				g.Expect(k8sClient.Create(ctx, job)).To(Succeed())
				g.Expect(job.Spec.Suspend).To(HaveValue(BeTrue()))
				g.Expect(job.Labels).To(HaveKeyWithValue(schedulingv1alpha1.QueuedLabel, schedulingv1alpha1.LabelTrue))
			}).Should(Succeed())
		})
	})
})
