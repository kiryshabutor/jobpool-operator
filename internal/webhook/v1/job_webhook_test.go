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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	schedulingv1alpha1 "github.com/kiryl-b-inno/jobpool-operator/api/v1alpha1"
)

// userLabels are set by the user; the webhook must keep them.
func userLabels() map[string]string {
	return map[string]string{"app": "demo"}
}

func newNamespace() string {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "job-webhook-"}}
	Expect(k8sClient.Create(ctx, ns)).To(Succeed())
	return ns.Name
}

func newPool(namespace string) *schedulingv1alpha1.JobPool {
	pool := &schedulingv1alpha1.JobPool{
		ObjectMeta: metav1.ObjectMeta{Name: schedulingv1alpha1.PoolName, Namespace: namespace},
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

func suspended(job *batchv1.Job) *batchv1.Job {
	job.Spec.Suspend = new(true)
	return job
}

func expectQueued(job *batchv1.Job) {
	ExpectWithOffset(1, job.Spec.Suspend).To(HaveValue(BeTrue()))
	ExpectWithOffset(1, job.Labels).To(HaveKeyWithValue(schedulingv1alpha1.QueuedLabel, schedulingv1alpha1.LabelTrue))
}

func expectUntouched(job *batchv1.Job) {
	ExpectWithOffset(1, job.Spec.Suspend).To(BeNil())
	ExpectWithOffset(1, job.Labels).NotTo(HaveKey(schedulingv1alpha1.QueuedLabel))
}

func expectRunning(job *batchv1.Job) {
	ExpectWithOffset(1, job.Spec.Suspend).To(HaveValue(BeFalse()))
	ExpectWithOffset(1, job.Labels).NotTo(HaveKey(schedulingv1alpha1.QueuedLabel))
}

// request returns a context with the admission request the webhook server would pass to Default.
func request(op admissionv1.Operation, namespace, user string, oldJob *batchv1.Job) context.Context {
	req := admissionv1.AdmissionRequest{
		Operation: op,
		Namespace: namespace,
		UserInfo:  authenticationv1.UserInfo{Username: user},
	}
	if oldJob != nil {
		raw, err := json.Marshal(oldJob)
		Expect(err).NotTo(HaveOccurred())
		req.OldObject = runtime.RawExtension{Raw: raw}
	}
	return admission.NewContextWithRequest(ctx, admission.Request{AdmissionRequest: req})
}

// resume patches spec.suspend=false as `kubectl patch` would; the operator also removes the label.
func resume(c client.Client, job *batchv1.Job) {
	base := job.DeepCopy()
	job.Spec.Suspend = new(false)
	if c == operatorClient {
		delete(job.Labels, schedulingv1alpha1.QueuedLabel)
	}
	ExpectWithOffset(1, c.Patch(ctx, job, client.MergeFrom(base))).To(Succeed())
}

// emptyCache is a cache that has not seen any JobPool yet.
type emptyCache struct{}

func (emptyCache) Get(_ context.Context, key client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
	return apierrors.NewNotFound(schedulingv1alpha1.GroupVersion.WithResource("jobpools").GroupResource(), key.Name)
}

func (emptyCache) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return nil
}

// staleCache still has the previous JobPool "default", being deleted: the cache of
// this replica has not seen that it is gone and a new one has replaced it.
type staleCache struct{ emptyCache }

func (staleCache) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	*obj.(*schedulingv1alpha1.JobPool) = schedulingv1alpha1.JobPool{ObjectMeta: metav1.ObjectMeta{
		Name:              key.Name,
		Namespace:         key.Namespace,
		UID:               "previous-pool",
		DeletionTimestamp: new(metav1.Now()),
		Finalizers:        []string{schedulingv1alpha1.Finalizer},
	}}
	return nil
}

var _ = Describe("Job Webhook", func() {
	var (
		namespace string
		defaulter JobDefaulter
		create    context.Context
	)

	BeforeEach(func() {
		namespace = newNamespace()
		// Read directly from the API server so a freshly created JobPool is visible at once.
		defaulter = JobDefaulter{Cache: k8sClient, APIReader: k8sClient, OperatorUser: operatorUser}
		create = request(admissionv1.Create, namespace, "user", nil)
	})

	Context("When calling Default on CREATE", func() {
		It("does not touch a Job in a namespace without JobPool", func() {
			job := newJob(namespace)
			Expect(defaulter.Default(create, job)).To(Succeed())
			expectUntouched(job)
		})

		It("queues a Job in a namespace with JobPool", func() {
			newPool(namespace)
			job := newJob(namespace)
			job.Labels = userLabels()

			Expect(defaulter.Default(create, job)).To(Succeed())

			expectQueued(job)
			Expect(job.Labels).To(HaveLen(len(userLabels())+1), "user labels are kept")
		})

		It("does not queue a Job suspended by the user", func() {
			newPool(namespace)
			job := suspended(newJob(namespace))

			Expect(defaulter.Default(create, job)).To(Succeed())

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
			Expect(defaulter.Default(create, job)).To(Succeed())
			expectUntouched(job)
		})

		It("uses the namespace of the admission request when the object has none", func() {
			newPool(namespace)
			job := newJob("")

			Expect(defaulter.Default(create, job)).To(Succeed())
			expectQueued(job)
		})

		It("reads the JobPool from the API server when the cache does not have it yet", func() {
			newPool(namespace)
			defaulter.Cache = emptyCache{}
			job := newJob(namespace)

			Expect(defaulter.Default(create, job)).To(Succeed())
			expectQueued(job)
		})

		It("checks the API server when the cache has a JobPool being deleted", func() {
			newPool(namespace)
			defaulter.Cache = staleCache{}
			job := newJob(namespace)

			Expect(defaulter.Default(create, job)).To(Succeed())
			expectQueued(job)
		})

		It("fails without an admission request", func() {
			Expect(defaulter.Default(ctx, newJob(namespace))).NotTo(Succeed())
		})
	})

	Context("When calling Default on UPDATE", func() {
		BeforeEach(func() {
			newPool(namespace)
		})

		It("puts a Job resumed by a user back into the queue", func() {
			oldJob := suspended(newJob(namespace))
			job := newJob(namespace)
			job.Spec.Suspend = new(false)

			Expect(defaulter.Default(request(admissionv1.Update, namespace, "user", oldJob), job)).To(Succeed())
			expectQueued(job)
		})

		It("puts a resumed Job back into the queue when the cache has a JobPool being deleted", func() {
			defaulter.Cache = staleCache{}
			oldJob := suspended(newJob(namespace))
			job := newJob(namespace)
			job.Spec.Suspend = new(false)

			Expect(defaulter.Default(request(admissionv1.Update, namespace, "user", oldJob), job)).To(Succeed())
			expectQueued(job)
		})

		It("lets the operator resume a Job", func() {
			oldJob := suspended(newJob(namespace))
			job := newJob(namespace)
			job.Spec.Suspend = new(false)

			Expect(defaulter.Default(request(admissionv1.Update, namespace, operatorUser, oldJob), job)).To(Succeed())
			expectRunning(job)
		})

		It("does not touch an update that does not resume the Job", func() {
			oldJob := newJob(namespace)
			job := newJob(namespace)
			job.Labels = userLabels()

			Expect(defaulter.Default(request(admissionv1.Update, namespace, "user", oldJob), job)).To(Succeed())
			Expect(job.Spec.Suspend).To(BeNil())
			Expect(job.Labels).To(Equal(userLabels()))
		})

		It("fails on a malformed old object", func() {
			ctx := admission.NewContextWithRequest(ctx, admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
				Operation: admissionv1.Update,
				Namespace: namespace,
				OldObject: runtime.RawExtension{Raw: []byte("{")},
			}})
			Expect(defaulter.Default(ctx, newJob(namespace))).NotTo(Succeed())
		})
	})

	// Requests go through the real API server and the webhook server of the manager,
	// which reads JobPools from its cache.
	Context("When going through the API server", func() {
		It("stores a new Job already suspended", func() {
			newPool(namespace)
			job := newJob(namespace)

			// One attempt only: if the cache lags, the webhook falls back to the API server.
			Expect(k8sClient.Create(ctx, job)).To(Succeed())
			expectQueued(job)
		})

		It("keeps a queued Job in the queue when a user resumes it", func() {
			newPool(namespace)
			job := newJob(namespace)
			Expect(k8sClient.Create(ctx, job)).To(Succeed())

			resume(k8sClient, job)
			expectQueued(job)
		})

		It("queues a Job suspended by the user when the user resumes it", func() {
			newPool(namespace)
			job := suspended(newJob(namespace))
			Expect(k8sClient.Create(ctx, job)).To(Succeed())
			Expect(job.Labels).NotTo(HaveKey(schedulingv1alpha1.QueuedLabel))

			resume(k8sClient, job)
			expectQueued(job)
		})

		It("lets the operator start a queued Job", func() {
			newPool(namespace)
			job := newJob(namespace)
			Expect(k8sClient.Create(ctx, job)).To(Succeed())

			resume(operatorClient, job)
			expectRunning(job)
		})

		It("lets a user resume a Job in a namespace without JobPool", func() {
			job := suspended(newJob(namespace))
			Expect(k8sClient.Create(ctx, job)).To(Succeed())

			resume(k8sClient, job)
			expectRunning(job)
		})
	})
})
