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
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	schedulingv1alpha1 "github.com/kiryl-b-inno/jobpool-operator/api/v1alpha1"
)

const (
	timeout  = 10 * time.Second
	interval = 100 * time.Millisecond
	// settle is how long Consistently checks that the controller does NOT start a Job.
	settle = 2 * time.Second
	// poolName is the name of the JobPool most tests create.
	poolName = "pool"
)

func newNamespace() string {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "jobpool-ctrl-"}}
	Expect(k8sClient.Create(ctx, ns)).To(Succeed())
	return ns.Name
}

func newPool(namespace, name string, maxJobs int32) *schedulingv1alpha1.JobPool {
	return &schedulingv1alpha1.JobPool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec:       schedulingv1alpha1.JobPoolSpec{MaxConcurrentJobs: maxJobs},
	}
}

func createPool(namespace string, maxJobs int32) *schedulingv1alpha1.JobPool {
	pool := newPool(namespace, poolName, maxJobs)
	ExpectWithOffset(1, k8sClient.Create(ctx, pool)).To(Succeed())
	return pool
}

// createJob creates a Job; suspend/queued mimic the user and the webhook.
func createJob(namespace, name string, suspend, queued bool) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: batchv1.JobSpec{
			Suspend: new(suspend),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers:    []corev1.Container{{Name: "main", Image: "busybox"}},
				},
			},
		},
	}
	if queued {
		job.Labels = map[string]string{schedulingv1alpha1.QueuedLabel: schedulingv1alpha1.LabelTrue}
	}
	ExpectWithOffset(1, k8sClient.Create(ctx, job)).To(Succeed())
}

// createQueuedJobs creates Jobs as the webhook would: suspended and labelled.
// Names are zero-padded so FIFO order (same creation second) equals name order.
func createQueuedJobs(namespace string, n int) []string {
	names := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		name := fmt.Sprintf("job-%02d", i)
		createJob(namespace, name, true, true)
		names = append(names, name)
	}
	return names
}

func getJob(g Gomega, namespace, name string) *batchv1.Job {
	job := &batchv1.Job{}
	g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, job)).To(Succeed())
	return job
}

func isRunning(job *batchv1.Job) bool {
	return (job.Spec.Suspend == nil || !*job.Spec.Suspend) && job.Labels[schedulingv1alpha1.QueuedLabel] == ""
}

func isQueued(job *batchv1.Job) bool {
	return job.Spec.Suspend != nil && *job.Spec.Suspend &&
		job.Labels[schedulingv1alpha1.QueuedLabel] == schedulingv1alpha1.LabelTrue
}

// runningJobs returns names of the given Jobs that are running, in order.
func runningJobs(g Gomega, namespace string, names []string) []string {
	var running []string
	for _, name := range names {
		if isRunning(getJob(g, namespace, name)) {
			running = append(running, name)
		}
	}
	return running
}

// markComplete simulates the Job controller finishing a Job. Since Kubernetes 1.31 the
// API server validates Job status: Complete requires startTime, completionTime and
// the SuccessCriteriaMet condition.
func markComplete(namespace, name string) {
	EventuallyWithOffset(1, func(g Gomega) {
		job := getJob(g, namespace, name)
		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.CompletionTime = &now
		job.Status.Succeeded = 1
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastTransitionTime: now},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: now},
		}
		g.Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}, timeout, interval).Should(Succeed())
}

// expectStatus waits for the counters of the active JobPool poolName.
func expectStatus(namespace string, active, queued int32) {
	expectPoolStatus(namespace, poolName, active, queued)
}

func expectPoolStatus(namespace, name string, active, queued int32) {
	EventuallyWithOffset(2, func(g Gomega) {
		pool := &schedulingv1alpha1.JobPool{}
		key := client.ObjectKey{Namespace: namespace, Name: name}
		g.Expect(k8sClient.Get(ctx, key, pool)).To(Succeed())
		g.Expect(pool.Status.ActiveJobs).To(Equal(active))
		g.Expect(pool.Status.QueuedJobs).To(Equal(queued))
		cond := meta.FindStatusCondition(pool.Status.Conditions, schedulingv1alpha1.ConditionReady)
		g.Expect(cond).NotTo(BeNil())
		g.Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		g.Expect(cond.Reason).To(Equal(schedulingv1alpha1.ReasonActive))
	}, timeout, interval).Should(Succeed())
}

// expectConflict waits until the JobPool reports that the namespace belongs to another one.
func expectConflict(namespace, name string) {
	EventuallyWithOffset(1, func(g Gomega) {
		pool := &schedulingv1alpha1.JobPool{}
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, pool)).To(Succeed())
		cond := meta.FindStatusCondition(pool.Status.Conditions, schedulingv1alpha1.ConditionReady)
		g.Expect(cond).NotTo(BeNil())
		g.Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		g.Expect(cond.Reason).To(Equal(schedulingv1alpha1.ReasonConflict))
	}, timeout, interval).Should(Succeed())
}

func namespaceLabels(g Gomega, namespace string) map[string]string {
	ns := &corev1.Namespace{}
	g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: namespace}, ns)).To(Succeed())
	return ns.Labels
}

// owner returns the JobPool the namespace is reserved for.
func owner(g Gomega, namespace string) string {
	ns := &corev1.Namespace{}
	g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: namespace}, ns)).To(Succeed())
	return ns.Annotations[schedulingv1alpha1.PoolAnnotation]
}

// setReservation writes the reservation annotations of the namespace, or removes
// them if pool is "".
func setReservation(namespace, pool string, reservedAt time.Time) {
	EventuallyWithOffset(1, func(g Gomega) {
		ns := &corev1.Namespace{}
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: namespace}, ns)).To(Succeed())
		if ns.Annotations == nil {
			ns.Annotations = map[string]string{}
		}
		if pool == "" {
			delete(ns.Annotations, schedulingv1alpha1.PoolAnnotation)
			delete(ns.Annotations, schedulingv1alpha1.PoolReservedAtAnnotation)
		} else {
			ns.Annotations[schedulingv1alpha1.PoolAnnotation] = pool
			ns.Annotations[schedulingv1alpha1.PoolReservedAtAnnotation] = reservedAt.UTC().Format(time.RFC3339)
		}
		g.Expect(k8sClient.Update(ctx, ns)).To(Succeed())
	}, timeout, interval).Should(Succeed())
}

var _ = Describe("JobPool Controller", func() {
	var namespace string

	BeforeEach(func() {
		namespace = newNamespace()
	})

	It("starts queued Jobs up to the limit", func() {
		createPool(namespace, 2)
		names := createQueuedJobs(namespace, 4)

		Eventually(func(g Gomega) {
			g.Expect(runningJobs(g, namespace, names)).To(Equal(names[:2]))
		}, timeout, interval).Should(Succeed())
		Consistently(func(g Gomega) {
			g.Expect(runningJobs(g, namespace, names)).To(HaveLen(2))
		}, settle, interval).Should(Succeed())
		expectStatus(namespace, 2, 2)
	})

	It("starts the next Job in FIFO order when one finishes", func() {
		createPool(namespace, 2)
		names := createQueuedJobs(namespace, 4)
		Eventually(func(g Gomega) {
			g.Expect(runningJobs(g, namespace, names)).To(Equal(names[:2]))
		}, timeout, interval).Should(Succeed())

		markComplete(namespace, names[0])

		Eventually(func(g Gomega) {
			g.Expect(runningJobs(g, namespace, names)).To(Equal(names[:3]))
		}, timeout, interval).Should(Succeed())
		expectStatus(namespace, 2, 1)
	})

	It("never starts a Job suspended by the user and does not count it", func() {
		createPool(namespace, 1)
		createJob(namespace, "user-paused", true, false)
		names := createQueuedJobs(namespace, 1)

		Eventually(func(g Gomega) {
			g.Expect(runningJobs(g, namespace, names)).To(Equal(names))
		}, timeout, interval).Should(Succeed())
		Consistently(func(g Gomega) {
			job := getJob(g, namespace, "user-paused")
			g.Expect(job.Spec.Suspend).To(HaveValue(BeTrue()))
			g.Expect(job.Labels).NotTo(HaveKey(schedulingv1alpha1.QueuedLabel))
		}, settle, interval).Should(Succeed())
		expectStatus(namespace, 1, 0)
	})

	It("does not suspend running Jobs when they exceed the limit", func() {
		running := []string{"running-1", "running-2", "running-3"}
		for _, name := range running {
			createJob(namespace, name, false, false)
		}
		createPool(namespace, 2)
		names := createQueuedJobs(namespace, 1)

		expectStatus(namespace, 3, 1)
		Consistently(func(g Gomega) {
			g.Expect(runningJobs(g, namespace, running)).To(Equal(running))
			g.Expect(isQueued(getJob(g, namespace, names[0]))).To(BeTrue())
		}, settle, interval).Should(Succeed())

		// 3 -> 2 active: still no free slot.
		markComplete(namespace, running[0])
		expectStatus(namespace, 2, 1)
		Consistently(func(g Gomega) {
			g.Expect(isQueued(getJob(g, namespace, names[0]))).To(BeTrue())
		}, settle, interval).Should(Succeed())

		// 2 -> 1 active: the queued Job can start.
		markComplete(namespace, running[1])
		Eventually(func(g Gomega) {
			g.Expect(runningJobs(g, namespace, names)).To(Equal(names))
		}, timeout, interval).Should(Succeed())
		expectStatus(namespace, 2, 0)
	})

	It("starts more Jobs when the limit is raised", func() {
		pool := createPool(namespace, 1)
		names := createQueuedJobs(namespace, 4)
		Eventually(func(g Gomega) {
			g.Expect(runningJobs(g, namespace, names)).To(Equal(names[:1]))
		}, timeout, interval).Should(Succeed())

		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pool), pool)).To(Succeed())
			pool.Spec.MaxConcurrentJobs = 3
			g.Expect(k8sClient.Update(ctx, pool)).To(Succeed())
		}, timeout, interval).Should(Succeed())

		Eventually(func(g Gomega) {
			g.Expect(runningJobs(g, namespace, names)).To(Equal(names[:3]))
		}, timeout, interval).Should(Succeed())
		expectStatus(namespace, 3, 1)
	})

	It("reserves and labels the namespace and adds the finalizer", func() {
		pool := createPool(namespace, 1)

		Eventually(func(g Gomega) {
			// No webhook runs here, so the controller reserves the namespace itself.
			g.Expect(owner(g, namespace)).To(Equal(poolName))
			g.Expect(namespaceLabels(g, namespace)).To(
				HaveKeyWithValue(schedulingv1alpha1.EnabledNamespaceLabel, schedulingv1alpha1.LabelTrue))
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pool), pool)).To(Succeed())
			g.Expect(pool.Finalizers).To(ContainElement(schedulingv1alpha1.Finalizer))
		}, timeout, interval).Should(Succeed())
	})

	It("releases the whole queue when the JobPool is deleted", func() {
		pool := createPool(namespace, 1)
		createJob(namespace, "user-paused", true, false)
		names := createQueuedJobs(namespace, 3)
		Eventually(func(g Gomega) {
			g.Expect(runningJobs(g, namespace, names)).To(HaveLen(1))
			g.Expect(namespaceLabels(g, namespace)).To(HaveKey(schedulingv1alpha1.EnabledNamespaceLabel))
		}, timeout, interval).Should(Succeed())

		Expect(k8sClient.Delete(ctx, pool)).To(Succeed())

		Eventually(func(g Gomega) {
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(pool), &schedulingv1alpha1.JobPool{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "JobPool should be gone")
		}, timeout, interval).Should(Succeed())
		Eventually(func(g Gomega) {
			g.Expect(runningJobs(g, namespace, names)).To(Equal(names))
			g.Expect(namespaceLabels(g, namespace)).NotTo(HaveKey(schedulingv1alpha1.EnabledNamespaceLabel))
			g.Expect(owner(g, namespace)).To(BeEmpty(), "the reservation should be released")
			g.Expect(getJob(g, namespace, "user-paused").Spec.Suspend).To(HaveValue(BeTrue()))
		}, timeout, interval).Should(Succeed())
	})

	It("keeps the reservation until the JobPool has released its queue", func() {
		// A foreign finalizer keeps the deleted JobPool around after ours is removed.
		const hold = "test.kiribu.dev/hold"
		pool := createPool(namespace, 1)
		names := createQueuedJobs(namespace, 2)
		Eventually(func(g Gomega) {
			g.Expect(runningJobs(g, namespace, names)).To(HaveLen(1))
			g.Expect(owner(g, namespace)).To(Equal(poolName))
		}, timeout, interval).Should(Succeed())
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pool), pool)).To(Succeed())
			controllerutil.AddFinalizer(pool, hold)
			g.Expect(k8sClient.Update(ctx, pool)).To(Succeed())
		}, timeout, interval).Should(Succeed())

		Expect(k8sClient.Delete(ctx, pool)).To(Succeed())

		Eventually(func(g Gomega) {
			g.Expect(runningJobs(g, namespace, names)).To(Equal(names))
			g.Expect(owner(g, namespace)).To(BeEmpty())
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pool), pool)).To(Succeed())
			g.Expect(pool.Finalizers).To(Equal([]string{hold}))
		}, timeout, interval).Should(Succeed())

		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pool), pool)).To(Succeed())
			controllerutil.RemoveFinalizer(pool, hold)
			g.Expect(k8sClient.Update(ctx, pool)).To(Succeed())
		}, timeout, interval).Should(Succeed())
	})

	// The webhook is not running in this suite, which is the case it guards against:
	// the controller must stay correct even if a second JobPool slips past it.
	Context("When the webhook was bypassed", func() {
		It("leaves a second JobPool in Conflict and keeps the limit", func() {
			createPool(namespace, 1)
			Eventually(func(g Gomega) {
				g.Expect(owner(g, namespace)).To(Equal(poolName))
			}, timeout, interval).Should(Succeed())
			Expect(k8sClient.Create(ctx, newPool(namespace, "second", 5))).To(Succeed())
			names := createQueuedJobs(namespace, 3)

			expectConflict(namespace, "second")
			expectStatus(namespace, 1, 2)
			Consistently(func(g Gomega) {
				g.Expect(runningJobs(g, namespace, names)).To(HaveLen(1))
				g.Expect(owner(g, namespace)).To(Equal(poolName))
			}, settle, interval).Should(Succeed())
		})

		It("hands the queue over to the second JobPool when the first is deleted", func() {
			pool := createPool(namespace, 1)
			Eventually(func(g Gomega) {
				g.Expect(owner(g, namespace)).To(Equal(poolName))
			}, timeout, interval).Should(Succeed())
			Expect(k8sClient.Create(ctx, newPool(namespace, "second", 2))).To(Succeed())
			expectConflict(namespace, "second")
			names := createQueuedJobs(namespace, 4)
			Eventually(func(g Gomega) {
				g.Expect(runningJobs(g, namespace, names)).To(Equal(names[:1]))
			}, timeout, interval).Should(Succeed())

			Expect(k8sClient.Delete(ctx, pool)).To(Succeed())

			// The queue is not released: the second JobPool takes it over with its own limit.
			expectPoolStatus(namespace, "second", 2, 2)
			Eventually(func(g Gomega) {
				g.Expect(owner(g, namespace)).To(Equal("second"))
				g.Expect(namespaceLabels(g, namespace)).To(HaveKey(schedulingv1alpha1.EnabledNamespaceLabel))
			}, timeout, interval).Should(Succeed())
			Consistently(func(g Gomega) {
				g.Expect(runningJobs(g, namespace, names)).To(Equal(names[:2]))
			}, settle, interval).Should(Succeed())
		})

		It("keeps the owner's queue when a JobPool in Conflict is deleted", func() {
			createPool(namespace, 1)
			Eventually(func(g Gomega) {
				g.Expect(owner(g, namespace)).To(Equal(poolName))
			}, timeout, interval).Should(Succeed())
			second := newPool(namespace, "second", 5)
			Expect(k8sClient.Create(ctx, second)).To(Succeed())
			expectConflict(namespace, "second")
			names := createQueuedJobs(namespace, 3)
			expectStatus(namespace, 1, 2)

			Expect(k8sClient.Delete(ctx, second)).To(Succeed())

			Eventually(func(g Gomega) {
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(second), &schedulingv1alpha1.JobPool{})
				g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "JobPool second should be gone")
			}, timeout, interval).Should(Succeed())
			Consistently(func(g Gomega) {
				g.Expect(runningJobs(g, namespace, names)).To(HaveLen(1))
				g.Expect(owner(g, namespace)).To(Equal(poolName))
				g.Expect(namespaceLabels(g, namespace)).To(HaveKey(schedulingv1alpha1.EnabledNamespaceLabel))
			}, settle, interval).Should(Succeed())
			expectStatus(namespace, 1, 2)
		})

		It("takes over a dead reservation of a JobPool that does not exist", func() {
			setReservation(namespace, "ghost", time.Now().Add(-time.Hour))

			createPool(namespace, 1)

			expectStatus(namespace, 0, 0)
			Eventually(func(g Gomega) {
				g.Expect(owner(g, namespace)).To(Equal(poolName))
			}, timeout, interval).Should(Succeed())
		})

		It("waits while the namespace is being reserved for another JobPool", func() {
			setReservation(namespace, "ghost", time.Now())

			createPool(namespace, 1)

			expectConflict(namespace, poolName)
			Consistently(func(g Gomega) {
				g.Expect(owner(g, namespace)).To(Equal("ghost"))
			}, settle, interval).Should(Succeed())
		})

		It("restores the reservation when someone removes it", func() {
			createPool(namespace, 1)
			Eventually(func(g Gomega) {
				g.Expect(owner(g, namespace)).To(Equal(poolName))
			}, timeout, interval).Should(Succeed())

			setReservation(namespace, "", time.Time{})

			Eventually(func(g Gomega) {
				g.Expect(owner(g, namespace)).To(Equal(poolName))
			}, timeout, interval).Should(Succeed())
		})
	})

	It("starts a Job queued after the JobPool is gone", func() {
		// The admission request passed the webhook while the JobPool existed and was stored
		// after the finalizer had released the queue.
		createJob(namespace, "late", true, true)

		Eventually(func(g Gomega) {
			g.Expect(runningJobs(g, namespace, []string{"late"})).To(Equal([]string{"late"}))
		}, timeout, interval).Should(Succeed())
	})

	It("removes the enabled label from a namespace without JobPool", func() {
		Eventually(func(g Gomega) {
			ns := &corev1.Namespace{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: namespace}, ns)).To(Succeed())
			ns.Labels[schedulingv1alpha1.EnabledNamespaceLabel] = schedulingv1alpha1.LabelTrue
			g.Expect(k8sClient.Update(ctx, ns)).To(Succeed())
		}, timeout, interval).Should(Succeed())

		Eventually(func(g Gomega) {
			g.Expect(namespaceLabels(g, namespace)).NotTo(HaveKey(schedulingv1alpha1.EnabledNamespaceLabel))
		}, timeout, interval).Should(Succeed())
	})

	It("restores the enabled label when someone changes it", func() {
		createPool(namespace, 1)
		expectLabel := func() {
			EventuallyWithOffset(1, func(g Gomega) {
				g.Expect(namespaceLabels(g, namespace)).To(
					HaveKeyWithValue(schedulingv1alpha1.EnabledNamespaceLabel, schedulingv1alpha1.LabelTrue))
			}, timeout, interval).Should(Succeed())
		}
		setLabel := func(value *string) {
			EventuallyWithOffset(1, func(g Gomega) {
				ns := &corev1.Namespace{}
				g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: namespace}, ns)).To(Succeed())
				if value == nil {
					delete(ns.Labels, schedulingv1alpha1.EnabledNamespaceLabel)
				} else {
					ns.Labels[schedulingv1alpha1.EnabledNamespaceLabel] = *value
				}
				g.Expect(k8sClient.Update(ctx, ns)).To(Succeed())
			}, timeout, interval).Should(Succeed())
		}
		expectLabel()

		setLabel(new("false"))
		expectLabel()

		setLabel(nil)
		expectLabel()
	})
})
