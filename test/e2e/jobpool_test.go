//go:build e2e
// +build e2e

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

package e2e

import (
	"cmp"
	"encoding/json"
	"fmt"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/kiryl-b-inno/jobpool-operator/test/utils"
)

const (
	queuedLabel  = "jobpool.kiribu.dev/queued"
	enabledLabel = "jobpool.kiribu.dev/enabled"
	poolName     = "default"
	managerName  = "jobpool-operator-controller-manager"
	leaseName    = "b4772dc6.kiribu.dev"

	queueTimeout = 3 * time.Minute
)

// jobPoolScenarios checks the queueing behaviour against a real cluster: real Job
// controller, real pods and the webhooks served by the deployed manager.
// It runs inside the ordered "Manager" container, after the manager is deployed.
func jobPoolScenarios() {
	It("never runs more Jobs than maxConcurrentJobs", func() {
		ns := createNamespace("e2e-limit")
		createPool(ns, 2)

		// All at once: concurrent admission requests must not let extra Jobs through.
		var wg sync.WaitGroup
		errs := make(chan error, 5)
		for range 5 {
			wg.Go(func() {
				_, err := tryCreateJob(ns, 15, false)
				errs <- err
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			Expect(err).NotTo(HaveOccurred())
		}

		Eventually(func(g Gomega) {
			g.Expect(countComplete(listJobs(ns))).To(Equal(5))
		}, queueTimeout, 2*time.Second).Should(Succeed())
		Expect(peakRunning(listJobs(ns))).To(Equal(2), "the pool must run exactly maxConcurrentJobs Jobs at its peak")
	})

	It("starts queued Jobs in creation order", func() {
		ns := createNamespace("e2e-fifo")
		createPool(ns, 1)
		names := make([]string, 0, 4)
		for range 4 {
			names = append(names, createJob(ns, 3, false))
			time.Sleep(time.Second)
		}

		Eventually(func(g Gomega) {
			g.Expect(countComplete(listJobs(ns))).To(Equal(4))
		}, queueTimeout, 2*time.Second).Should(Succeed())

		byName := map[string]batchv1.Job{}
		for _, job := range listJobs(ns) {
			byName[job.Name] = job
		}
		for i := 1; i < len(names); i++ {
			prev, cur := byName[names[i-1]], byName[names[i]]
			Expect(cur.Status.StartTime.Time).NotTo(BeTemporally("<", prev.Status.StartTime.Time),
				"Job %s started before the older Job %s", cur.Name, prev.Name)
		}
	})

	It("does not touch Jobs in namespaces without a JobPool", func() {
		ns := createNamespace("e2e-isolation")
		for range 5 {
			createJob(ns, 15, false)
		}

		for _, job := range listJobs(ns) {
			Expect(job.Spec.Suspend).NotTo(HaveValue(BeTrue()), "Job %s was suspended", job.Name)
			Expect(job.Labels).NotTo(HaveKey(queuedLabel))
		}
		Eventually(func(g Gomega) {
			g.Expect(countActive(listJobs(ns))).To(Equal(5))
		}).Should(Succeed())
	})

	It("allows only one JobPool per namespace", func() {
		ns := createNamespace("e2e-singleton")
		createPool(ns, 1)

		err := tryCreatePool(ns, poolName, 1)
		Expect(err).To(MatchError(ContainSubstring("already exists")))

		err = tryCreatePool(ns, "second", 1)
		Expect(err).To(MatchError(ContainSubstring(
			"JobPool must be named 'default': only one JobPool per namespace is allowed")))
	})

	It("leaves a Job suspended by the user alone", func() {
		ns := createNamespace("e2e-user-pause")
		createPool(ns, 2)
		name := createJob(ns, 1, true)

		Consistently(func(g Gomega) {
			job := getJob(ns, name)
			g.Expect(job.Spec.Suspend).To(HaveValue(BeTrue()))
			g.Expect(job.Labels).NotTo(HaveKey(queuedLabel))
		}, 20*time.Second, 2*time.Second).Should(Succeed())
	})

	It("puts a Job resumed by the user back into the queue", func() {
		ns := createNamespace("e2e-user-resume")
		createPool(ns, 1)
		createJob(ns, 30, false)
		queued := createJob(ns, 1, false)
		paused := createJob(ns, 1, true)
		Eventually(func(g Gomega) {
			g.Expect(countActive(listJobs(ns))).To(Equal(1))
		}).Should(Succeed())

		kubectl("patch", "job", queued, "-n", ns, "--type=merge", "-p", `{"spec":{"suspend":false}}`)
		kubectl("patch", "job", paused, "-n", ns, "--type=merge", "-p", `{"spec":{"suspend":false}}`)

		Consistently(func(g Gomega) {
			jobs := listJobs(ns)
			g.Expect(countActive(jobs)).To(Equal(1))
			g.Expect(countQueued(jobs)).To(Equal(2))
		}, 15*time.Second, time.Second).Should(Succeed())
	})

	It("releases the whole queue when the JobPool is deleted", func() {
		ns := createNamespace("e2e-delete")
		createPool(ns, 1)
		for range 4 {
			createJob(ns, 30, false)
		}
		Eventually(func(g Gomega) {
			g.Expect(countQueued(listJobs(ns))).To(Equal(3))
		}).Should(Succeed())

		kubectl("delete", "jobpool", poolName, "-n", ns)

		Eventually(func(g Gomega) {
			jobs := listJobs(ns)
			g.Expect(countQueued(jobs)).To(BeZero())
			for _, job := range jobs {
				g.Expect(job.Spec.Suspend).NotTo(HaveValue(BeTrue()), "Job %s is still suspended", job.Name)
			}
			g.Expect(namespaceLabels(ns)).NotTo(HaveKey(enabledLabel))
		}).Should(Succeed())
	})

	It("keeps the queue and the limit when the leader fails over", func() {
		ns := createNamespace("e2e-failover")
		createPool(ns, 1)
		for range 3 {
			createJob(ns, 10, false)
		}
		Eventually(func(g Gomega) {
			g.Expect(countActive(listJobs(ns))).To(Equal(1))
		}).Should(Succeed())

		leader := leaderPod()
		kubectl("delete", "pod", leader, "-n", namespace, "--wait=false")
		kubectl("wait", "--for=delete", "pod/"+leader, "-n", namespace, "--timeout=1m")
		// The other replica serves the webhook, even before the replacement pod is ready.
		createJob(ns, 10, false)

		Eventually(func(g Gomega) {
			g.Expect(countComplete(listJobs(ns))).To(Equal(4))
		}, queueTimeout, 2*time.Second).Should(Succeed())
		Expect(peakRunning(listJobs(ns))).To(Equal(1))
		Expect(leaderPod()).NotTo(Equal(leader))
		kubectl("rollout", "status", "deployment", managerName, "-n", namespace, "--timeout=2m")
	})

	// Runs last: it scales the manager down and back up.
	It("blocks Job creation only in JobPool namespaces while the webhook is down", func() {
		pooled := createNamespace("e2e-webhook-down")
		createPool(pooled, 1)
		plain := createNamespace("e2e-webhook-down-plain")

		scaleManager(0)
		// A plain defer, not DeferCleanup: in an Ordered container AfterAll runs before
		// the last spec's DeferCleanup, and it needs the manager back to remove finalizers.
		defer scaleManager(2)
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "pods", "-n", namespace,
				"-l", "control-plane=controller-manager", "-o", "name"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(BeEmpty())
		}).Should(Succeed())

		_, err := tryCreateJob(pooled, 1, false)
		Expect(err).To(MatchError(ContainSubstring("mjob.kiribu.dev")),
			"a Job slipped past the queue while the webhook was down")

		_, err = tryCreateJob(plain, 1, false)
		Expect(err).NotTo(HaveOccurred(), "a namespace without a JobPool must not depend on the webhook")
	})
}

// createNamespace creates a namespace and deletes it after the spec. Deletion waits,
// so the JobPool finalizer is handled while the manager is still deployed.
func createNamespace(name string) string {
	kubectl("create", "ns", name)
	DeferCleanup(func() {
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", name, "--timeout=2m"))
	})
	return name
}

// createPool creates the JobPool and waits until it is Ready: from then on the
// Job webhook is active for the namespace.
func createPool(ns string, maxJobs int) {
	Expect(tryCreatePool(ns, poolName, maxJobs)).To(Succeed())
	kubectl("wait", "--for=condition=Ready", "jobpool/"+poolName, "-n", ns, "--timeout=1m")
	Expect(namespaceLabels(ns)).To(HaveKeyWithValue(enabledLabel, "true"))
}

func tryCreatePool(ns, name string, maxJobs int) error {
	manifest := fmt.Sprintf(`apiVersion: scheduling.kiribu.dev/v1alpha1
kind: JobPool
metadata:
  name: %s
  namespace: %s
spec:
  maxConcurrentJobs: %d
`, name, ns, maxJobs)
	cmd := exec.Command("kubectl", "create", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	// Not utils.Run: it quotes the output with %q, which escapes the API server message.
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%w: %s", err, out)
	}
	return nil
}

// createJob creates a busybox Job sleeping for the given seconds and returns its name.
func createJob(ns string, sleepSeconds int, suspend bool) string {
	name, err := tryCreateJob(ns, sleepSeconds, suspend)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return name
}

func tryCreateJob(ns string, sleepSeconds int, suspend bool) (string, error) {
	manifest := fmt.Sprintf(`apiVersion: batch/v1
kind: Job
metadata:
  generateName: sleep-
  namespace: %s
spec:
  suspend: %t
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: sleep
        image: busybox:1.37
        command: ["sleep", "%d"]
`, ns, suspend, sleepSeconds)
	cmd := exec.Command("kubectl", "create", "-f", "-", "-o", "jsonpath={.metadata.name}")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return "", fmt.Errorf("%w: %s", err, exitErr.Stderr)
		}
		return "", err
	}
	return string(out), nil
}

func listJobs(ns string) []batchv1.Job {
	var list batchv1.JobList
	getJSON(&list, "get", "jobs", "-n", ns)
	return list.Items
}

func getJob(ns, name string) batchv1.Job {
	var job batchv1.Job
	getJSON(&job, "get", "job", name, "-n", ns)
	return job
}

func namespaceLabels(ns string) map[string]string {
	var namespace corev1.Namespace
	getJSON(&namespace, "get", "ns", ns)
	return namespace.Labels
}

// leaderPod returns the name of the manager pod holding the leader election lease.
func leaderPod() string {
	out, err := exec.Command("kubectl", "get", "lease", leaseName, "-n", namespace,
		"-o", "jsonpath={.spec.holderIdentity}").Output()
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	// holderIdentity is "<pod name>_<random id>".
	pod, _, _ := strings.Cut(string(out), "_")
	ExpectWithOffset(1, pod).NotTo(BeEmpty())
	return pod
}

// getJSON reads stdout only: kubectl warnings on stderr would break the JSON.
func getJSON(into any, args ...string) {
	out, err := exec.Command("kubectl", append(args, "-o", "json")...).Output()
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	ExpectWithOffset(1, json.Unmarshal(out, into)).To(Succeed())
}

func kubectl(args ...string) {
	_, err := utils.Run(exec.Command("kubectl", args...))
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
}

func scaleManager(replicas int) {
	kubectl("scale", "deployment", managerName, "-n", namespace, fmt.Sprintf("--replicas=%d", replicas))
	if replicas > 0 {
		kubectl("rollout", "status", "deployment", managerName, "-n", namespace, "--timeout=2m")
	}
}

func finished(job batchv1.Job, condition batchv1.JobConditionType) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == condition && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// countActive counts Jobs that are allowed to run: not suspended and not finished.
func countActive(jobs []batchv1.Job) int {
	n := 0
	for _, job := range jobs {
		suspended := job.Spec.Suspend != nil && *job.Spec.Suspend
		if !suspended && !finished(job, batchv1.JobComplete) && !finished(job, batchv1.JobFailed) {
			n++
		}
	}
	return n
}

func countComplete(jobs []batchv1.Job) int {
	n := 0
	for _, job := range jobs {
		if finished(job, batchv1.JobComplete) {
			n++
		}
	}
	return n
}

func countQueued(jobs []batchv1.Job) int {
	n := 0
	for _, job := range jobs {
		if job.Labels[queuedLabel] == "true" {
			n++
		}
	}
	return n
}

// peakRunning returns the largest number of completed Jobs that ran at the same time.
// It uses status.startTime (reset when a Job is resumed) and status.completionTime,
// so unlike polling it cannot miss a short overlap of more than a second.
func peakRunning(jobs []batchv1.Job) int {
	type event struct {
		at    time.Time
		delta int
	}
	events := make([]event, 0, 2*len(jobs))
	for _, job := range jobs {
		ExpectWithOffset(1, job.Status.StartTime).NotTo(BeNil(), "Job %s has no startTime", job.Name)
		ExpectWithOffset(1, job.Status.CompletionTime).NotTo(BeNil(), "Job %s has no completionTime", job.Name)
		events = append(events, event{job.Status.StartTime.Time, +1}, event{job.Status.CompletionTime.Time, -1})
	}
	// The controller starts the next Job only after the previous one has completed, so
	// within the same second (timestamps have a one-second resolution) completion goes first.
	slices.SortFunc(events, func(a, b event) int {
		if c := a.at.Compare(b.at); c != 0 {
			return c
		}
		return cmp.Compare(a.delta, b.delta)
	})
	peak, running := 0, 0
	for _, e := range events {
		running += e.delta
		peak = max(peak, running)
	}
	return peak
}
