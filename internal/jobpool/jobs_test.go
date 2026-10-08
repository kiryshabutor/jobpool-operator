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

package jobpool

import (
	"slices"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	schedulingv1alpha1 "github.com/kiryl-b-inno/jobpool-operator/api/v1alpha1"
)

var baseTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// jobOpt modifies a test Job.
type jobOpt func(*batchv1.Job)

// newJob returns a running Job created `age` seconds after baseTime.
func newJob(name string, age int, opts ...jobOpt) batchv1.Job {
	job := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "ns",
			CreationTimestamp: metav1.NewTime(baseTime.Add(time.Duration(age) * time.Second)),
		},
	}
	for _, opt := range opts {
		opt(&job)
	}
	return job
}

// queued makes the Job look like the webhook queued it.
func queued(job *batchv1.Job) {
	job.Spec.Suspend = new(true)
	job.Labels = map[string]string{schedulingv1alpha1.QueuedLabel: schedulingv1alpha1.LabelTrue}
}

// userSuspended makes the Job look like the user paused it.
func userSuspended(job *batchv1.Job) {
	job.Spec.Suspend = new(true)
}

func withCondition(t batchv1.JobConditionType, s corev1.ConditionStatus) jobOpt {
	return func(job *batchv1.Job) {
		job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{Type: t, Status: s})
	}
}

var (
	complete = withCondition(batchv1.JobComplete, corev1.ConditionTrue)
	failed   = withCondition(batchv1.JobFailed, corev1.ConditionTrue)
)

func deleting(job *batchv1.Job) {
	job.DeletionTimestamp = new(metav1.NewTime(baseTime))
}

func TestJobPredicates(t *testing.T) {
	tests := []struct {
		name                                    string
		job                                     batchv1.Job
		wantFinished, wantSuspended, wantActive bool
		wantQueued                              bool
	}{
		{
			name:       "running",
			job:        newJob("a", 0),
			wantActive: true,
		},
		{
			name:       "suspend explicitly false is running",
			job:        newJob("a", 0, func(j *batchv1.Job) { j.Spec.Suspend = new(false) }),
			wantActive: true,
		},
		{
			name:          "queued by webhook",
			job:           newJob("a", 0, queued),
			wantSuspended: true,
			wantQueued:    true,
		},
		{
			name:          "suspended by user",
			job:           newJob("a", 0, userSuspended),
			wantSuspended: true,
		},
		{
			name:         "complete",
			job:          newJob("a", 0, complete),
			wantFinished: true,
		},
		{
			name:         "failed",
			job:          newJob("a", 0, failed),
			wantFinished: true,
		},
		{
			name:       "condition with status False does not finish the job",
			job:        newJob("a", 0, withCondition(batchv1.JobFailed, corev1.ConditionFalse)),
			wantActive: true,
		},
		{
			name:       "SuccessCriteriaMet is not finished yet",
			job:        newJob("a", 0, withCondition(batchv1.JobSuccessCriteriaMet, corev1.ConditionTrue)),
			wantActive: true,
		},
		{
			name:          "finished job with queue label is not queued",
			job:           newJob("a", 0, queued, failed),
			wantFinished:  true,
			wantSuspended: true,
		},
		{
			name:          "queued job being deleted is not queued",
			job:           newJob("a", 0, queued, deleting),
			wantSuspended: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsFinished(&tt.job); got != tt.wantFinished {
				t.Errorf("IsFinished() = %v, want %v", got, tt.wantFinished)
			}
			if got := IsSuspended(&tt.job); got != tt.wantSuspended {
				t.Errorf("IsSuspended() = %v, want %v", got, tt.wantSuspended)
			}
			if got := IsActive(&tt.job); got != tt.wantActive {
				t.Errorf("IsActive() = %v, want %v", got, tt.wantActive)
			}
			if got := IsQueued(&tt.job); got != tt.wantQueued {
				t.Errorf("IsQueued() = %v, want %v", got, tt.wantQueued)
			}
		})
	}
}

func TestSortFIFO(t *testing.T) {
	c, b, a2, a1 := newJob("c", 0), newJob("b", 5), newJob("a2", 3), newJob("a1", 3)
	jobs := []*batchv1.Job{&b, &a2, &c, &a1}

	SortFIFO(jobs)

	if got, want := jobNames(jobs), []string{"c", "a1", "a2", "b"}; !slices.Equal(got, want) {
		t.Errorf("SortFIFO() = %v, want %v", got, want)
	}
}

func TestPlan(t *testing.T) {
	tests := []struct {
		name        string
		jobs        []batchv1.Job
		max         int32
		wantActive  int32
		wantToStart []string
		wantWaiting []string
	}{
		{
			name:        "below the limit starts oldest queued jobs",
			jobs:        []batchv1.Job{newJob("run", 0), newJob("q3", 3, queued), newJob("q1", 1, queued), newJob("q2", 2, queued)},
			max:         3,
			wantActive:  1,
			wantToStart: []string{"q1", "q2"},
			wantWaiting: []string{"q3"},
		},
		{
			name:        "limit reached starts nothing",
			jobs:        []batchv1.Job{newJob("r1", 0), newJob("r2", 0), newJob("q1", 1, queued)},
			max:         2,
			wantActive:  2,
			wantWaiting: []string{"q1"},
		},
		{
			name: "more active than the limit keeps running jobs and starts nothing",
			jobs: []batchv1.Job{
				newJob("r1", 0), newJob("r2", 0), newJob("r3", 0), newJob("q1", 1, queued),
			},
			max:         2,
			wantActive:  3,
			wantWaiting: []string{"q1"},
		},
		{
			name:       "empty queue",
			jobs:       []batchv1.Job{newJob("r1", 0)},
			max:        2,
			wantActive: 1,
		},
		{
			name:        "same creationTimestamp is ordered by name",
			jobs:        []batchv1.Job{newJob("b", 1, queued), newJob("a", 1, queued)},
			max:         1,
			wantToStart: []string{"a"},
			wantWaiting: []string{"b"},
		},
		{
			name: "finished, user-suspended and deleted jobs are ignored",
			jobs: []batchv1.Job{
				newJob("done", 0, complete),
				newJob("fail", 0, queued, failed),
				newJob("user", 0, userSuspended),
				newJob("gone", 0, queued, deleting),
				newJob("q1", 1, queued),
			},
			max:         1,
			wantToStart: []string{"q1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := Plan(tt.jobs, tt.max)

			if plan.Active != tt.wantActive {
				t.Errorf("Active = %d, want %d", plan.Active, tt.wantActive)
			}
			if got := jobNames(plan.ToStart); !slices.Equal(got, tt.wantToStart) {
				t.Errorf("ToStart = %v, want %v", got, tt.wantToStart)
			}
			if got := jobNames(plan.Waiting); !slices.Equal(got, tt.wantWaiting) {
				t.Errorf("Waiting = %v, want %v", got, tt.wantWaiting)
			}
		})
	}
}

func jobNames(jobs []*batchv1.Job) []string {
	names := make([]string, 0, len(jobs))
	for _, job := range jobs {
		names = append(names, job.Name)
	}
	return names
}
