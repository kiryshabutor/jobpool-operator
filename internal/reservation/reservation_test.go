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

package reservation

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	schedulingv1alpha1 "github.com/kiryl-b-inno/jobpool-operator/api/v1alpha1"
)

var now = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func TestOwner(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		wantOwner   string
		wantTime    time.Time
	}{
		{name: "not reserved"},
		{
			name: "reserved",
			annotations: map[string]string{
				schedulingv1alpha1.PoolAnnotation:           "a",
				schedulingv1alpha1.PoolReservedAtAnnotation: "2026-01-01T12:00:00Z",
			},
			wantOwner: "a",
			wantTime:  now,
		},
		{
			name:        "missing time counts as old",
			annotations: map[string]string{schedulingv1alpha1.PoolAnnotation: "a"},
			wantOwner:   "a",
		},
		{
			name: "malformed time counts as old",
			annotations: map[string]string{
				schedulingv1alpha1.PoolAnnotation:           "a",
				schedulingv1alpha1.PoolReservedAtAnnotation: "yesterday",
			},
			wantOwner: "a",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Annotations: tt.annotations}}
			owner, reservedAt := Owner(ns)
			if owner != tt.wantOwner || !reservedAt.Equal(tt.wantTime) {
				t.Errorf("Owner() = %q, %v; want %q, %v", owner, reservedAt, tt.wantOwner, tt.wantTime)
			}
		})
	}
}

func TestDecide(t *testing.T) {
	tests := []struct {
		name        string
		owner       string
		age         time.Duration
		ownerExists bool
		want        Decision
	}{
		{
			name: "free namespace",
			want: Decision{Verdict: Granted},
		},
		{
			name:  "own reservation, e.g. the create request is retried",
			owner: "a",
			want:  Decision{Verdict: Granted, Owner: "a"},
		},
		{
			name:        "another JobPool exists",
			owner:       "b",
			age:         time.Hour,
			ownerExists: true,
			want:        Decision{Verdict: Taken, Owner: "b"},
		},
		{
			name:        "another JobPool reserved a moment ago",
			owner:       "b",
			age:         time.Second,
			ownerExists: true,
			want:        Decision{Verdict: Taken, Owner: "b"},
		},
		{
			name:  "another JobPool not stored yet",
			owner: "b",
			age:   20 * time.Second,
			want:  Decision{Verdict: Pending, Owner: "b", RetryAfter: 40 * time.Second},
		},
		{
			name:  "dead reservation of a JobPool that never appeared",
			owner: "b",
			age:   TTL,
			want:  Decision{Verdict: Granted, Owner: "b"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Decide(tt.owner, now.Add(-tt.age), "a", tt.ownerExists, now)
			if got != tt.want {
				t.Errorf("Decide() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
