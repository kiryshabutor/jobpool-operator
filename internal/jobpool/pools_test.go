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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	schedulingv1alpha1 "github.com/kiryl-b-inno/jobpool-operator/api/v1alpha1"
)

func newPool(name string, age int, deleting bool) schedulingv1alpha1.JobPool {
	pool := schedulingv1alpha1.JobPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "ns",
			CreationTimestamp: metav1.NewTime(baseTime.Add(time.Duration(age) * time.Second)),
		},
	}
	if deleting {
		pool.DeletionTimestamp = new(metav1.NewTime(baseTime))
	}
	return pool
}

func TestPrimary(t *testing.T) {
	tests := []struct {
		name  string
		pools []schedulingv1alpha1.JobPool
		want  string // empty means nil
	}{
		{
			name: "no pools",
		},
		{
			name:  "single pool",
			pools: []schedulingv1alpha1.JobPool{newPool("a", 0, false)},
			want:  "a",
		},
		{
			name:  "oldest pool wins",
			pools: []schedulingv1alpha1.JobPool{newPool("young", 5, false), newPool("old", 1, false)},
			want:  "old",
		},
		{
			name:  "same creationTimestamp is ordered by name",
			pools: []schedulingv1alpha1.JobPool{newPool("b", 1, false), newPool("a", 1, false)},
			want:  "a",
		},
		{
			name:  "deleting oldest pool hands over to the next one",
			pools: []schedulingv1alpha1.JobPool{newPool("old", 1, true), newPool("young", 5, false)},
			want:  "young",
		},
		{
			name:  "all pools are being deleted",
			pools: []schedulingv1alpha1.JobPool{newPool("a", 1, true), newPool("b", 5, true)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Primary(tt.pools)
			var gotName string
			if got != nil {
				gotName = got.Name
			}
			if gotName != tt.want {
				t.Errorf("Primary() = %q, want %q", gotName, tt.want)
			}
		})
	}
}
