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
	schedulingv1alpha1 "github.com/kiryl-b-inno/jobpool-operator/api/v1alpha1"
)

// Primary returns the JobPool that manages the namespace: the oldest one that
// is not being deleted. It returns nil if there is no such JobPool.
// The validating webhook allows only one JobPool per namespace; Primary is the
// safety net for duplicates created while the webhook was not running.
func Primary(pools []schedulingv1alpha1.JobPool) *schedulingv1alpha1.JobPool {
	var primary *schedulingv1alpha1.JobPool
	for i := range pools {
		pool := &pools[i]
		if pool.DeletionTimestamp != nil {
			continue
		}
		if primary == nil || olderFirst(pool, primary) < 0 {
			primary = pool
		}
	}
	return primary
}
