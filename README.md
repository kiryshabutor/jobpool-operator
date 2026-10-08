# jobpool-operator

A Kubernetes operator that limits the number of **concurrently running Jobs** in a namespace.

An admin creates a `JobPool` named `default` with a `maxConcurrentJobs` field in a namespace. From then on, every new
Job in that namespace is created **suspended** (`spec.suspend: true`) and enters a queue. The controller resumes Jobs
in creation order (FIFO) while fewer than the limit are running. Clients create Jobs as usual and know nothing about
the limit: a Job is never rejected, it runs as soon as a slot frees up. Namespaces without a `JobPool` are untouched.

```
kubectl create job ──► mutating webhook ──► etcd ──► controller ──► suspend: false
                       (suspend: true +              (FIFO, at most
                        label queued)                 maxConcurrentJobs)
```

- **Mutating webhook** on Jobs: if the namespace has a `JobPool`, it sets `suspend: true` and the label
  `jobpool.kiribu.dev/queued=true` on a new Job, and on a Job that someone other than the operator resumes.
  It counts nothing; only the controller decides what starts.
- **One `JobPool` per namespace:** the name can only be `default` (a CEL rule in the CRD), and the API server itself
  does not allow two objects with the same name in a namespace.
- **The controller** counts running Jobs, starts the next ones from the queue and writes counters to `status`.
  When a `JobPool` is deleted, a finalizer starts the whole queue.

Why it is built this way: [docs/design.md](docs/design.md).

## Quickstart

### Requirements

- Docker
- [kind](https://kind.sigs.k8s.io/)
- kubectl
- Go 1.26+ (only for the `make` build and test targets)

Versions:

- **Tested** (locally and in CI): kind v0.33.0, Kubernetes 1.37 (`kindest/node:v1.37.0`), cert-manager v1.21.1.
- **Minimum for the operator itself** is Kubernetes 1.30: webhook `matchConditions` and `preStop: sleep` (beta in
  1.30–1.33, enabled by default). On other versions pick a compatible cert-manager from
  [its support table](https://cert-manager.io/docs/releases/); such combinations were not tested.

### Install into kind

```sh
# 0. Code: the solution lives in the jobpool-operator branch
git clone -b jobpool-operator https://github.com/kiryl-b-inno/inno-testtask.git
cd inno-testtask/jobpool-operator

# 1. Cluster
kind create cluster --name jobpool

# 2. cert-manager: issues the TLS certificate for the webhook
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.21.1/cert-manager.yaml
kubectl -n cert-manager wait --for=condition=Available deploy --all --timeout=180s

# 3. Operator image: build and load into kind
make docker-build IMG=jobpool-operator:dev
kind load docker-image jobpool-operator:dev --name jobpool

# 4. CRD, RBAC, webhook and 2 manager replicas
make deploy IMG=jobpool-operator:dev
kubectl -n jobpool-operator-system rollout status deploy/jobpool-operator-controller-manager
```

Instead of steps 3–4 you can apply a single bundled manifest: `make build-installer IMG=...` produces
`dist/install.yaml`.

Steps 1–4 in one go:

```sh
# kind needs higher inotify limits, otherwise kube-proxy crashes with "too many open files"
# (the defaults come back after a reboot)
sudo sysctl -w fs.inotify.max_user_instances=512 fs.inotify.max_user_watches=524288

hack/demo-up.sh                  # recreates the jobpool cluster and deploys the operator from the current code
KEEP_CLUSTER=1 hack/demo-up.sh   # keeps the cluster, only rebuilds and redeploys the operator
```

### Usage

```yaml
# config/samples/scheduling_v1alpha1_jobpool.yaml
apiVersion: scheduling.kiribu.dev/v1alpha1
kind: JobPool
metadata:
  name: default            # the only allowed name
spec:
  maxConcurrentJobs: 2
```

```sh
kubectl create ns demo
kubectl -n demo apply -f config/samples/scheduling_v1alpha1_jobpool.yaml
kubectl -n demo wait --for=condition=Ready jobpool/default   # the controller connected the namespace to the webhook

# 5 Jobs, each sleeps 20 seconds
for i in 1 2 3 4 5; do kubectl -n demo create -f config/samples/job_sleep.yaml; done

kubectl -n demo get jobpool        # short name: jp
# NAME      MAX   ACTIVE   QUEUED   READY   REASON   AGE
# default   2     2        3        True    Active   10s

kubectl -n demo get jobs -w        # Jobs run 2 at a time
kubectl -n demo events --for jobpool/default
```

A step-by-step walkthrough of all cases with expected output: [docs/e2e-guide.md](docs/e2e-guide.md).

### Uninstall

```sh
kubectl delete jobpools --all -A   # pools first: the finalizer starts their queues
make undeploy
kind delete cluster --name jobpool
```

Delete the pools **before** `make undeploy`, while the operator is running. Otherwise the CRD will not go away: its
deletion waits for the finalizer to be removed from each `JobPool`, and nobody is left to remove it.

## Behaviour and limitations

- **Queue instead of rejection.** A Job over the limit is created and waits suspended; it is not rejected.
- **Running Jobs are never stopped.** If more Jobs are already running than the limit allows (for example, they were
  started before the `JobPool` was created), the operator leaves them alone: suspending would kill their pods. It
  simply starts nothing new until the running count drops below the limit.
- **All Jobs in the namespace count,** including those created before the `JobPool`. Finished (`Complete`/`Failed`)
  and suspended Jobs take no slot.
- **The limit is enabled together with the namespace label.** `Ready=True` means the controller has set the label and
  connected the namespace to the webhook. The API server applies `namespaceSelector` using its own Namespace cache,
  which updates asynchronously, usually immediately; there is no strict bound on that delay. Jobs admitted before
  that count as Jobs started before the `JobPool`.
- **Other people's suspends are left alone.** If a user creates a Job with `suspend: true` themselves, it does not enter
  the queue and the operator does not start it, not even when the `JobPool` is deleted.
- **Resuming past the queue is not possible.** If a user resumes (`suspend: false`) their own Job or a queued Job, the
  webhook puts it into the queue: the Job runs when a slot frees up. Only the operator itself may start queued Jobs
  (its user is detected at startup via `SelfSubjectReview`).
- **Jobs from CronJobs and other controllers are queued too:** the webhook does not care who created the Job.
- **One `JobPool` per namespace.** The name must be `default`: a `JobPool` with another name is rejected by the CRD's
  CEL rule, a second `default` by the API server (`AlreadyExists`). A new `JobPool` also cannot be created while the
  old one is still being deleted.
- **Deleting a `JobPool` starts the whole queue,** and the `jobpool.kiribu.dev/enabled` label is removed from the
  namespace. A Job that entered the queue at the moment of deletion is started by the controller as well: it watches
  Jobs with the `queued` label in namespaces without a `JobPool`.
- **The `jobpool.kiribu.dev/enabled` label is owned by the controller.** If you change or remove it by hand, the
  controller immediately restores it (or removes it if the namespace has no `JobPool`).
- **If the operator is unavailable, Jobs cannot be created in connected namespaces** (`failurePolicy: Fail`).
  Otherwise the Job would be admitted unsuspended and start right away, and it cannot be suspended later without
  killing its pods: the limit would be broken. To keep the rest of the cluster unaffected, the webhook is called only
  for namespaces labelled `jobpool.kiribu.dev/enabled=true`. The controller sets this label itself when a `JobPool`
  appears in the namespace. On UPDATE the webhook is called only when a Job is being resumed (`matchConditions`); all
  other Job changes do not depend on the operator. The risk of unavailability is reduced by 2 manager replicas (both
  serve the webhook), a PodDisruptionBudget, a `preStop` pause before the pod stops, and `timeoutSeconds: 5`.
- **The limit counts Jobs, not pods.** One Job with a large `parallelism` can use a lot of resources; limiting pods and
  resources is the job of `ResourceQuota` and `LimitRange`, which work alongside `JobPool`.
- **Leader election provides no fencing.** If an old leader stalls longer than the lease duration (15 s) between
  planning and starting, and then sends its late requests, the limit can be exceeded by the size of its stale plan
  until the extra Jobs finish. This is a general limitation of controller-runtime operators; see
  [docs/design.md](docs/design.md#known-limitations).
- **The operator runs under its own identity.** Only the user the operator runs as (in-cluster, its ServiceAccount)
  can resume queued Jobs. With `make run` that is the user from your kubeconfig.

## Development

```sh
make test       # unit + envtest (controller and webhook)
make lint       # golangci-lint
make test-e2e   # dedicated kind cluster with its own kubeconfig, deploy, scenarios from docs/e2e-guide.md (~8 min)
```

`make deploy` and `make test-e2e` write the image tag into `config/manager/kustomization.yaml`; do not commit that
change.

## Layout

| Path | Contents |
|---|---|
| `api/v1alpha1/` | The `JobPool` type with the name CEL rule; label, finalizer and condition names |
| `internal/jobpool/` | Pure logic without a Kubernetes client: what is active, FIFO, start plan, resume detection |
| `internal/webhook/v1/` | Mutating webhook for Jobs |
| `internal/controller/` | Reconcile, finalizer, cleanup of namespaces without a `JobPool`, watches |
| `config/` | Kustomize manifests: CRD, RBAC, webhook, cert-manager, Deployment, PDB, samples |
| `test/e2e/` | E2E tests on kind |
| `docs/` | Design and the manual e2e guide |

## License

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
