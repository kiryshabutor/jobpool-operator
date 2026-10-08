# E2E guide

A manual, step-by-step check of the operator in a kind cluster, with expected output. The same scenarios are checked
automatically by `make test-e2e` (see [the end](#automated-e2e-tests)).

Before you start, install the operator following the [Quickstart](../README.md#quickstart). All commands run from the
`jobpool-operator` directory of the `jobpool-operator` branch.

```sh
kubectl -n jobpool-operator-system get pods,pdb
# NAME                                                       READY   STATUS    RESTARTS   AGE
# pod/jobpool-operator-controller-manager-6f5c7d6fc6-2j9hh   1/1     Running   0          1m
# pod/jobpool-operator-controller-manager-6f5c7d6fc6-6jvsf   1/1     Running   0          1m
#
# NAME                                                             MIN AVAILABLE   MAX UNAVAILABLE   ALLOWED DISRUPTIONS
# poddisruptionbudget.policy/jobpool-operator-controller-manager   1               N/A               1
```

For convenience, columns that show the queue:

```sh
COLS='NAME:.metadata.name,SUSPEND:.spec.suspend,QUEUED:.metadata.labels.jobpool\.kiribu\.dev/queued,ACTIVE:.status.active,DONE:.status.succeeded'
```

## 1. Limit and queue

```sh
kubectl create ns demo
kubectl -n demo apply -f config/samples/scheduling_v1alpha1_jobpool.yaml   # name: default, maxConcurrentJobs: 2
kubectl -n demo wait --for=condition=Ready jobpool/default
```

`Ready=True` means the controller has set the label and connected the namespace to the webhook. The API server applies
the label using its own Namespace cache, usually immediately:

```sh
kubectl get ns demo --show-labels
# NAME   STATUS   AGE   LABELS
# demo   Active   4s    jobpool.kiribu.dev/enabled=true,kubernetes.io/metadata.name=demo
```

Create 5 Jobs, each sleeps 20 seconds:

```sh
for i in 1 2 3 4 5; do kubectl -n demo create -f config/samples/job_sleep.yaml; done

kubectl -n demo get jp
# NAME      MAX   ACTIVE   QUEUED   READY   REASON   AGE
# default   2     2        3        True    Active   7s

kubectl -n demo get jobs -o custom-columns="$COLS"
# NAME          SUSPEND   QUEUED   ACTIVE   DONE
# sleep-5m55h   true      true     <none>   <none>
# sleep-jdhck   false     <none>   1        <none>
# sleep-jptg7   true      true     <none>   <none>
# sleep-jrf5f   true      true     <none>   <none>
# sleep-kp4jg   false     <none>   1        <none>
```

2 are running, 3 more are suspended with the `queued` label. Watch the queue move (`kubectl -n demo get jobs -w`):
when a Job finishes, the next one by creation time starts. After ~70 seconds all 5 are done:

```sh
kubectl -n demo get jp
# NAME      MAX   ACTIVE   QUEUED   READY   REASON   AGE
# default   2     0        0        True    Active   87s

kubectl -n demo events --for jobpool/default
# 84s   Normal   Started   JobPool/default   Started Job sleep-jdhck from queue
# 84s   Normal   Started   JobPool/default   Started Job sleep-kp4jg from queue
# 57s   Normal   Started   JobPool/default   Started Job sleep-5m55h from queue
# 56s   Normal   Started   JobPool/default   Started Job sleep-jptg7 from queue
# 34s   Normal   Started   JobPool/default   Started Job sleep-jrf5f from queue
```

The first two Jobs went through the queue too: the webhook suspends **every** new Job, and only the controller decides
what starts.

## 2. A namespace without a `JobPool` is unaffected

```sh
kubectl create ns plain
for i in 1 2 3; do kubectl -n plain create -f config/samples/job_sleep.yaml; done

kubectl get ns plain --show-labels
# NAME    STATUS   AGE   LABELS
# plain   Active   5s    kubernetes.io/metadata.name=plain

kubectl -n plain get jobs -o custom-columns="$COLS"
# NAME          SUSPEND   QUEUED   ACTIVE   DONE
# sleep-5xlpd   false     <none>   1        <none>
# sleep-6jjx9   false     <none>   1        <none>
# sleep-fp5w7   false     <none>   1        <none>
```

The namespace has no `enabled` label, so the API server does not even call the webhook. All Jobs start immediately.

## 3. A second `JobPool` in a namespace is rejected

A `JobPool` can only be named `default`, and the API server does not allow two objects with the same name in a
namespace:

```sh
kubectl -n demo create -f config/samples/scheduling_v1alpha1_jobpool.yaml
# Error from server (AlreadyExists): error when creating "config/samples/scheduling_v1alpha1_jobpool.yaml":
# jobpools.scheduling.kiribu.dev "default" already exists

kubectl -n demo apply -f - <<'EOF'
apiVersion: scheduling.kiribu.dev/v1alpha1
kind: JobPool
metadata:
  name: second
spec:
  maxConcurrentJobs: 1
EOF
# The JobPool "second" is invalid: <nil>: Invalid value: JobPool must be named 'default': only one JobPool
# per namespace is allowed
```

`kubectl apply` of the same file with `name: default` does not create a second pool but updates the existing one: it
is the same object.

## 4. A user's suspend is left alone

A Job the user created suspended does not enter the queue, and the operator does not start it:

```sh
kubectl -n demo create job paused --image=busybox:1.37 --dry-run=client -o yaml -- sleep 5 \
  | sed 's/^spec:/spec:\n  suspend: true/' | kubectl create -f -

kubectl -n demo get job paused -o custom-columns="$COLS"
# NAME     SUSPEND   QUEUED   ACTIVE   DONE
# paused   true      <none>   <none>   <none>

kubectl -n demo get jp
# NAME      MAX   ACTIVE   QUEUED   READY   REASON   AGE
# default   2     0        0        True    Active   2m
```

A slot is free (`ACTIVE 0`), but the Job stays suspended: it is the user's suspend.

## 5. Resuming past the queue is not possible

The user resumes their own Job. The webhook puts it into the queue, and the controller starts it as soon as there is a
slot. To see the queue, first take both slots:

```sh
for i in 1 2; do kubectl -n demo create -f config/samples/job_sleep.yaml; done

kubectl -n demo patch job paused -p '{"spec":{"suspend":false}}'
# job.batch/paused patched

kubectl -n demo get job paused -o custom-columns="$COLS"
# NAME     SUSPEND   QUEUED   ACTIVE   DONE
# paused   true      true     <none>   <none>

kubectl -n demo get jp
# NAME      MAX   ACTIVE   QUEUED   READY   REASON   AGE
# default   2     2        1        True    Active   3m
```

The request succeeded without an error, but the Job did not start over the limit: it is queued. After ~20 seconds,
when one of the Jobs finishes, `paused` starts. Only the operator itself may start queued Jobs.

## 6. Leader failure

Only one replica (the leader) reconciles; both serve the webhook. Kill the leader while the queue is not empty:

```sh
for i in 1 2 3 4; do kubectl -n demo create -f config/samples/job_sleep.yaml; done

kubectl -n jobpool-operator-system get lease b4772dc6.kiribu.dev -o jsonpath='{.spec.holderIdentity}{"\n"}'
# jobpool-operator-controller-manager-6f5c7d6fc6-2j9hh_...
kubectl -n jobpool-operator-system delete pod jobpool-operator-controller-manager-6f5c7d6fc6-2j9hh

# meanwhile Jobs are created without errors: the other replica answers the webhook
kubectl -n demo create -f config/samples/job_sleep.yaml
```

A few seconds later the lease belongs to the other replica, and the queue keeps moving: `kubectl -n demo get jp`.

## 7. Deleting the `JobPool` starts the whole queue

```sh
for i in 1 2 3; do kubectl -n demo create -f config/samples/job_sleep.yaml; done
kubectl -n demo get jobs -l jobpool.kiribu.dev/queued=true      # there are queued Jobs

kubectl -n demo delete jobpool default

kubectl -n demo get jobs -l jobpool.kiribu.dev/queued=true
# No resources found in demo namespace.

kubectl get ns demo --show-labels
# NAME   STATUS   AGE     LABELS
# demo   Active   2m33s   kubernetes.io/metadata.name=demo
```

The finalizer removed the label from the namespace and resumed every queued Job. New Jobs in `demo` are now created
unsuspended.

## 8. Operator unavailable: `failurePolicy: Fail`

```sh
kubectl -n demo apply -f config/samples/scheduling_v1alpha1_jobpool.yaml
kubectl -n demo wait --for=condition=Ready jobpool/default    # without this the webhook may not be connected yet
kubectl -n jobpool-operator-system scale deploy jobpool-operator-controller-manager --replicas=0
kubectl -n jobpool-operator-system wait --for=delete pod -l control-plane=controller-manager --timeout=60s

# namespace with a JobPool: creation is rejected (after about 5 s, the webhook's timeoutSeconds)
kubectl -n demo create -f config/samples/job_sleep.yaml
# Error from server (InternalError): error when creating "config/samples/job_sleep.yaml": Internal error occurred:
# failed calling webhook "mjob.kiribu.dev": failed to call webhook: Post "https://jobpool-operator-webhook-service.
# jobpool-operator-system.svc:443/mutate-batch-v1-job?timeout=5s": context deadline exceeded

# namespace without a JobPool: works as usual
kubectl -n plain create -f config/samples/job_sleep.yaml
# job.batch/sleep-nbwbq created
```

Without the operator a Job can neither be queued nor safely admitted: it would start over the limit. So creation is
rejected, but only in namespaces where the admin enabled the limit. Bring the operator back:

```sh
kubectl -n jobpool-operator-system scale deploy jobpool-operator-controller-manager --replicas=2
kubectl -n jobpool-operator-system rollout status deploy/jobpool-operator-controller-manager
```

## Cleanup

```sh
kubectl delete ns demo plain
```

## Automated e2e tests

```sh
make test-e2e
```

What it does:

1. Creates a dedicated kind cluster `jobpool-operator-test-e2e` (override with `KIND_CLUSTER=...`) unless it already
   exists. Its credentials go to a separate kubeconfig `bin/<cluster>.kubeconfig`, and every test command uses only
   that file: the current context in `~/.kube/config` is neither changed nor used.
2. Builds the image, loads it into kind, installs cert-manager and deploys the operator.
3. Runs the tests in `test/e2e/` (~8 minutes, the Jobs really run):
   - the manager is running (2 pods), metrics are served, cert-manager issued the certificate and injected the CA into
     the webhook;
   - limit: pool `max=2`, 5 Jobs created concurrently; the peak of running Jobs is exactly 2 (computed from the Jobs'
     `startTime` and `completionTime`, not by polling), all 5 complete;
   - FIFO: Jobs start (`status.startTime`) in creation order;
   - namespace without a `JobPool`: Jobs start immediately, without the label;
   - a second `default` is rejected (`AlreadyExists`), a pool with another name by the CEL rule;
   - a user's suspend holds for 20 seconds, the label does not appear;
   - a user's resume (of a queued Job and of their own): the Jobs go back into the queue, still 1 running;
   - deleting the `JobPool`: the queue is empty, all Jobs started, the `enabled` label removed;
   - leader failover with a non-empty queue: the new leader continues the queue, the peak is still 1, Jobs can be
     created while only one replica runs;
   - manager at `replicas=0`: a Job in a namespace with a pool is rejected, resuming is rejected too, while
     `kubectl label job` succeeds (`matchConditions`); in a namespace without a pool the Job is created.
4. Deletes the cluster and its kubeconfig.

After a failed run the cluster is kept for debugging; `make cleanup-test-e2e` deletes it.

If kind cannot bring up a cluster while others are running (`kubeadm init` fails with `connection refused`), raise the
inotify limits:

```sh
sudo sysctl -w fs.inotify.max_user_instances=512 fs.inotify.max_user_watches=524288
```
