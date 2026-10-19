# JobPool design

## Problem

Too many Jobs are created in one namespace, and they eat up cluster resources. The number of **concurrently active**
Jobs in a namespace has to be limited with a CRD and a controller.

Constraints:
- the limit applies only in the namespace where the `JobPool` is created;
- at most one `JobPool` per namespace;
- **active** Jobs are counted: finished ones take no slot.

## Overview

```
kubectl create jobpool
   → validating webhook vjobpool.kiribu.dev   namespace free? → reserve it (annotation on the Namespace)
   → etcd
   → JobPool controller                       finalizer, label enabled on the namespace

kubectl create job
   → API server: authentication, RBAC
   → mutating webhook mjob.kiribu.dev         JobPool in namespace? → suspend: true + label queued
   → etcd
   → JobPool controller                       active < max? → first queued ones: suspend: false, remove label
   → Kubernetes Job controller creates pods
```

## Decisions

### D1. A Job over the limit is queued, not rejected

With rejection, every client has to retry: humans, CI, other operators. With a queue all the logic stays inside the
operator: the client creates a Job as usual, and it **eventually** runs. This is the natural Kubernetes model: the
system drives the state towards the desired one by itself.

### D2. The queue is `spec.suspend`

`suspend: true` at creation: the Job exists, but no pods are created. `true → false`: the Job starts. A built-in
mechanism, nothing custom needed. Important: `false → true` on a **running** Job kills its pods, so we only suspend new
Jobs and Jobs that have not started yet.

### D3. The suspend is set by a mutating webhook, not by reconcile

The controller sees a Job only once it is in etcd. The built-in Job controller sees it at the same moment and creates
pods right away. That is a race we can lose. The webhook changes the Job **before** it is written to etcd, so the Job
controller already sees it suspended.

### D4. The webhook suspends every new Job; only the controller decides

If the webhook counted free slots itself, two concurrent requests would both see "2 of 3" and admit both Jobs: 4
running with a limit of 3. The controller processes a `JobPool` sequentially (one reconcile per key, plus leader
election). One decision point, a stateless webhook. The price is a small delay before start, even when a slot is free.

### D5. The webhook checks whether the namespace has a `JobPool`

Without a pool the Job passes unchanged; otherwise it would stay suspended forever. The namespace comes **from the
admission request**, not from the object: a manifest may have no `metadata.namespace` (`kubectl apply -n x`). A
`JobPool` being deleted (`deletionTimestamp`) counts as absent.

The pool is read from the cache: the webhook is on the path of every Job and must answer quickly. The webhook trusts
the cache only if it holds a live pool. In two cases this replica's cache may lag, and the webhook asks the API server
directly, once:

- **the pool is not in the cache:** it may have been created a split second ago;
- **the cache holds only a pool with a `deletionTimestamp`:** the old pool may already be gone and a new one created in
  its place. Trusting the cache would let the Job bypass the new pool's queue.

Both cases only happen while a pool is being created or deleted, and the webhook is called only for labelled
namespaces (D14), so the extra request is rare. The opposite error is safe: if the cache still shows a live pool that
no longer exists, the Job is queued without a pool, and the controller releases it (D10).

### D6. Our Jobs are marked with a label; someone else's suspend is left alone

A user may create a Job with `suspend: true` to start it later. The webhook leaves such a Job alone. Our suspend is
marked with the label `jobpool.kiribu.dev/queued=true`. A label rather than an annotation, because labels can be
filtered on (`List` with `MatchingLabels`). The queue is the set of Jobs with this label.

### D7. Active Job = not suspended and not finished

A Job has no `phase`. It is active if `spec.suspend != true` and it has no `Complete`/`Failed` = True condition.
Counting by `status.active` does not work: for a just-started Job it stays 0 for a while, and the controller would
start one too many. `spec.suspend` is changed by us, so the change is visible immediately. **All** Jobs in the
namespace are counted, including those created before the pool.

### D8. The order is FIFO

Sort by `creationTimestamp`, ties broken by name (timestamps have one-second precision). Jobs being deleted are not
started from the queue.

### D9. Running Jobs over the limit are left alone

If more Jobs are already active than the limit (started before the `JobPool`, or the limit was lowered), suspending
would kill their pods. We just start nothing until the active count drops below the limit:
`free = max(0, max − active)`.

### D10. Deleting a `JobPool` → the finalizer releases the queue

Without a finalizer, queued Jobs would stay suspended forever. Finalizer `jobpool.kiribu.dev/release-queue`:

```
status Ready=False, reason=Terminating
1. remove the enabled label from the namespace (the webhook is no longer called)
2. resume all Jobs with the queued label
3. release the reservation of the namespace (D11)
4. remove the finalizer
```

The label goes first so that new Jobs stop being queued. But an admission request that passed the webhook before the
label was removed may be persisted to etcd **after** step 2: such a Job ends up in a queue that no longer exists. A
re-check a second later does not guarantee anything: in-flight requests have no known upper bound. So the controller
watches Jobs with the `queued` label in namespaces **without** a `JobPool`: an event for such a Job triggers a
reconcile of the namespace (a request with an empty name), which finds no pool and releases the Job (D12). This works
at any time, not only during deletion.

A new `JobPool` cannot appear while the old one is being deleted: the namespace stays reserved for the old one until
step 3, and the webhook rejects any other pool until then (D11). The steps are idempotent: if the finalizer is
interrupted after step 3, the retry sees that the reservation is no longer its own and only removes the finalizer. The
queue is already released, and a new pool may have taken the namespace since: its queue must not be touched.

### D11. One `JobPool` per namespace: validating webhook + reservation on the Namespace

**History.** This is the third version, and the previous two are worth knowing:

1. **Validating webhook with `List`:** pools in the namespace → reject if one exists. The check and the write are not
   atomic: two concurrent CREATEs both see an empty namespace, and both are persisted.
2. **Fixed name `default`** (a CEL rule in the CRD, `self.metadata.name == 'default'`): the API server rejects a
   second `default` atomically (`AlreadyExists`). Atomic, but the rule about *how many* pools is expressed through the
   *name*. A meaningless name; a confusing error (`AlreadyExists` instead of "one pool per namespace"); and, most
   importantly, a dead end for the API: several pools per namespace with Job selectors (`team=ml` → 2, `team=batch` →
   10) cannot be added without a breaking change of the CRD, because every existing pool is called `default`.
3. **Now:** the name is free, the rule lives in a validating webhook again, and atomicity comes from a reservation.

**The reservation.** The webhook on CREATE of a `JobPool` writes two annotations to the Namespace:

```yaml
jobpool.kiribu.dev/pool: <JobPool name>
jobpool.kiribu.dev/pool-reserved-at: <RFC 3339 time>
```

The write is a patch with an optimistic lock (`resourceVersion`). Two concurrent CREATEs read the same Namespace
version; the API server accepts only the first patch and answers the second with `Conflict`. The second webhook
re-reads the Namespace, sees the reservation of the first pool and rejects its own. Every admitted CREATE writes to the
Namespace (a new reservation or a renewed one, see below), so of two concurrent CREATEs only one can win: the
compare-and-swap is done by etcd, as with the fixed name, but the key is now the Namespace instead of the pool name. The Namespace is the natural lock:
there is exactly one per namespace, and the operator already owns a label on it (D14). An annotation rather than a
label: nobody filters by it (cf. D6), and a `JobPool` name (up to 253 characters) does not always fit into a label value.

The webhook's rules (`internal/reservation`, a pure function with unit tests):

| Reservation on the Namespace | Decision |
|---|---|
| none | reserve for this pool, admit |
| this pool's name | renew the time, admit (a retried CREATE) |
| another pool, which exists (even being deleted) | reject: `JobPool "a" already exists in namespace "x"` |
| another pool, which does not exist, younger than 1 min | reject: the namespace "is being reserved", retry later |
| another pool, which does not exist, older than 1 min | take over: reserve for this pool, admit |

**Why the TTL.** The webhook reserves the namespace *before* the pool is stored. After the webhook the request may
still fail (another validating webhook, etcd), and the reservation is left dead. "The owner pool does not exist" is not
enough to take it over: the owner's CREATE may still be in flight, a few milliseconds after the webhook. Both pools
would then pass. A request ends within a minute (validating webhooks run in parallel with timeouts ≤ 30 s, the API
server's `--request-timeout` is 60 s by default), so a reservation older than a minute without a pool is surely dead. The
cost: after a failed CREATE another pool can be created in this namespace only a minute later (the same pool at once).
The TTL has to stay above `--request-timeout`: a cluster with a larger value needs a larger TTL.

**Why a retried CREATE renews the reservation.** Without the renewal the own-reservation case would write nothing, and
the optimistic lock would not order it against another pool. Scenario: the CREATE of `a` failed after the webhook; a
minute later `a` is created again and, at the same moment, `b`. The webhook of `a` sees its own reservation and admits
`a` without a write; the webhook of `b` sees a dead reservation (no pool `a` yet, older than a minute) and takes it over
with the unchanged `resourceVersion`. Both pools are stored. With the renewal the two webhooks race for the same
`resourceVersion` again: if `a` renews first, `b` sees a fresh reservation and is rejected; if `b` takes over first, the
renewal of `a` gets `Conflict`, and `a` sees a fresh reservation of `b` and is rejected.

**Side effects.** The webhook writes to the Namespace, so it is registered with `sideEffects: NoneOnDryRun` and does not
write anything on `--dry-run`: it only says whether the pool would be admitted.

**Release.** The finalizer of the pool removes the reservation after releasing the queue (D10), only if the
reservation is its own, with the same optimistic lock.

**The controller as a second line of defence.** The webhook can be bypassed: webhooks disabled (`ENABLE_WEBHOOKS=false`),
pools created before the webhook was installed, a reservation removed by hand. So the controller does not rely on it:

- a pool whose namespace is free (or dead-reserved) gets the reservation from the controller itself; this also
  restores a reservation removed by hand, since the Namespace is watched;
- a pool whose namespace is reserved for another pool gets `Ready=False, reason=Conflict` and does nothing: the queue
  is managed only by the owner. It checks the reservation again on every Namespace change and at least once a minute:
  an owner removed past its finalizer (by hand) leaves the Namespace unchanged, and only the periodic check notices
  that its reservation is dead;
- when the owner is deleted while such a pool exists, the finalizer does not release the queue but hands it over: it
  only releases the reservation, and the waiting pool takes the namespace with its own limit.

**Extensibility.** Several pools per namespace with Job selectors would change one rule: "the namespace is taken"
becomes "the new pool's selector overlaps an existing one". The API stays backwards compatible: a pool without a
selector covers all Jobs, as now.

### D12. Reconcile is triggered by `Watches()` + map functions, not `Owns()`

`Owns()` works through `ownerReferences`, and Jobs are created by others. Making the `JobPool` the owner of Jobs is not
an option: when the pool is deleted, the garbage collector would delete all the Jobs. Every event maps to the pools of
its namespace from the cache (normally one), or, when there is no pool but our traces have to be cleaned up, to a
request for the namespace with an empty name:

- `For(JobPool)`: creation, limit change, deletion;
- `Watches(Job)`: new Job, completion, deletion, suspend change. The event is enqueued if the namespace has a pool
  (per the cache) **or** the Job has the `queued` label: such a Job must not stay suspended without a pool (D10);
- `Watches(Namespace)`: someone changed the `enabled` label or the reservation. The event is enqueued if the namespace
  has a pool or carries our label: without a pool the label has to be removed. A released reservation lets a pool in
  `Conflict` take the namespace over (D11).

The "is there a pool" check in the map function keeps Jobs across the whole cluster from triggering reconciles with
requests to the API server.

If there is no pool, reconcile cleans up: it removes the label from the namespace and releases Jobs with the `queued`
label. Before that it confirms the pool is absent bypassing the cache: a pool created a split second ago may not be in
the cache yet.

The webhook does not trigger reconcile directly: after it, the request may still be rejected by someone else. We react
to what is actually persisted. Reconcile is level-triggered: everything is recomputed from scratch, so merged or
repeated events do no harm.

### D13. The Job list is read bypassing the cache

The controller-runtime cache lags behind the controller's own writes, and the Job and `JobPool` event streams are
independent. Scenario: reconcile #1 starts `A` and updates the status; the status update triggers reconcile #2, but the
event for `A` has not arrived yet. In the cache `A` is still suspended, and if a new `N` sits ahead of it in the queue,
one Job too many starts. The limit is broken.

Options: read Jobs via `mgr.GetAPIReader()` (+1 request per reconcile), or expectations as in the built-in controllers
(in-memory state, more complex). The first was chosen: simple and obviously correct, and there are not many Jobs in a
single namespace. Jobs are patched with an optimistic lock (`resourceVersion`): if the Job changed, reconcile retries
on fresh data.

The cache remains where staleness is safe or covered: map functions and the Job webhook (if the pool is missing or
being deleted, the webhook goes to the API server, D5).

### D14. `failurePolicy: Fail` + `namespaceSelector` + `matchConditions`

| | `Ignore` | `Fail` |
|---|---|---|
| Operator unavailable | The Job is created unsuspended and starts right away | Job creation is rejected |
| Cost | The limit is broken and cannot be fixed: suspending kills pods | The client retries (controllers do it themselves) |

`Fail` was chosen: guaranteeing the limit is the whole point. But a webhook on Jobs across the whole cluster with
`Fail` would stop Job creation everywhere, including `kube-system`, when the operator is down. Hence
`namespaceSelector`: the webhook is called only for namespaces labelled `jobpool.kiribu.dev/enabled=true`. The
controller sets the label when a `JobPool` appears in the namespace and removes it in the finalizer. The label is owned
by the controller: manual changes are corrected immediately (D12). The `JobPool` check inside the webhook (D5) remains
the second line of defence.

The `JobPool` webhook has `failurePolicy: Fail` and no `namespaceSelector`: while the operator is down, no `JobPool`
can be created anywhere. This is deliberate: without the webhook nobody reserves the namespace, and creating a pool is
a rare admin operation that can wait. Jobs, the frequent operation, are affected only in namespaces with a pool.

The webhook is also registered for UPDATE (D17), but `matchConditions` (CEL in the webhook configuration) lets only
resumes through to it. All other Job changes (labels, finalizers from the garbage collector) do not depend on the
operator and keep working when it is unavailable.

The limit is enabled when the API server sees the label on the namespace. The controller sets the label and writes
`Ready=True` right after, which is why the instructions run `kubectl wait --for=condition=Ready` after creating a pool.
This is not a strict guarantee: the API server evaluates `namespaceSelector` against its own Namespace cache
(informer), which updates asynchronously, usually immediately, but with no guaranteed bound (especially with several
API servers). Jobs admitted before that count as Jobs started before the pool (D9). Strict activation would need a
different protocol; an extra `sleep` does not provide it. Without `namespaceSelector` there would be no window, but
then `Fail` would hit the whole cluster.

### D15. Webhook high availability

- 2 manager replicas: the leader reconciles, both serve the webhook;
- readiness = the webhook server is running (`StartedChecker`), traffic goes only to ready pods;
- `LeaderElectionReleaseOnCancel`: on shutdown the leader releases the lease immediately;
- PodDisruptionBudget `minAvailable: 1`: a drain will not take down both replicas;
- `preStop: sleep 5s`: a pod being deleted drops out of the Service endpoints immediately, but kube-proxy updates its
  rules with a delay, and requests keep arriving for a while. The pause before SIGTERM lets the pod answer them;
  otherwise Jobs would be rejected by timeout during a rolling update or drain (the leader failover e2e caught this in
  CI). The `sleep` action is built into Kubernetes (GA since 1.34), no shell is needed in the image;
- soft anti-affinity: replicas land on different nodes when possible;
- `timeoutSeconds: 5`, so requests are not held up.

### D16. Observability

`status`: `activeJobs`, `queuedJobs`, `observedGeneration`, a `Ready` condition with reason `Active` / `Terminating` /
`Conflict`. `kubectl get jp` columns: Max, Active, Queued, Ready, Reason. A `Started` / `Released` event for every Job
started, a `Conflict` warning for a pool whose namespace belongs to another one. The owner of a namespace:
`kubectl get ns x -o jsonpath='{.metadata.annotations.jobpool\.kiribu\.dev/pool}'`.

### D17. Resuming past the queue is not possible

A CREATE-only webhook left a bypass: `kubectl patch job x -p '{"spec":{"suspend":false}}'`. A queued Job, or a Job the
user created suspended, would start over the limit, and it cannot be stopped afterwards (D2).

So the webhook also handles UPDATE: if **someone other than the operator** resumes a Job in a namespace with a pool,
the Job goes back to suspended and gets the `queued` label. For the user, "resume" means "join the queue": the Job
runs when a slot frees up. Same principle as D1: a queue, not a rejection. A suspend the user set themselves is still
left alone until they lift it.

The webhook recognises the operator by the username in the admission request. The operator learns its own name at
startup via `SelfSubjectReview` (same as `kubectl auth whoami`): in-cluster this is its ServiceAccount. Nothing needs
configuring, but the operator must run under **its own** identity. When run locally (`make run`) it uses the user from
the kubeconfig, and if the same user runs `kubectl`, the webhook will take their resume for the operator's. That is
acceptable for local development; in-cluster the operator has a dedicated ServiceAccount.

## Known limitations

- **Leader election provides no fencing.** client-go [says so explicitly](https://pkg.go.dev/k8s.io/client-go/tools/leaderelection#hdr-Overview):
  two leaders may act at the same time. Scenario: the old leader built the plan "start `A` and `B`" and stalled longer
  than the lease duration (15 s); the new leader started other Jobs; the old one woke up and started `A` and `B`. The
  optimistic lock does not help: it checks the version of each Job, not of the whole namespace. Result: the limit is
  exceeded by the size of the stale plan (up to `maxConcurrentJobs` extra Jobs) until the extra ones finish. It
  requires the process to pause for more than 15 seconds exactly between planning and starting; a normal leader
  change (`ReleaseOnCancel`, pod deletion) does not do that. A strict guarantee would need atomic slot reservation in
  the API server. This is a general limitation of controller-runtime operators, including the built-in controllers.
- **The limit counts Jobs, not pods.** One Job with a large `parallelism` uses a lot of resources. The task is to limit
  the number of Jobs; resources are limited by `ResourceQuota` and `LimitRange`, which are compatible with `JobPool`.
- **FIFO with one-second precision.** `creationTimestamp` has one-second precision; within a second, order is by name.
  The order is stable, but for Jobs with `generateName` within one second it does not match creation order.
- **An unavailable operator blocks deletion of the `JobPool`** (finalizer) and of a namespace with a pool until the
  operator is back. This is the usual cost of a finalizer. It also blocks the creation of new pools (D14).
- **The reservation is an ordinary annotation.** Anyone allowed to `patch` the Namespace can change it. A removed
  reservation is restored by the controller within moments, but in that window the webhook admits a new pool; if the
  new pool reserves the namespace first, the old one reports `Conflict` and the new pool's limit applies. Writing
  another pool's name into it has the same effect (D11). The limit itself holds: only one pool manages the queue.
  Namespace `patch` is an admin permission, the same one needed to remove the `enabled` label.
- **A dead reservation blocks other pools for up to a minute** after a failed CREATE (D11).

## Rejected alternatives

| Alternative | Why not |
|---|---|
| Reject Jobs over the limit | Every client has to retry (D1) |
| Suspend in reconcile | Race with the Job controller: pods may be created first (D3) |
| Webhook with a counter | Concurrent requests exceed the limit (D4) |
| ResourceQuota `count/jobs.batch` | Counts all Jobs, including finished ones, and rejects rather than queues |
| CEL for "one pool per namespace" by content | CEL sees only the object itself, not its neighbours (D11) |
| Validating webhook that only lists pools | Check and write are not atomic: concurrent CREATEs both pass (D11) |
| Fixed name `default` (CEL rule on the name) | Atomic, but the name means nothing and selectors per pool become a breaking change (D11) |
| Separate lock object (e.g. a `Lease`) per namespace | One more object to create and clean up; the Namespace is already there and already ours (D11) |
| Mutex in the webhook process | Both replicas serve the webhook (D15); a lock in memory does not span them |
| Take over a reservation as soon as its pool is missing | The owner's CREATE may still be in flight; both pools would pass (D11) |
| `Owns()` / `JobPool` owns Jobs | The Jobs are not ours; GC would delete them with the pool (D12) |
| Count active Jobs by `status.active` | Lags right after start (D7) |
| Read Jobs from the cache | A stale cache breaks the limit (D13) |
| Re-check the queue in the finalizer after 1 s | Misses requests persisted later (D10) |
| Reject a user's resume | The user could not start their Job while a pool exists; a queue is fairer (D17) |
| `failurePolicy: Ignore` | Silently breaks the limit when the operator is unavailable (D14) |
| `Fail` without `namespaceSelector` | An operator outage blocks Jobs across the whole cluster (D14) |

## Tests

- `internal/jobpool`: table-driven unit tests of the pure logic (start plan, FIFO, resume detection), 100% coverage.
- `internal/reservation`: table-driven unit tests of the reservation rules (D11); the API calls are covered by the
  envtest suites below.
- `internal/webhook/v1`: envtest, requests go through a real API server and admission chain. Covers CREATE, resume by
  a user and by the operator (the operator via impersonation), a cache miss and a terminating pool in the cache.
- `internal/webhook/v1alpha1`: envtest for the `JobPool` webhook: a second pool, a pool being deleted, ten concurrent
  CREATEs (exactly one passes; without the optimistic lock the test fails), a retried CREATE that renews an old
  reservation, a retried CREATE racing another pool (without the renewal both pass), a fresh and a dead reservation,
  dry run.
- `internal/controller`: envtest with a running manager and no webhooks, which is the bypass case of D11. There is no
  Job controller in envtest, so Job completion is simulated via `status`. Regressions: the reservation is kept until
  the queue is released, a second pool gets `Conflict` and later takes the queue over, deleting a pool in `Conflict`
  leaves the owner's queue alone, a dead and a fresh reservation, a removed reservation, a queued Job without a pool, a
  manual change of the `enabled` label.
- `test/e2e`: kind with real Jobs, pods, cert-manager, 2 replicas, leader failover. The scenarios are described in
  [e2e-guide.md](e2e-guide.md).
