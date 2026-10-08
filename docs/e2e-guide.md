# E2E-гайд

Ручная проверка оператора в kind-кластере, шаг за шагом, с ожидаемым выводом. Те же сценарии автоматически
проверяет `make test-e2e` (см. [в конце](#автоматические-e2e-тесты)).

Перед началом оператор должен быть установлен по [Quickstart](../README.md#quickstart). Все команды выполняются из
каталога `jobpool-operator` ветки `jobpool-operator`.

```sh
kubectl -n jobpool-operator-system get pods,pdb
# NAME                                                       READY   STATUS    RESTARTS   AGE
# pod/jobpool-operator-controller-manager-6f5c7d6fc6-2j9hh   1/1     Running   0          1m
# pod/jobpool-operator-controller-manager-6f5c7d6fc6-6jvsf   1/1     Running   0          1m
#
# NAME                                                             MIN AVAILABLE   MAX UNAVAILABLE   ALLOWED DISRUPTIONS
# poddisruptionbudget.policy/jobpool-operator-controller-manager   1               N/A               1
```

Для удобства — колонки, по которым видна очередь:

```sh
COLS='NAME:.metadata.name,SUSPEND:.spec.suspend,QUEUED:.metadata.labels.jobpool\.kiribu\.dev/queued,ACTIVE:.status.active,DONE:.status.succeeded'
```

## 1. Лимит и очередь

```sh
kubectl create ns demo
kubectl -n demo apply -f config/samples/scheduling_v1alpha1_jobpool.yaml   # name: default, maxConcurrentJobs: 2
kubectl -n demo wait --for=condition=Ready jobpool/default
```

`Ready=True` значит, что контроллер поставил label и подключил namespace к webhook-у. API server применяет label по
своему кэшу Namespace, обычно сразу:

```sh
kubectl get ns demo --show-labels
# NAME   STATUS   AGE   LABELS
# demo   Active   4s    jobpool.kiribu.dev/enabled=true,kubernetes.io/metadata.name=demo
```

Создаём 5 Job, каждая спит 20 секунд:

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

Работают 2, ещё 3 на паузе с label `queued`. Смотрим, как очередь сдвигается (`kubectl -n demo get jobs -w`):
когда Job завершается, следующая по времени создания стартует. Через ~70 секунд все 5 завершены:

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

Первые две Job тоже прошли через очередь: webhook ставит на паузу **все** новые Job, решение о запуске принимает
только контроллер.

## 2. Namespace без `JobPool` не затрагивается

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

Label `enabled` у namespace нет, поэтому API server webhook даже не вызывает. Все Job стартуют сразу.

## 3. Второй `JobPool` в namespace отклоняется

`JobPool` может называться только `default`, а два объекта с одним именем в namespace не допускает API server:

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

`kubectl apply` того же файла с `name: default` не создаёт второй пул, а обновляет существующий: это тот же объект.

## 4. Пауза пользователя не трогается

Job, которую пользователь сам создал на паузе, в очередь не попадает, и оператор её не запускает:

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

Место свободно (`ACTIVE 0`), но Job остаётся на паузе: это пауза пользователя.

## 5. Снять паузу в обход очереди нельзя

Пользователь снимает паузу со своей Job. Webhook ставит её в очередь, и контроллер запускает её, как только есть
место. Чтобы увидеть очередь, сначала займём оба места:

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

Запрос прошёл без ошибки, но Job не стартовала сверх лимита: она в очереди. Через ~20 секунд, когда одна из Job
завершится, `paused` запустится. Запускать Job из очереди может только сам оператор.

## 6. Отказ лидера

Реконсилит только одна реплика (лидер), webhook обслуживают обе. Убиваем лидера при непустой очереди:

```sh
for i in 1 2 3 4; do kubectl -n demo create -f config/samples/job_sleep.yaml; done

kubectl -n jobpool-operator-system get lease b4772dc6.kiribu.dev -o jsonpath='{.spec.holderIdentity}{"\n"}'
# jobpool-operator-controller-manager-6f5c7d6fc6-2j9hh_...
kubectl -n jobpool-operator-system delete pod jobpool-operator-controller-manager-6f5c7d6fc6-2j9hh

# в это время Job создаётся без ошибок: webhook отвечает вторая реплика
kubectl -n demo create -f config/samples/job_sleep.yaml
```

Через несколько секунд lease у другой реплики, а очередь продолжает двигаться: `kubectl -n demo get jp`.

## 7. Удаление `JobPool` запускает всю очередь

```sh
for i in 1 2 3; do kubectl -n demo create -f config/samples/job_sleep.yaml; done
kubectl -n demo get jobs -l jobpool.kiribu.dev/queued=true      # есть Job в очереди

kubectl -n demo delete jobpool default

kubectl -n demo get jobs -l jobpool.kiribu.dev/queued=true
# No resources found in demo namespace.

kubectl get ns demo --show-labels
# NAME   STATUS   AGE     LABELS
# demo   Active   2m33s   kubernetes.io/metadata.name=demo
```

Finalizer снял label с namespace и паузу со всех Job из очереди. Новые Job в `demo` теперь создаются без паузы.

## 8. Оператор недоступен: `failurePolicy: Fail`

```sh
kubectl -n demo apply -f config/samples/scheduling_v1alpha1_jobpool.yaml
kubectl -n demo wait --for=condition=Ready jobpool/default    # без этого webhook может быть ещё не подключён
kubectl -n jobpool-operator-system scale deploy jobpool-operator-controller-manager --replicas=0
kubectl -n jobpool-operator-system wait --for=delete pod -l control-plane=controller-manager --timeout=60s

# namespace с JobPool: создание отклоняется (примерно через 5 с, это timeoutSeconds webhook-а)
kubectl -n demo create -f config/samples/job_sleep.yaml
# Error from server (InternalError): error when creating "config/samples/job_sleep.yaml": Internal error occurred:
# failed calling webhook "mjob.kiribu.dev": failed to call webhook: Post "https://jobpool-operator-webhook-service.
# jobpool-operator-system.svc:443/mutate-batch-v1-job?timeout=5s": context deadline exceeded

# namespace без JobPool: работает как обычно
kubectl -n plain create -f config/samples/job_sleep.yaml
# job.batch/sleep-nbwbq created
```

Без оператора Job нельзя ни поставить в очередь, ни безопасно пропустить: она стартовала бы сверх лимита. Поэтому
создание отклоняется, но только в namespace, где админ включил лимит. Возвращаем оператор:

```sh
kubectl -n jobpool-operator-system scale deploy jobpool-operator-controller-manager --replicas=2
kubectl -n jobpool-operator-system rollout status deploy/jobpool-operator-controller-manager
```

## Очистка

```sh
kubectl delete ns demo plain
```

## Автоматические e2e-тесты

```sh
make test-e2e
```

Что делает:

1. Создаёт отдельный kind-кластер `jobpool-operator-test-e2e` (имя меняется через `KIND_CLUSTER=...`), если его ещё
   нет. Доступ к нему пишется в отдельный kubeconfig `bin/<кластер>.kubeconfig`, и все команды теста работают только
   через него: текущий контекст в `~/.kube/config` не меняется и не используется.
2. Собирает образ, загружает его в kind, ставит cert-manager и деплоит оператор.
3. Запускает тесты из `test/e2e/` (~8 минут, Job реально выполняются):
   - менеджер запущен (2 пода), метрики отдаются, cert-manager выпустил сертификат и подставил CA в webhook;
   - лимит: пул `max=2`, 5 Job создаются одновременно — пик работающих ровно 2 (считается по `startTime` и
     `completionTime` Job, а не опросом), все 5 завершаются;
   - FIFO: Job запускаются (`status.startTime`) в порядке создания;
   - namespace без `JobPool` — Job стартуют сразу, без label;
   - второй `default` отклоняется (`AlreadyExists`), пул с другим именем — CEL-правилом;
   - пауза пользователя держится 20 секунд, label не появляется;
   - снятие паузы пользователем (с Job из очереди и со своей) — Job возвращаются в очередь, работает по-прежнему 1;
   - удаление `JobPool` — очередь пуста, все Job запущены, label `enabled` снят;
   - смена лидера при непустой очереди — новый лидер продолжает очередь, пик по-прежнему 1, Job создаются, пока
     работает одна реплика;
   - менеджер в `replicas=0` — Job в namespace с пулом отклоняется, снять паузу тоже нельзя, а `kubectl label job`
     проходит (`matchConditions`); в namespace без пула Job создаётся.
4. Удаляет кластер и его kubeconfig.

После неудачного прогона кластер остаётся для разбора; удаляет его `make cleanup-test-e2e`.

Если kind не может поднять кластер при уже работающих других (`kubeadm init` падает с `connection refused`),
поднимите лимиты inotify:

```sh
sudo sysctl -w fs.inotify.max_user_instances=512 fs.inotify.max_user_watches=524288
```
