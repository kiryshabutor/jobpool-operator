# jobpool-operator

Kubernetes-оператор, который ограничивает число **одновременно работающих Job** в namespace.

Админ создаёт в namespace ресурс `JobPool` с полем `maxConcurrentJobs`. После этого каждая новая Job в этом namespace
создаётся **на паузе** (`spec.suspend: true`) и встаёт в очередь. Контроллер снимает паузу с Job в порядке создания
(FIFO), когда работающих меньше лимита. Клиенты создают Job как обычно и о лимите ничего не знают: Job не отклоняется,
а выполнится, как только освободится место. Namespace без `JobPool` оператор не трогает.

```
kubectl create job ──► mutating webhook ──► etcd ──► controller ──► suspend: false
                       (suspend: true +              (FIFO, не больше
                        label queued)                 maxConcurrentJobs)
```

- **Mutating webhook** на создание Job: если в namespace есть `JobPool`, ставит `suspend: true` и label
  `jobpool.kiribu.dev/queued=true`. Ничего не считает, решение о запуске принимает только контроллер.
- **Validating webhook** на создание `JobPool`: в одном namespace может быть только один `JobPool`.
- **Контроллер** считает работающие Job, запускает следующие из очереди и пишет счётчики в `status`.
  При удалении `JobPool` finalizer запускает всю очередь.

Почему устроено именно так — в [docs/design.md](docs/design.md).

## Quickstart

### Требования

- Docker
- [kind](https://kind.sigs.k8s.io/)
- kubectl
- Go 1.26+ (только для `make`-целей сборки и тестов)

### Установка в kind

```sh
# 1. Кластер
kind create cluster --name jobpool

# 2. cert-manager: выпускает TLS-сертификат для webhook-ов
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.21.1/cert-manager.yaml
kubectl -n cert-manager wait --for=condition=Available deploy --all --timeout=180s

# 3. Образ оператора: собрать и загрузить в kind
make docker-build IMG=jobpool-operator:dev
kind load docker-image jobpool-operator:dev --name jobpool

# 4. CRD, RBAC, webhook-и и 2 реплики менеджера
make deploy IMG=jobpool-operator:dev
kubectl -n jobpool-operator-system rollout status deploy/jobpool-operator-controller-manager
```

Вместо шагов 3–4 можно поставить один собранный манифест: `make build-installer IMG=...` создаёт `dist/install.yaml`.

### Использование

```yaml
# config/samples/scheduling_v1alpha1_jobpool.yaml
apiVersion: scheduling.kiribu.dev/v1alpha1
kind: JobPool
metadata:
  name: jobpool-sample
spec:
  maxConcurrentJobs: 2
```

```sh
kubectl create ns demo
kubectl -n demo apply -f config/samples/scheduling_v1alpha1_jobpool.yaml

# 5 Job, каждая спит 20 секунд
for i in 1 2 3 4 5; do kubectl -n demo create -f config/samples/job_sleep.yaml; done

kubectl -n demo get jobpool        # короткое имя: jp
# NAME             MAX   ACTIVE   QUEUED   READY   REASON   AGE
# jobpool-sample   2     2        3        True    Active   10s

kubectl -n demo get jobs -w        # Job запускаются по 2
kubectl -n demo events --for jobpool/jobpool-sample
```

Пошаговый сценарий со всеми случаями и ожидаемым выводом — в [docs/e2e-guide.md](docs/e2e-guide.md).

### Удаление

```sh
kubectl delete jobpools --all -A   # сначала пулы: finalizer запустит их очереди
make undeploy
kind delete cluster --name jobpool
```

Пулы нужно удалить **до** `make undeploy`, пока оператор работает. Иначе CRD не удалится: удаление будет ждать,
пока с `JobPool` снимут finalizer, а снимать его будет некому.

## Поведение и ограничения

- **Очередь вместо отказа.** Job сверх лимита создаётся и ждёт на паузе, её не отклоняют.
- **Работающие Job не останавливаются.** Если работающих Job уже больше лимита (например, их запустили до создания
  `JobPool`), оператор их не трогает: пауза убила бы их поды. Он просто не запускает новые, пока работающих не станет
  меньше лимита.
- **Считаются все Job namespace,** в том числе созданные до `JobPool`. Завершённые (`Complete`/`Failed`) и стоящие на
  паузе место не занимают.
- **Чужие паузы не трогаются.** Если пользователь сам создал Job с `suspend: true`, она не встаёт в очередь, и оператор
  её не запускает, даже при удалении `JobPool`.
- **Job от CronJob и других контроллеров тоже встают в очередь:** webhook не различает, кто создал Job.
- **Один `JobPool` на namespace.** Второй отклоняет webhook. Если два пула всё же проскочили одновременно, главным
  считается самый старый, второй получает `Ready=False, reason=Duplicate`.
- **Удаление `JobPool` запускает всю очередь,** а label `jobpool.kiribu.dev/enabled` снимается с namespace.
- **Если оператор недоступен, Job в подключённых namespace не создаются** (`failurePolicy: Fail`). Иначе Job прошла бы
  без паузы и сразу стартовала, а поставить её на паузу потом нельзя без убийства подов: лимит был бы нарушен. Чтобы
  это не задело остальной кластер, webhook вызывается только для namespace с label `jobpool.kiribu.dev/enabled=true`.
  Этот label контроллер ставит сам, когда в namespace появляется `JobPool`. Риск недоступности снижают 2 реплики
  менеджера (webhook обслуживают обе), PodDisruptionBudget и `timeoutSeconds: 5`.

## Разработка

```sh
make test       # unit + envtest (контроллер и webhook-и)
make lint       # golangci-lint
make test-e2e   # kind-кластер jobpool-operator-test-e2e, деплой, 12 тестов, удаление кластера (~6 мин)
```

`make deploy` и `make test-e2e` записывают тег образа в `config/manager/kustomization.yaml`; это изменение не коммитим.

## Структура

| Путь | Что там |
|---|---|
| `api/v1alpha1/` | Тип `JobPool`, имена label, finalizer и condition |
| `internal/jobpool/` | Чистая логика без Kubernetes-клиента: что активно, FIFO, план запуска, главный пул |
| `internal/webhook/v1/` | Mutating webhook для Job |
| `internal/webhook/v1alpha1/` | Validating webhook для `JobPool` |
| `internal/controller/` | Reconcile, finalizer, watch-и |
| `config/` | Kustomize-манифесты: CRD, RBAC, webhook-и, cert-manager, Deployment, PDB, samples |
| `test/e2e/` | E2E-тесты на kind |
| `docs/` | Дизайн и ручной e2e-гайд |

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
