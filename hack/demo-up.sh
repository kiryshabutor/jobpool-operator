#!/usr/bin/env bash
# Recreates the kind cluster "jobpool" and installs cert-manager and the operator built from the current code.
# Afterwards follow docs/e2e-guide.md.
#
#   hack/demo-up.sh                  # recreate the cluster from scratch
#   KEEP_CLUSTER=1 hack/demo-up.sh   # keep the cluster, only rebuild and redeploy the operator
set -euo pipefail

CLUSTER=jobpool
CTX="kind-$CLUSTER"
IMG=jobpool-operator:dev
NODE_IMAGE=kindest/node:v1.37.0
CERT_MANAGER=https://github.com/cert-manager/cert-manager/releases/download/v1.21.1/cert-manager.yaml

cd "$(dirname "$0")/.."

step() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

step "Checking inotify limits"
if (( $(sysctl -n fs.inotify.max_user_instances) < 512 )); then
  echo "inotify limits are too low: kube-proxy will crash with 'too many open files'. Run:" >&2
  echo "  sudo sysctl -w fs.inotify.max_user_instances=512 fs.inotify.max_user_watches=524288" >&2
  exit 1
fi

if [[ -z "${KEEP_CLUSTER:-}" ]] || ! kind get clusters | grep -qx "$CLUSTER"; then
  step "Cluster $CLUSTER"
  kind delete cluster --name "$CLUSTER"
  kind create cluster --name "$CLUSTER" --image "$NODE_IMAGE"

  step "cert-manager"
  kubectl --context="$CTX" apply -f "$CERT_MANAGER" >/dev/null
  kubectl --context="$CTX" -n cert-manager wait --for=condition=Available deploy --all --timeout=180s
fi

step "Building $IMG and loading it into kind"
make docker-build IMG="$IMG"
kind load docker-image "$IMG" --name "$CLUSTER"

step "Deploying the operator"
kubectl config use-context "$CTX" >/dev/null   # make deploy uses the current context
make deploy IMG="$IMG"
git checkout -- config/manager/kustomization.yaml   # make deploy writes the image tag there
# the tag stays the same between rebuilds, so restart to pick up the new image
kubectl --context="$CTX" -n jobpool-operator-system rollout restart deploy/jobpool-operator-controller-manager >/dev/null
kubectl --context="$CTX" -n jobpool-operator-system rollout status deploy/jobpool-operator-controller-manager --timeout=180s

step "Done"
kubectl --context="$CTX" -n jobpool-operator-system get pods,pdb
cat <<'EOF'

Current context: kind-jobpool. Next: docs/e2e-guide.md. Columns that show the queue:

  COLS='NAME:.metadata.name,SUSPEND:.spec.suspend,QUEUED:.metadata.labels.jobpool\.kiribu\.dev/queued,ACTIVE:.status.active,DONE:.status.succeeded'
EOF
