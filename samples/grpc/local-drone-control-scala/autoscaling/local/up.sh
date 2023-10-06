#!/usr/bin/env bash

set -euo pipefail

# logs and failures

function red {
  echo -en "\033[0;31m$@\033[0m"
}

function green {
  echo -en "\033[0;32m$@\033[0m"
}

function blue {
  echo -en "\033[0;34m$@\033[0m"
}

function info {
  echo
  echo $(blue "$@")
  echo
}

function success {
  echo
  echo $(green "$@")
  echo
}

function error {
  echo $(red "$@") 1>&2
}

function fail {
  error "$@"
  exit 1
}

# requirements

function command_exists {
  type -P "$1" > /dev/null 2>&1
}

command_exists "docker" || fail "docker is required (https://www.docker.com)"
command_exists "kubectl" || fail "kubectl is required (https://kubernetes.io/docs/reference/kubectl)"
command_exists "k3d" || fail "k3d is required (https://k3d.io)"
command_exists "helm" || fail "helm is required (https://helm.sh)"

# options

declare local_image="local-drone-control:latest"
declare central_host="host.k3d.internal"
declare central_port="8101"
declare central_tls="false"

while [[ $# -gt 0 ]] ; do
  case "$1" in
    --local-image  ) local_image="$2"  ; shift 2 ;;
    --central-host ) central_host="$2" ; shift 2 ;;
    --central-port ) central_port="$2" ; shift 2 ;;
    --central-tls  ) central_tls="$2"  ; shift 2 ;;
    * ) error "unknown option: $1" ; shift ;;
  esac
done

# image exists check

[ -n "$(docker images -q "$local_image")" ] || fail "Docker image [$local_image] not found. Build locally before running."

# directories

readonly local=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
readonly autoscaling="$(cd "$local/.." && pwd)"

# deploy to local k3s cluster

info "Creating k3s cluster ..."

# with port mapping for traefik ingress
k3d cluster create edge --port 8080:80@loadbalancer

info "Installing postgresql persistence ..."

helm dependency update "$local/persistence"
helm install local "$local/persistence" --create-namespace --namespace persistence --wait

info "Installing prometheus monitoring stack ..."

helm dependency update "$local/monitoring"
helm install local "$local/monitoring" --create-namespace --namespace monitoring --wait

info "Installing vertical pod autoscaler ..."

helm dependency update "$local/autoscaler"
helm install local "$local/autoscaler" --create-namespace --namespace kube-system --wait

info "Deploying local-drone-control service ..."

k3d image import --cluster edge "$local_image"

kubectl create secret generic central-drone-control \
  --from-literal=host=$central_host \
  --from-literal=port=$central_port \
  --from-literal=tls=$central_tls

kubectl create secret generic database-credentials \
  --from-literal=host=local-postgresql.persistence.svc \
  --from-literal=port=5432 \
  --from-literal=database=postgres \
  --from-literal=user=postgres \
  --from-literal=password=postgres

kubectl apply -f "$autoscaling/kubernetes"
kubectl wait pods -l app=local-drone-control --for condition=Ready --timeout=120s
kubectl get pods

info "Setting up ingress ..."

kubectl apply -f "$local/ingress"

success "Local Drone Control service running in k3s and available at localhost:8080"
