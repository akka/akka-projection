#!/usr/bin/env bash

set -euo pipefail

# logs and failures

function red {
  echo -en "\033[0;31m$@\033[0m"
}

function blue {
  echo -en "\033[0;34m$@\033[0m"
}

function info {
  echo
  echo $(blue "$@")
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

command_exists "k3d" || fail "k3d is required (https://k3d.io)"

# destroy k3s cluster

info "Deleting k3s cluster ..."

k3d cluster delete edge
