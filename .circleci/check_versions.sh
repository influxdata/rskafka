#!/usr/bin/env bash
# Checks that container versions in CI and dev tooling like docker compose files are in-sync

set -euo pipefail

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
readonly SCRIPTPATH

readonly DEPS=(redpanda kafka zookeeper)

pushd "$SCRIPTPATH/.."

for dependency in "${DEPS[@]}"; do
    echo "========================================"
    echo "Checking $dependency"
    versions="$(find . \( -name target -prune \) -o \( -name .git -prune \) -o \( -name coverage -prune \) -o \( -name corpus -prune \) -o -type f -exec grep image: {} \; | grep "$dependency" | sed -e 's/^.*image:.*://' | sort -u)"
    echo "Found:"
    echo "$versions"
    n="$(echo "$versions" | wc -l)"
    if [[ $n != 1 ]]; then
        echo "Found $n different versions!"
        exit 1
    fi
done
