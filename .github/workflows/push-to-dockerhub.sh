#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

readonly version=$(cat VERSION)
readonly git_sha=$(git rev-parse HEAD)
readonly git_timestamp=$(TZ=UTC git show --quiet --date='format-local:%Y%m%d%H%M%S' --format="%cd")
readonly slug=${version}-${git_timestamp}-${git_sha:0:16}

docker tag bsideup/liiklus:latest bsideup/liiklus:${version}
docker tag bsideup/liiklus:latest bsideup/liiklus:${slug}

docker push bsideup/liiklus:${version}
docker push bsideup/liiklus:${slug}
