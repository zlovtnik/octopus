#!/usr/bin/env bash
set -euo pipefail
source scripts/ci/common.sh
ci_install_cleanup
coverage_container="$(ci_container_name octopus)"
tar -cf - . | ci_run octopus -i -w /workspace \
  -v /var/run/docker.sock:/var/run/docker.sock \
  azul/zulu-openjdk:21 \
  sh -c 'tar --no-same-owner -xf - && sh /workspace/scripts/ci/tasks/test-1.sh'
mkdir -p artifacts/octopus-coverage
docker_cmd cp "$coverage_container:/workspace/target/scala-3.3.8/jacoco/report" artifacts/octopus-coverage/jacoco
docker_cmd cp "$coverage_container:/workspace/target/cucumber" artifacts/octopus-coverage/cucumber
docker_cmd cp "$coverage_container:/workspace/octopus-ci.jar" artifacts/octopus.jar
