pipeline {
  agent any
  options {
    disableConcurrentBuilds(abortPrevious: true)
    skipDefaultCheckout(true)
    timestamps()
    timeout(time: 90, unit: 'MINUTES')
  }
  stages {
    stage('Checkout') {
      steps {
        deleteDir()
        checkout scm
      }
    }
    stage('Test and assemble') {
      steps {
        sh '''
          set -eu
          coverage_container="octopus-source-${BUILD_NUMBER}"
          cleanup() {
            docker rm --force "$coverage_container" >/dev/null 2>&1 || true
          }
          trap cleanup EXIT
          tar -cf - . | docker run --name "$coverage_container" -i -w /workspace \
            -v /var/run/docker.sock:/var/run/docker.sock \
            azul/zulu-openjdk:21 \
            sh -c 'tar --no-same-owner -xf - && apt-get -o Dir::Etc::sourceparts="-" update && apt-get install -y --no-install-recommends curl bash python3 && curl -fsSL https://github.com/sbt/sbt/releases/download/v1.12.14/sbt-1.12.14.tgz | tar xz -C /opt && ln -s /opt/sbt/bin/sbt /usr/local/bin/sbt && OCTOPUS_REQUIRE_DOCKER=true sbt -Dsbt.supershell=false scalafmtCheckAll "scalafixAll --check" jacoco assembly && python3 scripts/check_coverage.py target/scala-3.3.8/jacoco/report/jacoco.xml && cp target/scala-3.*/octopus.jar octopus-ci.jar'
          mkdir -p artifacts/octopus-coverage
          docker cp "$coverage_container:/workspace/target/scala-3.3.8/jacoco/report" artifacts/octopus-coverage/jacoco
          docker cp "$coverage_container:/workspace/target/cucumber" artifacts/octopus-coverage/cucumber
          docker cp "$coverage_container:/workspace/octopus-ci.jar" artifacts/octopus.jar
          cleanup
          trap - EXIT
        '''
        archiveArtifacts artifacts: 'artifacts/octopus-coverage/**,artifacts/octopus.jar', fingerprint: true
      }
    }
  }
}
