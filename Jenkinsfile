pipeline {
  agent any
  options {
    buildDiscarder(logRotator(numToKeepStr: '20', artifactNumToKeepStr: '10'))
    disableConcurrentBuilds(abortPrevious: true)
    skipDefaultCheckout(true)
    timestamps()
    timeout(time: 90, unit: 'MINUTES')
  }
  stages {
    stage('Checkout') {
      options { timeout(time: 10, unit: 'MINUTES') }
      steps {
        deleteDir()
        checkout scm
      }
    }
    stage('Test and assemble') {
      options { timeout(time: 75, unit: 'MINUTES') }
      steps {
        sh 'bash scripts/ci/test.sh'
        archiveArtifacts artifacts: 'artifacts/octopus-coverage/**,artifacts/octopus.jar', fingerprint: true
      }
    }
  }
  post {
    always {
      timeout(time: 5, unit: 'MINUTES') {
        sh label: 'Reclaim CI resources', script: 'if [ -f scripts/ci/cleanup.sh ]; then bash scripts/ci/cleanup.sh; fi'
      }
    }
  }
}
