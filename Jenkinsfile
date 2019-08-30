pipeline {
  agent {
    label 'centos7-alljava-maven-docker'
  }

  options {
    parallelsAlwaysFailFast()
    skipStagesAfterUnstable()
  }

  environment {
    GIT = credentials('eg-oss-ci')
    GIT_USERNAME = "${env.GIT_USR}"
    GIT_PASSWORD = "${env.GIT_PSW}"

    MAVEN_SETTINGS = credentials('eg-oss-settings.xml')

    OSS_GPG_PUB_KEYRING = credentials('pubring.gpg')
    OSS_GPG_SEC_KEYRING = credentials('secring.gpg')
    OSS_GPG_PASSPHRASE = credentials('private-key-passphrase')
  }

  stages {
    stage('Build and deploy') {
      steps {
        echo 'Checking out project...'
        checkout scm
        echo 'Building...'
        echo 'Maven Settings'
        sh """${MAVEN_SETTINGS}"""
        sh 'mvn clean deploy jacoco:report checkstyle:checkstyle spotbugs:spotbugs -Darguments="-s MAVEN_SETTINGS"'
        jacoco()
        recordIssues(
            enabledForFailure: true, aggregatingResults: true,
            tools: [checkStyle(reportEncoding: 'UTF-8'), spotbugs()]
        )
        echo 'Pushing images...'
        script {
          DOCKER_REGISTRY = sh(script: 'mvn help:evaluate -Dexpression=docker.registry -q -DforceStdout', returnStdout: true).trim()
        }
        echo 'Docker registry ${DOCKER_REGISTRY}'
        sh 'docker images'
        withCredentials([usernamePassword(credentialsId: 'dockerhub-egopensource', passwordVariable: 'PASSWORD', usernameVariable: 'USERNAME')]) {
          sh 'docker login -u $USERNAME -p $PASSWORD'
        }
        // docker push ${DOCKER_REGISTRY}/beekeeper-cleanup
        // docker push ${DOCKER_REGISTRY}/beekeeper-path-scheduler-apiary
      }
    }

    stage('Release') {
      options {
       timeout(time: 2, unit: 'HOURS')
      }

      input {
        message "Perform release?"
        ok 'Perform release'
        parameters {
          string(description: 'Release version', name: 'RELEASE_VERSION')
          string(description: 'Development version', name: 'DEVELOPMENT_VERSION')
        }
      }

      steps {
        script {
          if (env.RELEASE_VERSION == null || env.DEVELOPMENT_VERSION == null) {
            currentBuild.result = 'ABORTED'
            error 'Invalid version'
          }
        }
        echo 'Performing release...'
        sh """mvn --batch-mode release:prepare release:perform \
                -Dresume=false \
                -DreleaseVersion=${RELEASE_VERSION} \
                -DdevelopmentVersion=${DEVELOPMENT_VERSION} \
                -DautoVersionSubmodules=true \
                 -Darguments=\"-s MAVEN_SETTINGS\""""
        echo 'Pushing images...'
        script {
          DOCKER_REGISTRY = sh(script: 'mvn help:evaluate -Dexpression=docker.registry -q -DforceStdout', returnStdout: true).trim()
        }
        sh 'docker tag ${DOCKER_REGISTRY}/beekeeper-cleanup:${RELEASE_VERSION} ${DOCKER_REGISTRY}/beekeeper-cleanup:latest'
        sh 'docker tag ${DOCKER_REGISTRY}/beekeeper-path-scheduler-apiary:${RELEASE_VERSION} ${DOCKER_REGISTRY}/beekeeper-path-scheduler-apiary:latest'
        sh 'docker push ${DOCKER_REGISTRY}/beekeeper-cleanup'
        sh 'docker push ${DOCKER_REGISTRY}/beekeeper-path-scheduler-apiary'
      }
    }
  }
}
