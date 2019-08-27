pipeline {
  agent {
    label 'centos7-alljava-maven-docker'
  }

  options {
    parallelsAlwaysFailFast()
    skipStagesAfterUnstable()
  }

  environment {
    GIT = credentials('s-hcom-ci')
    GIT_USERNAME = "${env.GIT_USR}"
    GIT_PASSWORD = "${env.GIT_PSW}"

    DOCKER_ORG = readMavenPom().getProperties().getProperty('docker.org')
  }

  stages {
    stage('Build and deploy') {
      steps {
        echo 'Checking out project...'
        checkout scm
        echo 'Building...'
        withMaven(jdk: 'OpenJDK11', maven: 'Maven3.6', mavenSettingsConfig: 'hcomdata-oss-settings') {
          sh 'mvn clean deploy jacoco:report checkstyle:checkstyle spotbugs:spotbugs'
        }
        jacoco()
        recordIssues(
            enabledForFailure: true, aggregatingResults: true,
            tools: [checkStyle(reportEncoding: 'UTF-8'), spotbugs()]
        )
        echo 'Pushing images...'
        withCredentials([usernamePassword(credentialsId: 'dockerhub-egopensource', passwordVariable: 'PASSWORD', usernameVariable: 'USERNAME')]) {
          docker login -u $USERNAME -p $PASSWORD
        }
        docker push $DOCKER_ORG/beekeeper-cleanup
        docker push $DOCKER_ORG/beekeeper-path-scheduler-apiary
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
        withMaven(jdk: 'OpenJDK11', maven: 'Maven3.6', mavenSettingsConfig: 'hcomdata-oss-settings') {
          sh """mvn --batch-mode release:prepare release:perform \
                  -Dresume=false \
                  -DreleaseVersion=${RELEASE_VERSION} \
                  -DdevelopmentVersion=${DEVELOPMENT_VERSION} \
                  -DautoVersionSubmodules=true"""
        }
        echo 'Pushing images...'
        docker tag $DOCKER_ORG/beekeeper-cleanup:${RELEASE_VERSION} $DOCKER_ORG/beekeeper-cleanup:latest
        docker tag $DOCKER_ORG/beekeeper-path-scheduler-apiary:${RELEASE_VERSION} $DOCKER_ORG/beekeeper-cleanup:latest
        docker push $DOCKER_ORG/beekeeper-cleanup
        docker push $DOCKER_ORG/beekeeper-path-scheduler-apiary
      }
    }
  }
}
