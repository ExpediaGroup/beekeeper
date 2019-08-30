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

    withCredentials([file(credentialsId: 'eg-oss-settings.xml', variable: 'SETTINGS')]) {
      MAVEN_SETTINGS = SETTINGS
    }
    OSS_GPG_PUB_KEYRING = credentials('pubring.gpg')
    OSS_GPG_SEC_KEYRING = credentials('secring.gpg')
    OSS_GPG_PASSPHRASE = credentials('private-key-passphrase')

    pom = readMavenPom file: 'pom.xml'
    PROJECT_VERSION = pom.version
  }

  stages {
    stage('Build and deploy') {
      steps {
        echo 'Checking out project...'
        checkout scm
        echo 'Building...'
        echo 'Maven Settings'
        echo $MAVEN_SETTINGS
        echo 'Project version'
        echo $PROJECT_VERSION
        withMaven(jdk: 'OpenJDK11', maven: 'Maven3.6') {
          sh 'mvn clean deploy jacoco:report checkstyle:checkstyle spotbugs:spotbugs --settings $MAVEN_SETTINGS'
        }
        jacoco()
        recordIssues(
            enabledForFailure: true, aggregatingResults: true,
            tools: [checkStyle(reportEncoding: 'UTF-8'), spotbugs()]
        )
        echo 'Pushing images...'
        docker images
        withCredentials([usernamePassword(credentialsId: 'dockerhub-egopensource', passwordVariable: 'PASSWORD', usernameVariable: 'USERNAME')]) {
          docker login -u $USERNAME -p $PASSWORD
        }
        // docker push $DOCKER_ORG/beekeeper-cleanup
        // docker push $DOCKER_ORG/beekeeper-path-scheduler-apiary
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
        withMaven(jdk: 'OpenJDK11', maven: 'Maven3.6') {
          sh """mvn --batch-mode release:prepare release:perform \
                  -Dresume=false \
                  -DreleaseVersion=${RELEASE_VERSION} \
                  -DdevelopmentVersion=${DEVELOPMENT_VERSION} \
                  -DautoVersionSubmodules=true \
                  --settings $MAVEN_SETTINGS"""
        }
        echo 'Pushing images...'
        docker tag $DOCKER_ORG/beekeeper-cleanup:${RELEASE_VERSION} $DOCKER_ORG/beekeeper-cleanup:latest
        docker tag $DOCKER_ORG/beekeeper-path-scheduler-apiary:${RELEASE_VERSION} $DOCKER_ORG/beekeeper-path-scheduler-apiary:latest
        docker push $DOCKER_ORG/beekeeper-cleanup
        docker push $DOCKER_ORG/beekeeper-path-scheduler-apiary
      }
    }
  }
}
