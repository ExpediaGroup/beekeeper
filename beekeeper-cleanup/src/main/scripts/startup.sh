#!/usr/bin/env bash

if [[ $DB_PASSWORD_STRATEGY == "aws-secrets-manager" ]]; then
  curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
  unzip awscliv2.zip
  ./aws/install
  db_password=--spring.datasource.password=$(aws secretsmanager get-secret-value --secret-id $DB_PASSWORD_KEY --query 'SecretString' --output text)
fi

if [[ ! -z $BEEKEEPER_CONFIG ]]; then
  echo "$BEEKEEPER_CONFIG"|base64 -d > ./conf/beekeeper-config.yml
  java -jar /app/libs/beekeeper-cleanup-app-$APP_VERSION.jar --config=./conf/beekeeper-config.yml $db_password
else
  java -jar /app/libs/beekeeper-cleanup-app-$APP_VERSION.jar $db_password
fi
