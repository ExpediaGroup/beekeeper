#!/usr/bin/env bash

if [[ $DB_PASSWORD_STRATEGY == "aws-secrets-manager" ]]; then
  db_password=--spring.datasource.password=$(aws secretsmanager get-secret-value --secret-id $DB_PASSWORD_KEY --query 'SecretString' --output text)
fi

if [[ ! -z $BEEKEEPER_CONFIG ]]; then
  echo "$BEEKEEPER_CONFIG"|base64 -d > ./conf/beekeeper-config.yml
  java -jar ./lib/beekeeper-scheduler-apiary-app.jar --config=./conf/beekeeper-config.yml $db_password
else
  java -jar ./lib/beekeeper-scheduler-apiary-app.jar $db_password
fi
