#!/usr/bin/env bash

if [[ $DB_PASSWORD_STRATEGY == "aws-secrets-manager" ]]; then
  # This should go, we shouldn't be install aws cli here
  curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
  unzip awscliv2.zip
  ./aws/install
  db_password=--spring.datasource.password=$(aws secretsmanager get-secret-value --secret-id $DB_PASSWORD_KEY --query 'SecretString' --output text)
fi

if [[ ! -z $BEEKEEPER_CONFIG ]]; then
  mkdir /app/conf
  echo "$BEEKEEPER_CONFIG" | base64 -d >/app/conf/beekeeper-config.yml
  java -cp /app/resources:/app/classes:/app/libs/* com.expediagroup.beekeeper.scheduler.apiary.BeekeeperPathSchedulerApiary --config=/app/conf/beekeeper-config.yml $db_password
else
  java -cp /app/resources:/app/classes:/app/libs/* com.expediagroup.beekeeper.scheduler.apiary.BeekeeperPathSchedulerApiary $db_password
fi

