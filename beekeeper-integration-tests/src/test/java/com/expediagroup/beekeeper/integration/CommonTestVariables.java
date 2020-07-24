package com.expediagroup.beekeeper.integration;

import static java.time.ZoneOffset.UTC;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public final class CommonTestVariables {

  // AWS
  public static final String AWS_REGION = "us-west-2";
  public static final String AWS_S3_BUCKET = "test-path-bucket";

  // HOUSEKEEPINGENTITY COLUMNS
  public static final String ID_FIELD = "id";
  public static final String PATH_FIELD = "path";
  public static final String DATABASE_NAME_FIELD = "database_name";
  public static final String TABLE_NAME_FIELD = "table_name";
  public static final String PARTITION_NAME_FIELD = "partition_name";
  public static final String HOUSEKEEPING_STATUS_FIELD = "housekeeping_status";
  public static final String CREATION_TIMESTAMP_FIELD = "creation_timestamp";
  public static final String MODIFIED_TIMESTAMP_FIELD = "modified_timestamp";
  public static final String CLEANUP_TIMESTAMP_FIELD = "cleanup_timestamp";
  public static final String CLEANUP_DELAY_FIELD = "cleanup_delay";
  public static final String CLEANUP_ATTEMPTS_FIELD = "cleanup_attempts";
  public static final String CLIENT_ID_FIELD = "client_id";
  public static final String LIFECYCLE_TYPE_FIELD = "lifecycle_type";

  // HOUSEKEEPINGENTITY DEFAULT VALUES
  public static final String DATABASE_NAME_VALUE = "some_database";
  public static final String TABLE_NAME_VALUE = "some_table";
  public static final LocalDateTime CREATION_TIMESTAMP_VALUE = LocalDateTime.now(UTC).minus(1L, ChronoUnit.MINUTES);
  public static final String CLEANUP_DELAY_VALUE = "PT1S";
  public static final int CLEANUP_ATTEMPTS_VALUE = 0;
  public static final String CLIENT_ID_VALUE = "apiary-metastore-event";
}
