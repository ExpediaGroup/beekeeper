package com.expediagroup.beekeeper.integration;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;

class ResultSetToHousekeepingEntityMapper {

  static final String ID = "id";
  static final String PATH = "path";
  static final String DATABASE_NAME = "database_name";
  static final String TABLE_NAME = "table_name";
  static final String PARTITION_NAME = "partition_name";
  static final String HOUSEKEEPING_STATUS = "housekeeping_status";
  static final String CREATION_TIMESTAMP = "creation_timestamp";
  static final String MODIFIED_TIMESTAMP = "modified_timestamp";
  static final String CLEANUP_TIMESTAMP = "cleanup_timestamp";
  static final String CLEANUP_DELAY = "cleanup_delay";
  static final String CLEANUP_ATTEMPTS = "cleanup_attempts";
  static final String CLIENT_ID = "client_id";
  static final String LIFECYCLE_TYPE = "lifecycle_type";

  static HousekeepingPath mapToHousekeepingPath(ResultSet resultSet) throws SQLException {
    return new HousekeepingPath.Builder()
        .id(resultSet.getLong(ID))
        .path(resultSet.getString(PATH))
        .databaseName(resultSet.getString(DATABASE_NAME))
        .tableName(resultSet.getString(TABLE_NAME))
        .housekeepingStatus(HousekeepingStatus.valueOf(resultSet.getString(HOUSEKEEPING_STATUS)))
        .creationTimestamp(Timestamp.valueOf(resultSet.getString(CREATION_TIMESTAMP)).toLocalDateTime())
        .modifiedTimestamp(Timestamp.valueOf(resultSet.getString(MODIFIED_TIMESTAMP)).toLocalDateTime())
        .cleanupDelay(Duration.parse(resultSet.getString(CLEANUP_DELAY)))
        .cleanupAttempts(resultSet.getInt(CLEANUP_ATTEMPTS))
        .clientId(resultSet.getString(CLIENT_ID))
        .lifecycleType(resultSet.getString(LIFECYCLE_TYPE))
        .build();
  }

  static HousekeepingMetadata mapToHousekeepingMetadata(ResultSet resultSet) throws SQLException {
    return new HousekeepingMetadata.Builder()
        .id(resultSet.getLong(ID))
        .path(resultSet.getString(PATH))
        .databaseName(resultSet.getString(DATABASE_NAME))
        .tableName(resultSet.getString(TABLE_NAME))
        .partitionName(resultSet.getString(PARTITION_NAME))
        .housekeepingStatus(HousekeepingStatus.valueOf(resultSet.getString(HOUSEKEEPING_STATUS)))
        .creationTimestamp(Timestamp.valueOf(resultSet.getString(CREATION_TIMESTAMP)).toLocalDateTime())
        .modifiedTimestamp(Timestamp.valueOf(resultSet.getString(MODIFIED_TIMESTAMP)).toLocalDateTime())
        .cleanupDelay(Duration.parse(resultSet.getString(CLEANUP_DELAY)))
        .cleanupAttempts(resultSet.getInt(CLEANUP_ATTEMPTS))
        .clientId(resultSet.getString(CLIENT_ID))
        .lifecycleType(resultSet.getString(LIFECYCLE_TYPE))
        .build();
  }
}
