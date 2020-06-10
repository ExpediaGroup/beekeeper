package com.expediagroup.beekeeper.core.model;

import java.time.Duration;
import java.time.LocalDateTime;

import com.expediagroup.beekeeper.core.monitoring.Taggable;

public interface Housekeeping extends Taggable {

  String getLifecycleType();

  void setLifecycleType(String lifecycleType);

  Long getId();

  String getDatabaseName();

  void setDatabaseName(String databaseName);

  String getTableName();

  void setTableName(String tableName);

  HousekeepingStatus getHousekeepingStatus();

  void setHousekeepingStatus(HousekeepingStatus housekeepingStatus);

  Duration getCleanupDelay();

  void setCleanupDelay(Duration cleanupDelay);

  LocalDateTime getCreationTimestamp();

  void setCreationTimestamp(LocalDateTime creationTimestamp);

  LocalDateTime getModifiedTimestamp();

  void setModifiedTimestamp(LocalDateTime modifiedTimestamp);

  LocalDateTime getCleanupTimestamp();

  void setCleanupTimestamp(LocalDateTime cleanupTimestamp);

  int getCleanupAttempts();

  void setCleanupAttempts(int cleanupAttempts);

  String getClientId();

  void setClientId(String clientId);
}
