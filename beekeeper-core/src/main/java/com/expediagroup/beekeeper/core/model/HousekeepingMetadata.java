/**
 * Copyright (C) 2019-2020 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.expediagroup.beekeeper.core.model;

import static java.lang.String.format;

import java.time.Duration;
import java.time.LocalDateTime;

import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.annotations.UpdateTimestamp;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.monitoring.MetricTag;

@Entity
@Table(name = "housekeeping_metadata")
public class HousekeepingMetadata implements HousekeepingEntity {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(name = "path", nullable = false)
  private String path;

  @Column(name = "database_name", nullable = false)
  private String databaseName;

  @Column(name = "table_name", nullable = false)
  private String tableName;

  @Column(name = "partition_name")
  private String partitionName;

  @Column(name = "housekeeping_status", nullable = false)
  @Enumerated(EnumType.STRING)
  private HousekeepingStatus housekeepingStatus;

  @Column(name = "creation_timestamp", nullable = false, updatable = false)
  private LocalDateTime creationTimestamp;

  @Column(name = "modified_timestamp")
  @UpdateTimestamp
  private LocalDateTime modifiedTimestamp;

  @Column(name = "cleanup_timestamp", nullable = false)
  private LocalDateTime cleanupTimestamp;

  @Column(name = "cleanup_delay", nullable = false)
  @Convert(converter = DurationConverter.class)
  private Duration cleanupDelay;

  @Column(name = "cleanup_attempts", nullable = false)
  private int cleanupAttempts;

  @Column(name = "client_id")
  private String clientId;
  @Column(name = "lifecycle_type", nullable = false)
  private String lifecycleType;

  public HousekeepingMetadata() {

  }

  private HousekeepingMetadata(Long id, String path, String databaseName, String tableName,
      String partitionName, HousekeepingStatus housekeepingStatus, LocalDateTime creationTimestamp,
      LocalDateTime modifiedTimestamp, LocalDateTime cleanupTimestamp, Duration cleanupDelay, int cleanupAttempts,
      String lifecycleType, String clientId) {
    this.id = id;
    this.path = path;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.partitionName = partitionName;
    this.housekeepingStatus = housekeepingStatus;
    this.creationTimestamp = creationTimestamp;
    this.modifiedTimestamp = modifiedTimestamp;
    this.cleanupTimestamp = cleanupTimestamp;
    this.cleanupDelay = cleanupDelay;
    this.cleanupAttempts = cleanupAttempts;
    this.lifecycleType = lifecycleType;
    this.clientId = clientId;
  }

  @Override
  public String getLifecycleType() {
    return lifecycleType;
  }

  public void setLifecycleType(String lifecycleType) {
    this.lifecycleType = lifecycleType;
  }

  @Override
  public Long getId() {
    return id;
  }

  @Override
  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  @Override
  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * @return The full name of the partition in hive e.g. event_date=2020-01-01/event_hour=0
   */
  public String getPartitionName() {
    return partitionName;
  }

  public void setPartitionName(String partitionName) {
    this.partitionName = partitionName;
  }

  @Override
  public HousekeepingStatus getHousekeepingStatus() {
    return housekeepingStatus;
  }

  public void setHousekeepingStatus(HousekeepingStatus housekeepingStatus) {
    this.housekeepingStatus = housekeepingStatus;
  }

  @Override
  public LocalDateTime getCreationTimestamp() {
    return creationTimestamp;
  }

  @Override
  public LocalDateTime getModifiedTimestamp() {
    return modifiedTimestamp;
  }

  @Override
  public LocalDateTime getCleanupTimestamp() {
    return cleanupTimestamp;
  }

  public void setCleanupTimestamp(LocalDateTime cleanupTimestamp) {
    this.cleanupTimestamp = cleanupTimestamp;
  }

  @Override
  public int getCleanupAttempts() {
    return cleanupAttempts;
  }

  public void setCleanupAttempts(int cleanupAttempts) {
    this.cleanupAttempts = cleanupAttempts;
  }

  @Override
  public String getClientId() {
    return clientId;
  }

  public void setClientId(String clientId) {
    this.clientId = clientId;
  }

  @Override
  public Duration getCleanupDelay() {
    return cleanupDelay;
  }

  public void setCleanupDelay(Duration cleanupDelay) {
    this.cleanupDelay = cleanupDelay;
    cleanupTimestamp = creationTimestamp.plus(cleanupDelay);
  }

  @Override
  public MetricTag getMetricTag() {
    return new MetricTag("table", String.join(".", databaseName, tableName));
  }

  @Override
  public String toString() {
    return format(
        "%s(path=%s, databaseName=%s, tableName=%s, partitionName=%s, housekeepingStatus=%s, creationTimestamp=%s, modifiedTimestamp=%s, cleanupTimestamp=%s, cleanupDelay=%s, cleanupAttempts=%s, clientId=%s, lifecycleType=%s)",
        HousekeepingMetadata.class.getSimpleName(), path, databaseName, tableName, partitionName, housekeepingStatus,
        creationTimestamp, modifiedTimestamp, cleanupTimestamp, cleanupDelay, cleanupAttempts, clientId, lifecycleType);
  }

  public static final class Builder {

    private Long id;
    private String path;
    private String databaseName;
    private String tableName;
    private String partitionName;
    private HousekeepingStatus housekeepingStatus;
    private LocalDateTime creationTimestamp;
    private LocalDateTime modifiedTimestamp;
    private Duration cleanupDelay;
    private int cleanupAttempts;
    private String lifecycleType;
    private String clientId;

    public Builder() { }

    public HousekeepingMetadata.Builder id(Long id) {
      this.id = id;
      return this;
    }

    public HousekeepingMetadata.Builder path(String path) {
      this.path = path;
      return this;
    }

    public HousekeepingMetadata.Builder databaseName(String databaseName) {
      this.databaseName = databaseName;
      return this;
    }

    public HousekeepingMetadata.Builder tableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public HousekeepingMetadata.Builder partitionName(String partitionName) {
      this.partitionName = partitionName;
      return this;
    }

    public HousekeepingMetadata.Builder housekeepingStatus(HousekeepingStatus housekeepingStatus) {
      this.housekeepingStatus = housekeepingStatus;
      return this;
    }

    public HousekeepingMetadata.Builder creationTimestamp(LocalDateTime creationTimestamp) {
      this.creationTimestamp = creationTimestamp;
      return this;
    }

    public HousekeepingMetadata.Builder modifiedTimestamp(LocalDateTime modifiedTimestamp) {
      this.modifiedTimestamp = modifiedTimestamp;
      return this;
    }

    public HousekeepingMetadata.Builder cleanupDelay(Duration cleanupDelay) {
      this.cleanupDelay = cleanupDelay;
      return this;
    }

    public HousekeepingMetadata.Builder cleanupAttempts(int cleanupAttempts) {
      this.cleanupAttempts = cleanupAttempts;
      return this;
    }

    public HousekeepingMetadata.Builder lifecycleType(String lifecycleType) {
      this.lifecycleType = lifecycleType;
      return this;
    }

    public HousekeepingMetadata.Builder clientId(String clientId) {
      this.clientId = clientId;
      return this;
    }

    public HousekeepingMetadata build() {
      LocalDateTime cleanupTimestamp = configureCleanupTimestamp();

      return new HousekeepingMetadata(id, path, databaseName, tableName, partitionName, housekeepingStatus,
          creationTimestamp, modifiedTimestamp, cleanupTimestamp, cleanupDelay, cleanupAttempts, lifecycleType,
          clientId);
    }

    private LocalDateTime configureCleanupTimestamp() {
      if (creationTimestamp == null) {
        throw new BeekeeperException("Table requires a creation timestamp");
      }
      if (cleanupDelay == null) {
        throw new BeekeeperException("Table requires a cleanup delay");
      }
      return creationTimestamp.plus(cleanupDelay);
    }
  }
}
