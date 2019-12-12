/**
 * Copyright (C) 2019 Expedia, Inc.
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
import com.expediagroup.beekeeper.core.monitoring.Taggable;

@Entity
@Table(name = "path")
public class EntityHousekeepingPath implements HousekeepingPath, Taggable {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(name = "path", nullable = false, unique = true)
  private String path;

  @Column(name = "database_name")
  private String databaseName;

  @Column(name = "table_name")
  private String tableName;

  @Column(name = "path_status", nullable = false)
  @Enumerated(EnumType.STRING)
  private PathStatus pathStatus;

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

  @Column(name = "cleanup_type", nullable = false)
  private String cleanupType;

  @Column(name = "client_id")
  private String clientId;

  public EntityHousekeepingPath() {

  }

  private EntityHousekeepingPath(Long id, String path, String databaseName, String tableName,
      PathStatus pathStatus, LocalDateTime creationTimestamp, LocalDateTime modifiedTimestamp,
      LocalDateTime cleanupTimestamp, Duration cleanupDelay, int cleanupAttempts, String cleanupType,
      String clientId) {
    this.id = id;
    this.path = path;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.pathStatus = pathStatus;
    this.creationTimestamp = creationTimestamp;
    this.modifiedTimestamp = modifiedTimestamp;
    this.cleanupTimestamp = cleanupTimestamp;
    this.cleanupDelay = cleanupDelay;
    this.cleanupAttempts = cleanupAttempts;
    this.cleanupType = cleanupType;
    this.clientId = clientId;
  }

  @Override
  public Long getId() {
    return id;
  }

  @Override
  public String getPath() {
    return path;
  }

  @Override
  public void setPath(String path) {
    this.path = path;
  }

  @Override
  public String getDatabaseName() {
    return databaseName;
  }

  @Override
  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  @Override
  public PathStatus getPathStatus() {
    return pathStatus;
  }

  @Override
  public void setPathStatus(PathStatus pathStatus) {
    this.pathStatus = pathStatus;
  }

  @Override
  public LocalDateTime getCreationTimestamp() {
    return creationTimestamp;
  }

  @Override
  public void setCreationTimestamp(LocalDateTime creationTimestamp) {
    this.creationTimestamp = creationTimestamp;
  }

  @Override
  public LocalDateTime getModifiedTimestamp() {
    return modifiedTimestamp;
  }

  @Override
  public void setModifiedTimestamp(LocalDateTime modifiedTimestamp) {
    this.modifiedTimestamp = modifiedTimestamp;
  }

  @Override
  public LocalDateTime getCleanupTimestamp() {
    return cleanupTimestamp;
  }

  @Override
  public void setCleanupTimestamp(LocalDateTime cleanupTimestamp) {
    this.cleanupTimestamp = cleanupTimestamp;
  }

  @Override
  public int getCleanupAttempts() {
    return cleanupAttempts;
  }

  @Override
  public void setCleanupAttempts(int cleanupAttempts) {
    this.cleanupAttempts = cleanupAttempts;
  }

  @Override
  public String getCleanupType() {
    return cleanupType;
  }

  @Override
  public void setCleanupType(String cleanupType) {
    this.cleanupType = cleanupType;
  }

  @Override
  public String getClientId() {
    return clientId;
  }

  @Override
  public void setClientId(String clientId) {
    this.clientId = clientId;
  }

  @Override
  public Duration getCleanupDelay() {
    return cleanupDelay;
  }

  @Override
  public void setCleanupDelay(Duration cleanupDelay) {
    this.cleanupDelay = cleanupDelay;
    cleanupTimestamp = creationTimestamp.plus(cleanupDelay);
  }

  @Override
  public MetricTag getMetricTag() {
    return new MetricTag("table", String.join(".", databaseName, tableName));
  }

  public static final class Builder {

    private Long id;
    private String path;
    private String databaseName;
    private String tableName;
    private PathStatus pathStatus;
    private LocalDateTime creationTimestamp;
    private LocalDateTime modifiedTimestamp;
    private LocalDateTime cleanupTimestamp;
    private Duration cleanupDelay;
    private int cleanupAttempts;
    private String cleanupType;
    private String clientId;

    public Builder() { }

    public Builder id(Long id) {
      this.id = id;
      return this;
    }

    public Builder path(String path) {
      this.path = path;
      return this;
    }

    public Builder pathStatus(PathStatus pathStatus) {
      this.pathStatus = pathStatus;
      return this;
    }

    public Builder databaseName(String databaseName) {
      this.databaseName = databaseName;
      return this;
    }

    public Builder tableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder creationTimestamp(LocalDateTime creationTimestamp) {
      this.creationTimestamp = creationTimestamp;
      return this;
    }

    public Builder modifiedTimestamp(LocalDateTime modifiedTimestamp) {
      this.modifiedTimestamp = modifiedTimestamp;
      return this;
    }

    public Builder cleanupDelay(Duration cleanupDelay) {
      this.cleanupDelay = cleanupDelay;
      return this;
    }

    public Builder cleanupAttempts(int cleanupAttempts) {
      this.cleanupAttempts = cleanupAttempts;
      return this;
    }

    public Builder cleanupType(String cleanupType) {
      this.cleanupType = cleanupType;
      return this;
    }

    public Builder clientId(String clientId) {
      this.clientId = clientId;
      return this;
    }

    public EntityHousekeepingPath build() {
      cleanupTimestamp = configureCleanupTimestamp();

      return new EntityHousekeepingPath(id, path, databaseName, tableName, pathStatus,
          creationTimestamp, modifiedTimestamp,
          cleanupTimestamp, cleanupDelay, cleanupAttempts, cleanupType, clientId);
    }

    private LocalDateTime configureCleanupTimestamp() {
      if (creationTimestamp == null) {
        throw new BeekeeperException("Path requires a creation timestamp");
      }
      if (cleanupDelay == null) {
        throw new BeekeeperException("Path requires a cleanup delay");
      }
      return creationTimestamp.plus(cleanupDelay);
    }

  }
}
