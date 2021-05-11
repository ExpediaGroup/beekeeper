/**
 * Copyright (C) 2019-2021 Expedia, Inc.
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

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.monitoring.MetricTag;

@Data
@NoArgsConstructor
@Entity
@Table(name = "housekeeping_path")
public class HousekeepingPath implements HousekeepingEntity {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(name = "path", nullable = false, unique = true)
  private String path;

  @Column(name = "database_name")
  private String databaseName;

  @Column(name = "table_name")
  private String tableName;

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

  @Builder
  public HousekeepingPath(Long id, String path, String databaseName, String tableName,
      HousekeepingStatus housekeepingStatus, LocalDateTime creationTimestamp, LocalDateTime modifiedTimestamp,
      LocalDateTime cleanupTimestamp, Duration cleanupDelay, int cleanupAttempts, String lifecycleType,
      String clientId) {

    this.id = id;
    this.path = path;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.housekeepingStatus = housekeepingStatus;
    this.creationTimestamp = creationTimestamp;
    this.modifiedTimestamp = modifiedTimestamp;
    this.cleanupDelay = cleanupDelay;
    this.cleanupTimestamp = configureCleanupTimestamp();
    this.cleanupAttempts = cleanupAttempts;
    this.lifecycleType = lifecycleType;
    this.clientId = clientId;

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
        "%s(path=%s, databaseName=%s, tableName=%s, housekeepingStatus=%s, creationTimestamp=%s, modifiedTimestamp=%s, cleanupTimestamp=%s, cleanupDelay=%s, cleanupAttempts=%s, clientId=%s, lifecycleType=%s)",
        HousekeepingPath.class.getSimpleName(), path, databaseName, tableName, housekeepingStatus, creationTimestamp,
        modifiedTimestamp, cleanupTimestamp, cleanupDelay, cleanupAttempts, clientId, lifecycleType);
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
