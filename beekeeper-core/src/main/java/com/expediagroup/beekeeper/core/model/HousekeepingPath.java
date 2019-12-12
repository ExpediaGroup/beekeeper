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

public interface HousekeepingPath {

  Long getId();

  String getPath();

  void setPath(String path);

  String getDatabaseName();

  void setDatabaseName(String databaseName);

  String getTableName();

  void setTableName(String tableName);

  PathStatus getPathStatus();

  void setPathStatus(PathStatus pathStatus);

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

  String getCleanupType();

  void setCleanupType(String cleanupType);

  String getClientId();

  void setClientId(String clientId);
}
