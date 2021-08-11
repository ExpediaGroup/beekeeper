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
package com.expediagroup.beekeeper.api.util;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;

public class DummyHousekeepingEntityGenerator {

  private static final String DEFAULT_DB_NAME = "randomDatabase";
  private static final String DEFAULT_TABLE_NAME = "randomTable";
  private static final LocalDateTime CREATION_TIMESTAMP = LocalDateTime.now(ZoneId.of("UTC"));
  private static final Duration CLEANUP_DELAY = Duration.ofDays(3);

  public static HousekeepingMetadata generateDummyHousekeepingMetadata() {
    return generateDummyHousekeepingMetadata(DEFAULT_DB_NAME, DEFAULT_TABLE_NAME);
  }

  public static HousekeepingMetadata generateDummyHousekeepingMetadata(String databaseName, String tableName) {
    return HousekeepingMetadata
        .builder()
        .path("s3://some/path/")
        .databaseName(databaseName)
        .tableName(tableName)
        .partitionName("event_date=2020-01-01/event_hour=0/event_type=A")
        .housekeepingStatus(SCHEDULED)
        .creationTimestamp(CREATION_TIMESTAMP)
        .modifiedTimestamp(CREATION_TIMESTAMP)
        .cleanupDelay(CLEANUP_DELAY)
        .cleanupAttempts(0)
        .lifecycleType(EXPIRED.toString())
        .build();
  }

  public static HousekeepingPath generateDummyHousekeepingPath(String databaseName, String tableName) {
    return HousekeepingPath
        .builder()
        .path("s3://some/path/")
        .databaseName(databaseName)
        .tableName(tableName)
        .housekeepingStatus(SCHEDULED)
        .creationTimestamp(CREATION_TIMESTAMP)
        .modifiedTimestamp(CREATION_TIMESTAMP)
        .cleanupDelay(CLEANUP_DELAY)
        .cleanupAttempts(0)
        .lifecycleType(EXPIRED.toString())
        .build();
  }

}
