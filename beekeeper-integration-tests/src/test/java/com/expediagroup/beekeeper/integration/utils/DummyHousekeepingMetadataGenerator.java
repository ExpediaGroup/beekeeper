package com.expediagroup.beekeeper.integration.utils;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;

public class DummyHousekeepingMetadataGenerator {

  private static final String DEFAULT_DB_NAME = "randomDatabase";
  private static final String DEFAULT_TABLE_NAME = "randomTable";
  private static final LocalDateTime CREATION_TIMESTAMP = LocalDateTime.now(ZoneId.of("UTC"));
  private static final Duration CLEANUP_DELAY = Duration.parse("P3D");

  public static HousekeepingMetadata generateDummyHousekeepingMetadata() {
    return generateDummyHousekeepingMetadata(DEFAULT_DB_NAME, DEFAULT_TABLE_NAME);
  }

  public static HousekeepingMetadata generateDummyHousekeepingMetadata(String tableName, String databaseName) {
    return HousekeepingMetadata.builder()
        .path("s3://some/path/")
        .databaseName(databaseName)
        .tableName(tableName)
        .partitionName("event_date=2020-01-01/event_hour=0/event_type=A")
        .housekeepingStatus(SCHEDULED)
        .creationTimestamp(CREATION_TIMESTAMP)
        .modifiedTimestamp(CREATION_TIMESTAMP)
        .cleanupDelay(Duration.parse("P3D"))
        .cleanupAttempts(0)
        .lifecycleType(EXPIRED.toString())
        .build();
  }
}

