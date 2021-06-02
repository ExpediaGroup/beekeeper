package com.expediagroup.beekeeper.api.response;

import static org.junit.jupiter.api.Assertions.assertEquals;

import static com.expediagroup.beekeeper.api.response.HousekeepingMetadataResponse.convertToHouseKeepingMetadataResponse;
import static com.expediagroup.beekeeper.api.util.DummyHousekeepingMetadataGenerator.generateDummyHousekeepingMetadata;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.junit.jupiter.api.Test;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;

public class HousekeepingMetadataResponseTest {

  private static final String databaseName = "randomDatabase";
  private static final String tableName = "randomTable";
  private static final String path = "s3://some/path/";
  private static final String partitionName = null;
  private static final HousekeepingStatus housekeepingStatus = SCHEDULED;
  private static final LocalDateTime creationTimestamp = LocalDateTime.now(ZoneId.of("UTC"));
  private static final Duration cleanupDelay = Duration.parse("P3D");
  private static final int cleanupAttempts = 0;
  private static final String lifecycleEventType = EXPIRED.toString();

  @Test
  public void testConvertToHouseKeepingMetadataResponse(){

    HousekeepingMetadata metadata = HousekeepingMetadata.builder()
        .path(path)
        .databaseName(databaseName)
        .tableName(tableName)
        .partitionName(partitionName)
        .housekeepingStatus(housekeepingStatus)
        .creationTimestamp(creationTimestamp)
        .modifiedTimestamp(creationTimestamp)
        .cleanupDelay(cleanupDelay)
        .cleanupAttempts(cleanupAttempts)
        .lifecycleType(lifecycleEventType)
        .build();

    HousekeepingMetadataResponse housekeepingMetadataResponse = convertToHouseKeepingMetadataResponse(metadata);
    assertEquals(housekeepingMetadataResponse.getPath(),path);
    assertEquals(housekeepingMetadataResponse.getDatabaseName(),databaseName);
    assertEquals(housekeepingMetadataResponse.getTableName(),tableName);
    assertEquals(housekeepingMetadataResponse.getPartitionName(),partitionName);
    assertEquals(housekeepingMetadataResponse.getHousekeepingStatus(),housekeepingStatus);
    assertEquals(housekeepingMetadataResponse.getCreationTimestamp(),creationTimestamp);
    assertEquals(housekeepingMetadataResponse.getModifiedTimestamp(),creationTimestamp);
    assertEquals(housekeepingMetadataResponse.getCleanupDelay(),cleanupDelay);
    assertEquals(housekeepingMetadataResponse.getCleanupAttempts(),cleanupAttempts);
    assertEquals(housekeepingMetadataResponse.getLifecycleType(),lifecycleEventType);
  }

}
