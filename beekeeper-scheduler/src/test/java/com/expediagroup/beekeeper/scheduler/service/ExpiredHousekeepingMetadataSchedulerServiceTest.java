/**
 * Copyright (C) 2019-2025 Expedia, Inc.
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
package com.expediagroup.beekeeper.scheduler.service;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.FAILED_TO_SCHEDULE;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.ArgumentCaptor;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.PeriodDuration;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;
import com.expediagroup.beekeeper.core.service.BeekeeperHistoryService;
import com.expediagroup.beekeeper.scheduler.hive.HiveClient;
import com.expediagroup.beekeeper.scheduler.hive.HiveClientFactory;
import com.expediagroup.beekeeper.scheduler.hive.PartitionInfo;

@ExtendWith(MockitoExtension.class)
public class ExpiredHousekeepingMetadataSchedulerServiceTest {

  private static final String PATH = "path";
  private static final String DATABASE_NAME = "database";
  private static final String TABLE_NAME = "table";
  private static final String PARTITION_NAME = "event_date=2020-01-01/event_hour=0/event_type=A";
  private static final String PARTITION_PATH = PATH + "/" + PARTITION_NAME;
  private static final LocalDateTime CREATION_TIMESTAMP = LocalDateTime.now(ZoneId.of("UTC"));

  @Mock
  private HousekeepingMetadataRepository housekeepingMetadataRepository;

  @Mock
  private BeekeeperHistoryService beekeeperHistoryService;

  @Mock
  private HiveClientFactory hiveClientFactory;

  @Mock
  private HiveClient hiveClient;

  @InjectMocks
  private ExpiredHousekeepingMetadataSchedulerService expiredHousekeepingMetadataSchedulerService;

  @Test
  public void typicalCreateScheduleForHousekeeping() {
    HousekeepingMetadata metadata = createHousekeepingMetadataTable();

    when(housekeepingMetadataRepository.findRecordForCleanupByDbTableAndPartitionName(DATABASE_NAME, TABLE_NAME, null))
        .thenReturn(Optional.empty());
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);

    expiredHousekeepingMetadataSchedulerService.scheduleForHousekeeping(metadata);

    verify(housekeepingMetadataRepository).save(metadata);
    verify(beekeeperHistoryService).saveHistory(metadata, SCHEDULED);
  }

  @Test
  public void typicalCreatePartitionScheduleForHousekeeping() {
    HousekeepingMetadata metadata = createHousekeepingMetadataPartition();
    HousekeepingMetadata tableMetadata = createHousekeepingMetadataTable();
    tableMetadata.setCleanupDelay(PeriodDuration.parse("P1D"));

    when(housekeepingMetadataRepository
        .findRecordForCleanupByDbTableAndPartitionName(DATABASE_NAME, TABLE_NAME, PARTITION_NAME))
        .thenReturn(Optional.empty());
    when(housekeepingMetadataRepository.findRecordForCleanupByDbTableAndPartitionName(DATABASE_NAME, TABLE_NAME, null))
        .thenReturn(Optional.of(tableMetadata));

    expiredHousekeepingMetadataSchedulerService.scheduleForHousekeeping(metadata);

    verify(housekeepingMetadataRepository).save(metadata);
    verify(beekeeperHistoryService).saveHistory(metadata, SCHEDULED);
  }

  @Test
  public void typicalUpdateScheduleForHousekeepingWhenChangingCleanupDelay() {
    HousekeepingMetadata existingTable = spy(createHousekeepingMetadataTable());
    HousekeepingMetadata metadata = createHousekeepingMetadataTable();
    metadata.setCleanupDelay(PeriodDuration.parse("P30D"));

    when(housekeepingMetadataRepository.findRecordForCleanupByDbTableAndPartitionName(DATABASE_NAME, TABLE_NAME, null))
        .thenReturn(Optional.of(existingTable));
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);

    expiredHousekeepingMetadataSchedulerService.scheduleForHousekeeping(metadata);

    verify(existingTable).setPath(metadata.getPath());
    verify(existingTable).setHousekeepingStatus(metadata.getHousekeepingStatus());
    verify(existingTable).setClientId(metadata.getClientId());
    verify(existingTable).setCleanupDelay(metadata.getCleanupDelay());
    verify(housekeepingMetadataRepository).save(existingTable);
    verify(beekeeperHistoryService).saveHistory(metadata, SCHEDULED);
  }

  @Test
  public void typicalUpdatePartitionedTableWithShorterCleanupDelay() {
    HousekeepingMetadata existingTable = spy(createHousekeepingMetadataTable());
    HousekeepingMetadata metadata = createHousekeepingMetadataTable();
    metadata.setCleanupDelay(PeriodDuration.parse("PT3H"));

    when(housekeepingMetadataRepository.findRecordForCleanupByDbTableAndPartitionName(DATABASE_NAME, TABLE_NAME, null))
        .thenReturn(Optional.of(existingTable));
    when(housekeepingMetadataRepository.findMaximumCleanupTimestampForDbAndTable(DATABASE_NAME, TABLE_NAME))
        .thenReturn(CREATION_TIMESTAMP.plus(Duration.parse("P30D")));
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);

    expiredHousekeepingMetadataSchedulerService.scheduleForHousekeeping(metadata);

    verify(existingTable).setPath(metadata.getPath());
    verify(existingTable).setHousekeepingStatus(metadata.getHousekeepingStatus());
    verify(existingTable).setCleanupDelay(metadata.getCleanupDelay());
    verify(existingTable).setClientId(metadata.getClientId());
    // new delay is 3 hours, which is less than the current maximum of 30 days, so cleanup timestamp set as max value
    verify(existingTable).setCleanupTimestamp(CREATION_TIMESTAMP.plus(Duration.parse("P30D")));

    verify(housekeepingMetadataRepository).save(existingTable);
    verify(beekeeperHistoryService).saveHistory(any(), eq(SCHEDULED));
  }

  @Test
  public void verifyLifecycleType() {
    assertThat(expiredHousekeepingMetadataSchedulerService.getLifecycleEventType()).isEqualTo(EXPIRED);
  }

  @Test
  public void useCurrentTimeFromPartitionInfo() {
    HousekeepingMetadata tableMetadata = createHousekeepingMetadataTable();
    LocalDateTime beforeTest = LocalDateTime.now();

    LocalDateTime currentTime = LocalDateTime.now();
    Map<String, PartitionInfo> partitionInfoMap = Map.of(
        PARTITION_NAME, new PartitionInfo(PARTITION_PATH, currentTime));

    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(hiveClient.getTablePartitionsInfo(DATABASE_NAME, TABLE_NAME)).thenReturn(partitionInfoMap);
    when(housekeepingMetadataRepository
        .findRecordForCleanupByDbTableAndPartitionName(DATABASE_NAME, TABLE_NAME, null))
        .thenReturn(Optional.empty());

    expiredHousekeepingMetadataSchedulerService.scheduleForHousekeeping(tableMetadata);

    ArgumentCaptor<HousekeepingMetadata> metadataCaptor = ArgumentCaptor.forClass(HousekeepingMetadata.class);
    verify(housekeepingMetadataRepository, times(2)).save(metadataCaptor.capture());
    
    List<HousekeepingMetadata> savedMetadata = metadataCaptor.getAllValues();
    LocalDateTime afterTest = LocalDateTime.now();

    HousekeepingMetadata partitionMetadata = savedMetadata.stream()
        .filter(metadata -> PARTITION_NAME.equals(metadata.getPartitionName()))
        .findFirst()
        .orElseThrow();

    assertThat(partitionMetadata.getCreationTimestamp()).isNotNull();
    assertThat(partitionMetadata.getCreationTimestamp())
        .isAfterOrEqualTo(beforeTest)
        .isBeforeOrEqualTo(afterTest);
  }

  @Test
  public void scheduleFails() {
    HousekeepingMetadata metadata = createHousekeepingMetadataTable();
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadataRepository.save(metadata)).thenThrow(new RuntimeException());

    assertThatExceptionOfType(BeekeeperException.class)
        .isThrownBy(() -> expiredHousekeepingMetadataSchedulerService.scheduleForHousekeeping(metadata))
        .withMessage(format("Unable to schedule %s", metadata));
    verify(housekeepingMetadataRepository).save(metadata);
    verify(beekeeperHistoryService).saveHistory(any(), eq(FAILED_TO_SCHEDULE));
  }

  @Test
  public void schedulePreexistingTablePartitionsOnTableEvent() {
    HousekeepingMetadata tableMetadata = createHousekeepingMetadataTable();

    String partitionName2 = "event_date=2020-01-01/event_hour=1/event_type=B";
    String partitionName3 = "event_date=2020-01-01/event_hour=3/event_type=C";
    Map<String, PartitionInfo> partitionInfoMap = Map.of(
        PARTITION_NAME, new PartitionInfo(PARTITION_PATH, CREATION_TIMESTAMP),
        partitionName2, new PartitionInfo("path/" + partitionName2, CREATION_TIMESTAMP),
        partitionName3, new PartitionInfo("path/" + partitionName3, CREATION_TIMESTAMP));

    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(hiveClient.getTablePartitionsInfo(DATABASE_NAME, TABLE_NAME)).thenReturn(partitionInfoMap);
    when(housekeepingMetadataRepository
        .findRecordForCleanupByDbTableAndPartitionName(DATABASE_NAME, TABLE_NAME, null))
        .thenReturn(Optional.empty());

    expiredHousekeepingMetadataSchedulerService.scheduleForHousekeeping(tableMetadata);

    ArgumentCaptor<HousekeepingMetadata> metadataCaptor = ArgumentCaptor.forClass(HousekeepingMetadata.class);
    verify(housekeepingMetadataRepository, times(4)).save(metadataCaptor.capture());
    
    List<HousekeepingMetadata> savedMetadata = metadataCaptor.getAllValues();
    for (HousekeepingMetadata metadata : savedMetadata) {
      if (metadata.getPartitionName() != null) {
        assertThat(metadata.getCreationTimestamp()).isEqualTo(CREATION_TIMESTAMP);
      }
    }
    
    verify(beekeeperHistoryService, times(4)).saveHistory(any(), eq(SCHEDULED));
  }

  @Test
  public void updateScheduledPartsAndCreateNewEntriesForExistingPartsOnTableEvent() {
    HousekeepingMetadata existingPartition = spy(createHousekeepingMetadataPartition());
    existingPartition.setCleanupDelay(PeriodDuration.parse("P4M"));
    HousekeepingMetadata existingTable = createHousekeepingMetadataTable();
    existingTable.setCleanupDelay(PeriodDuration.parse("P4M"));

    HousekeepingMetadata tableMetadata = createHousekeepingMetadataTable();
    String partitionName2 = "event_date=2020-01-01/event_hour=1/event_type=B";
    Map<String, PartitionInfo> partitionInfoMap = Map.of(
        PARTITION_NAME, new PartitionInfo(PARTITION_PATH, CREATION_TIMESTAMP),
        partitionName2, new PartitionInfo("path/" + partitionName2, CREATION_TIMESTAMP));

    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(hiveClient.getTablePartitionsInfo(DATABASE_NAME, TABLE_NAME)).thenReturn(partitionInfoMap);

    when(housekeepingMetadataRepository
        .findRecordForCleanupByDbTableAndPartitionName(DATABASE_NAME, TABLE_NAME, null))
        .thenReturn(Optional.of(existingTable));
    when(housekeepingMetadataRepository.findRecordsForCleanupByDbAndTableName(DATABASE_NAME, TABLE_NAME)).thenReturn(
        singletonList(existingPartition));

    expiredHousekeepingMetadataSchedulerService.scheduleForHousekeeping(tableMetadata);

    ArgumentCaptor<HousekeepingMetadata> metadataCaptor = ArgumentCaptor.forClass(HousekeepingMetadata.class);
    verify(housekeepingMetadataRepository, times(3)).save(metadataCaptor.capture());

    List<HousekeepingMetadata> savedMetadata = metadataCaptor.getAllValues();
    for (HousekeepingMetadata metadata : savedMetadata) {
      if (metadata.getPartitionName() != null && !metadata.equals(existingPartition)) {
        assertThat(metadata.getCreationTimestamp()).isEqualTo(CREATION_TIMESTAMP);
      }
    }
    
    verify(existingPartition).setCleanupDelay(PeriodDuration.parse("P3D"));
    verify(beekeeperHistoryService, times(3)).saveHistory(any(), eq(SCHEDULED));
  }

  @Test
  public void integrationTestForPartitionCreationTimeFlow() {
    HousekeepingMetadata tableMetadata = createHousekeepingMetadataTable();

    LocalDateTime olderPartitionCreationTime = LocalDateTime.of(2023, 1, 15, 12, 0);
    LocalDateTime newerPartitionCreationTime = LocalDateTime.of(2024, 2, 20, 8, 30);
    
    String olderPartitionName = "event_date=2023-01-15/event_hour=12";
    String olderPartitionPath = "path/" + olderPartitionName;
    String newerPartitionName = "event_date=2024-02-20/event_hour=8";
    String newerPartitionPath = "path/" + newerPartitionName;

    Map<String, PartitionInfo> partitionInfoMap = Map.of(
        olderPartitionName, new PartitionInfo(olderPartitionPath, olderPartitionCreationTime),
        newerPartitionName, new PartitionInfo(newerPartitionPath, newerPartitionCreationTime)
    );
    
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(hiveClient.getTablePartitionsInfo(DATABASE_NAME, TABLE_NAME)).thenReturn(partitionInfoMap);
    when(housekeepingMetadataRepository
        .findRecordForCleanupByDbTableAndPartitionName(DATABASE_NAME, TABLE_NAME, null))
        .thenReturn(Optional.empty());

    expiredHousekeepingMetadataSchedulerService.scheduleForHousekeeping(tableMetadata);

    ArgumentCaptor<HousekeepingMetadata> metadataCaptor = ArgumentCaptor.forClass(HousekeepingMetadata.class);
    verify(housekeepingMetadataRepository, times(3)).save(metadataCaptor.capture());
    
    List<HousekeepingMetadata> savedMetadata = metadataCaptor.getAllValues();

    Map<String, LocalDateTime> expectedCreationTimes = Map.of(
        olderPartitionName, olderPartitionCreationTime,
        newerPartitionName, newerPartitionCreationTime
    );
    
    for (String partitionName : expectedCreationTimes.keySet()) {
      LocalDateTime expectedTime = expectedCreationTimes.get(partitionName);
      
      HousekeepingMetadata partitionMetadata = savedMetadata.stream()
          .filter(metadata -> partitionName.equals(metadata.getPartitionName()))
          .findFirst()
          .orElseThrow(() -> new AssertionError("Metadata for partition " + partitionName + " not found"));
      
      assertThat(partitionMetadata.getCreationTimestamp()).isEqualTo(expectedTime);

      if (partitionMetadata.getCleanupDelay() != null) {
        Duration cleanupDelay = Duration.parse(tableMetadata.getCleanupDelay().toString());
        LocalDateTime expectedCleanupTime = expectedTime.plus(cleanupDelay);
        assertThat(partitionMetadata.getCleanupTimestamp()).isEqualTo(expectedCleanupTime);
      }
    }
  }

  private HousekeepingMetadata createHousekeepingMetadataPartition() {
    return createEntityHousekeepingTable(PARTITION_NAME);
  }

  private HousekeepingMetadata createHousekeepingMetadataTable() {
    return createEntityHousekeepingTable(null);
  }

  private HousekeepingMetadata createEntityHousekeepingTable(String partitionName) {
    return HousekeepingMetadata
        .builder()
        .path(PATH)
        .databaseName(DATABASE_NAME)
        .tableName(TABLE_NAME)
        .partitionName(partitionName)
        .housekeepingStatus(SCHEDULED)
        .creationTimestamp(CREATION_TIMESTAMP)
        .modifiedTimestamp(CREATION_TIMESTAMP)
        .cleanupDelay(PeriodDuration.parse("P3D"))
        .cleanupAttempts(0)
        .lifecycleType(EXPIRED.toString())
        .build();
  }
}
