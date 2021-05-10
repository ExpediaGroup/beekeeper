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
package com.expediagroup.beekeeper.scheduler.service;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;

@ExtendWith(MockitoExtension.class)
public class ExpiredHousekeepingMetadataSchedulerServiceTest {

  private static final String PATH = "path";
  private static final String DATABASE_NAME = "database";
  private static final String TABLE_NAME = "table";
  private static final String PARTITION_NAME = "event_date=2020-01-01/event_hour=0/event_type=A";
  private static final LocalDateTime CREATION_TIMESTAMP = LocalDateTime.now(ZoneId.of("UTC"));

  @Mock
  private HousekeepingMetadataRepository housekeepingMetadataRepository;

  @InjectMocks
  private ExpiredHousekeepingMetadataSchedulerService expiredHousekeepingMetadataSchedulerService;

  @Test
  public void typicalCreateScheduleForHousekeeping() {
    HousekeepingMetadata metadata = createHousekeepingMetadataTable();

    when(housekeepingMetadataRepository.findRecordForCleanupByDbTableAndPartitionName(DATABASE_NAME, TABLE_NAME,
        null)).thenReturn(Optional.empty());

    expiredHousekeepingMetadataSchedulerService.scheduleForHousekeeping(metadata);

    verify(housekeepingMetadataRepository).save(metadata);
  }

  @Test
  public void typicalCreatePartitionScheduleForHousekeeping() {
    HousekeepingMetadata metadata = createHousekeepingMetadataPartition();
    HousekeepingMetadata tableMetadata = createHousekeepingMetadataTable();

    when(housekeepingMetadataRepository.findRecordForCleanupByDbTableAndPartitionName(DATABASE_NAME, TABLE_NAME,
        PARTITION_NAME)).thenReturn(Optional.empty());
    when(housekeepingMetadataRepository.findRecordForCleanupByDbTableAndPartitionName(DATABASE_NAME, TABLE_NAME,
        null)).thenReturn(Optional.of(tableMetadata));

    expiredHousekeepingMetadataSchedulerService.scheduleForHousekeeping(metadata);

    verify(housekeepingMetadataRepository).save(metadata);
  }

  @Test
  public void typicalUpdateScheduleForHousekeepingWhenChangingCleanupDelay() {
    HousekeepingMetadata existingTable = spy(createHousekeepingMetadataTable());
    HousekeepingMetadata metadata = createHousekeepingMetadataTable();
    metadata.setCleanupDelay(Duration.parse("P30D"));

    when(housekeepingMetadataRepository.findRecordForCleanupByDbTableAndPartitionName(DATABASE_NAME, TABLE_NAME,
        null)).thenReturn(Optional.of(existingTable));

    expiredHousekeepingMetadataSchedulerService.scheduleForHousekeeping(metadata);

    verify(existingTable).setPath(metadata.getPath());
    verify(existingTable).setHousekeepingStatus(metadata.getHousekeepingStatus());
    verify(existingTable).setClientId(metadata.getClientId());
    verify(existingTable).setCleanupDelay(metadata.getCleanupDelay());

    //if we uncomment his line exception disappears
    //verifyNoMoreInteractions(existingTable);
    verify(housekeepingMetadataRepository).save(existingTable);
  }

  @Test
  public void typicalUpdatePartitionedTableWithShorterCleanupDelay() {
    HousekeepingMetadata existingTable = spy(createHousekeepingMetadataTable());
    HousekeepingMetadata metadata = createHousekeepingMetadataTable();
    metadata.setCleanupDelay(Duration.parse("PT3H"));

    when(housekeepingMetadataRepository.findRecordForCleanupByDbTableAndPartitionName(DATABASE_NAME, TABLE_NAME,
        null)).thenReturn(Optional.of(existingTable));
    when(housekeepingMetadataRepository.findMaximumCleanupTimestampForDbAndTable(DATABASE_NAME, TABLE_NAME)).thenReturn(
        CREATION_TIMESTAMP.plus(Duration.parse("P30D")));
    when(housekeepingMetadataRepository.countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE_NAME,
        TABLE_NAME)).thenReturn(1L);

    expiredHousekeepingMetadataSchedulerService.scheduleForHousekeeping(metadata);

    verify(existingTable).setPath(metadata.getPath());
    verify(existingTable).setHousekeepingStatus(metadata.getHousekeepingStatus());
    verify(existingTable).setCleanupDelay(metadata.getCleanupDelay());
    verify(existingTable).setClientId(metadata.getClientId());
    // new delay is 3 hours, which is less than the current maximum of 30 days, so cleanup timestamp set as max value
    verify(existingTable).setCleanupTimestamp(CREATION_TIMESTAMP.plus(Duration.parse("P30D")));

    verify(housekeepingMetadataRepository).save(existingTable);
  }

  @Test
  public void verifyLifecycleType() {
    assertThat(expiredHousekeepingMetadataSchedulerService.getLifecycleEventType())
        .isEqualTo(EXPIRED);
  }

  @Test
  public void scheduleFails() {
    HousekeepingMetadata metadata = createHousekeepingMetadataTable();

    when(housekeepingMetadataRepository.save(metadata)).thenThrow(new RuntimeException());

    assertThatExceptionOfType(BeekeeperException.class)
        .isThrownBy(() -> expiredHousekeepingMetadataSchedulerService.scheduleForHousekeeping(metadata))
        .withMessage(format("Unable to schedule %s", metadata));
    verify(housekeepingMetadataRepository).save(metadata);
  }

  private HousekeepingMetadata createHousekeepingMetadataPartition() {
    return createEntityHousekeepingTable(PARTITION_NAME);
  }

  private HousekeepingMetadata createHousekeepingMetadataTable() {
    return createEntityHousekeepingTable(null);
  }

  private HousekeepingMetadata createEntityHousekeepingTable(String partitionName) {
    return HousekeepingMetadata.builder()
        .path(PATH)
        .databaseName(DATABASE_NAME)
        .tableName(TABLE_NAME)
        .partitionName(partitionName)
        .housekeepingStatus(SCHEDULED)
        .creationTimestamp(CREATION_TIMESTAMP)
        .modifiedTimestamp(CREATION_TIMESTAMP)
        .cleanupDelay(Duration.parse("P3D"))
        .cleanupAttempts(0)
        .lifecycleType(EXPIRED.toString())
        .build();
  }
}
