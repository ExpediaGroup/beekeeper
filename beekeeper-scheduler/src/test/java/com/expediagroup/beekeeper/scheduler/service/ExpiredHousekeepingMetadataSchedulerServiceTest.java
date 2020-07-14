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
package com.expediagroup.beekeeper.scheduler.service;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
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

  @Mock
  private HousekeepingMetadataRepository housekeepingMetadataRepository;

  @InjectMocks
  private ExpiredHousekeepingMetadataSchedulerService expiredHousekeepingMetadataSchedulerService;

  @Test
  public void typicalCreateScheduleForHousekeeping() {
    HousekeepingMetadata metadata = createEntityHousekeepingTable();

    when(housekeepingMetadataRepository.findRecordForCleanupByDatabaseAndTable(DATABASE_NAME, TABLE_NAME,
        PARTITION_NAME)).thenReturn(Optional.empty());

    expiredHousekeepingMetadataSchedulerService.scheduleForHousekeeping(metadata);

    verify(housekeepingMetadataRepository).save(metadata);
  }

  @Test
  public void typicalUpdateScheduleForHousekeepingWhenChangingCleanupDelay() {
    HousekeepingMetadata existingTable = spy(createEntityHousekeepingTable());
    HousekeepingMetadata metadata = createEntityHousekeepingTable();
    metadata.setCleanupDelay(Duration.parse("P30D"));

    when(housekeepingMetadataRepository.findRecordForCleanupByDatabaseAndTable(DATABASE_NAME, TABLE_NAME,
        PARTITION_NAME)).thenReturn(Optional.of(existingTable));

    expiredHousekeepingMetadataSchedulerService.scheduleForHousekeeping(metadata);

    verify(housekeepingMetadataRepository).findRecordForCleanupByDatabaseAndTable(DATABASE_NAME, TABLE_NAME,
        PARTITION_NAME);
    verify(existingTable).setHousekeepingStatus(metadata.getHousekeepingStatus());
    verify(existingTable).setClientId(metadata.getClientId());
    verify(existingTable).setCleanupDelay(metadata.getCleanupDelay());
    verifyNoMoreInteractions(existingTable);
    verify(housekeepingMetadataRepository).save(existingTable);
  }

  @Test
  public void verifyLifecycleType() {
    assertThat(expiredHousekeepingMetadataSchedulerService.getLifecycleEventType())
        .isEqualTo(EXPIRED);
  }

  @Test
  public void scheduleFails() {
    HousekeepingMetadata metadata = createEntityHousekeepingTable();

    when(housekeepingMetadataRepository.save(metadata)).thenThrow(new RuntimeException());

    assertThatExceptionOfType(BeekeeperException.class)
        .isThrownBy(() -> expiredHousekeepingMetadataSchedulerService.scheduleForHousekeeping(metadata))
        .withMessage(format("Unable to schedule %s", metadata));
    verify(housekeepingMetadataRepository).save(metadata);
  }

  private HousekeepingMetadata createEntityHousekeepingTable() {
    LocalDateTime creationTimestamp = LocalDateTime.now(ZoneId.of("UTC"));
    return new HousekeepingMetadata.Builder()
        .path(PATH)
        .databaseName(DATABASE_NAME)
        .tableName(TABLE_NAME)
        .partitionName(PARTITION_NAME)
        .housekeepingStatus(SCHEDULED)
        .creationTimestamp(creationTimestamp)
        .modifiedTimestamp(creationTimestamp)
        .cleanupDelay(Duration.parse("P3D"))
        .cleanupAttempts(0)
        .lifecycleType(EXPIRED.toString())
        .build();
  }
}
