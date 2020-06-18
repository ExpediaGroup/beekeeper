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

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DELETED;
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
import com.expediagroup.beekeeper.core.model.EntityHousekeepingTable;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.core.repository.HousekeepingTableRepository;

@ExtendWith(MockitoExtension.class)
public class ExpiredTableSchedulerServiceTest {

  private static String DATABASE_NAME = "database";
  private static String TABLE_NAME = "table";

  @Mock
  private HousekeepingTableRepository housekeepingTableRepository;

  @InjectMocks
  private ExpiredTableSchedulerService expiredTableSchedulerService;

  @Test
  public void typicalCreateScheduleForHousekeeping() {
    EntityHousekeepingTable table = createEntityHousekeepingTable();

    when(housekeepingTableRepository.findRecordForCleanupByDatabaseAndTable(DATABASE_NAME, TABLE_NAME)).thenReturn(
        Optional.empty());

    expiredTableSchedulerService.scheduleForHousekeeping(table);

    verify(housekeepingTableRepository).save(table);
  }

  @Test
  public void typicalUpdateScheduleForHousekeepingWhenTableDeletedManually() {
    EntityHousekeepingTable existingTable = spy(createEntityHousekeepingTable());
    EntityHousekeepingTable table = createEntityHousekeepingTable();
    table.setHousekeepingStatus(DELETED);

    when(housekeepingTableRepository.findRecordForCleanupByDatabaseAndTable(DATABASE_NAME, TABLE_NAME)).thenReturn(
        Optional.of(existingTable));

    expiredTableSchedulerService.scheduleForHousekeeping(table);

    verify(housekeepingTableRepository).findRecordForCleanupByDatabaseAndTable(DATABASE_NAME, TABLE_NAME);
    verify(existingTable).setHousekeepingStatus(table.getHousekeepingStatus());
    verify(existingTable).setClientId(table.getClientId());
    verifyNoMoreInteractions(existingTable);
    verify(housekeepingTableRepository).save(existingTable);
  }

  @Test
  public void typicalUpdateScheduleForHousekeepingWhenChangingCleanupDelay() {
    EntityHousekeepingTable existingTable = spy(createEntityHousekeepingTable());
    EntityHousekeepingTable table = createEntityHousekeepingTable();
    table.setCleanupDelay(Duration.parse("P30D"));

    when(housekeepingTableRepository.findRecordForCleanupByDatabaseAndTable(DATABASE_NAME, TABLE_NAME)).thenReturn(
        Optional.of(existingTable));

    expiredTableSchedulerService.scheduleForHousekeeping(table);

    verify(housekeepingTableRepository).findRecordForCleanupByDatabaseAndTable(DATABASE_NAME, TABLE_NAME);
    verify(existingTable).setHousekeepingStatus(table.getHousekeepingStatus());
    verify(existingTable).setClientId(table.getClientId());
    verify(existingTable).setCleanupDelay(table.getCleanupDelay());
    verifyNoMoreInteractions(existingTable);
    verify(housekeepingTableRepository).save(existingTable);
  }

  @Test
  public void verifyLifecycleType() {
    assertThat(expiredTableSchedulerService.getLifecycleEventType())
        .isEqualTo(EXPIRED);
  }

  @Test
  public void scheduleFails() {
    EntityHousekeepingTable table = createEntityHousekeepingTable();

    when(housekeepingTableRepository.save(table)).thenThrow(new RuntimeException());

    assertThatExceptionOfType(BeekeeperException.class)
        .isThrownBy(() -> expiredTableSchedulerService.scheduleForHousekeeping(table))
        .withMessage(
            format("Unable to schedule table '%s.%s' for deletion", table.getDatabaseName(), table.getTableName()));
    verify(housekeepingTableRepository).save(table);
  }

  private EntityHousekeepingTable createEntityHousekeepingTable() {
    LocalDateTime creationTimestamp = LocalDateTime.now(ZoneId.of("UTC"));
    return new EntityHousekeepingTable.Builder()
        .databaseName(DATABASE_NAME)
        .tableName(TABLE_NAME)
        .housekeepingStatus(HousekeepingStatus.SCHEDULED)
        .creationTimestamp(creationTimestamp)
        .modifiedTimestamp(creationTimestamp)
        .cleanupDelay(Duration.parse("P3D"))
        .cleanupAttempts(0)
        .lifecycleType(EXPIRED.toString())
        .build();
  }
}
