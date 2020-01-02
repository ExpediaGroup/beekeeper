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
package com.expediagroup.beekeeper.scheduler.apiary.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;

@ExtendWith(MockitoExtension.class)
public class PathSchedulerServiceTest {

  @Mock
  private HousekeepingPathRepository housekeepingPathRepository;

  @InjectMocks
  private PathSchedulerService pathSchedulerService;

  private final static String DATABASE = "some_db";
  private final static String TABLE = "some_table";
  private final static String PATH = "s3://some/path";
  private final static String CLEANUP_DELAY = "P3D";
  private final static PageRequest PAGER = PageRequest.of(0,1);

  private EntityHousekeepingPath createHousekeepingPath(String database, String table, String path, String cleanupDelay) {
    return new EntityHousekeepingPath.Builder()
        .creationTimestamp(LocalDateTime.now())
        .databaseName(database)
        .tableName(table)
        .path(path)
        .cleanupDelay(Duration.parse(cleanupDelay))
        .build();
  }

  @Test
  public void typicalNewExpirationHousekeeping() {
    EntityHousekeepingPath path = createHousekeepingPath(DATABASE, TABLE, PATH, CLEANUP_DELAY);
    when(housekeepingPathRepository.findExpiredRecordByDatabaseAndTableName(DATABASE, TABLE, PAGER))
        .thenReturn(new PageImpl<>(List.of()));
    pathSchedulerService.scheduleExpiration(path);
    verify(housekeepingPathRepository).save(path);
  }

  @Test
  public void typicalExistingExpirationHousekeeping() {
    EntityHousekeepingPath existingPath = createHousekeepingPath(DATABASE, TABLE, PATH, CLEANUP_DELAY);
    EntityHousekeepingPath newPath = createHousekeepingPath(DATABASE, TABLE, "s3://some/new/path", "P14D");

    when(housekeepingPathRepository.findExpiredRecordByDatabaseAndTableName(DATABASE, TABLE, PAGER))
        .thenReturn(new PageImpl<>(List.of(existingPath)));

    pathSchedulerService.scheduleExpiration(newPath);
    verify(housekeepingPathRepository).save(existingPath);
    assertThat(existingPath.getPath()).isEqualTo(newPath.getPath());
    assertThat(existingPath.getCleanupTimestamp()).isEqualTo(newPath.getCleanupTimestamp());
    assertThat(existingPath.getCleanupDelay()).isEqualTo(newPath.getCleanupDelay());
  }

  @Test
  public void expirationScheduleFails() {
    EntityHousekeepingPath path = createHousekeepingPath(DATABASE, TABLE, PATH, CLEANUP_DELAY);
    when(housekeepingPathRepository.findExpiredRecordByDatabaseAndTableName(DATABASE, TABLE, PAGER))
        .thenThrow(new RuntimeException());

    assertThatExceptionOfType(BeekeeperException.class)
        .isThrownBy(() -> pathSchedulerService.scheduleExpiration(path))
        .withMessage("Unable to schedule path 's3://some/path' for expiration");
    verify(housekeepingPathRepository).findExpiredRecordByDatabaseAndTableName(DATABASE, TABLE, PAGER);
    verify(housekeepingPathRepository, never()).save(path);
  }

  @Test
  public void typicalScheduleForHousekeeping() {
    EntityHousekeepingPath path = createHousekeepingPath(DATABASE, TABLE, PATH, CLEANUP_DELAY);
    pathSchedulerService.scheduleForHousekeeping(path);
    verify(housekeepingPathRepository).save(path);
  }

  @Test
  public void scheduleFails() {
    EntityHousekeepingPath path = createHousekeepingPath(DATABASE, TABLE, PATH, CLEANUP_DELAY);
    when(housekeepingPathRepository.save(path)).thenThrow(new RuntimeException());
    assertThatExceptionOfType(BeekeeperException.class)
        .isThrownBy(() -> pathSchedulerService.scheduleForHousekeeping(path))
        .withMessage("Unable to schedule path 's3://some/path' for deletion");
    verify(housekeepingPathRepository).save(path);
  }
}
