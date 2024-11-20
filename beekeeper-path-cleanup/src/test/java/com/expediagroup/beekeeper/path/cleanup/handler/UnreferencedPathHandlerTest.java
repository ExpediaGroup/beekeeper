/**
 * Copyright (C) 2019-2023 Expedia, Inc.
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
package com.expediagroup.beekeeper.path.cleanup.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.time.Duration;
import java.time.LocalDateTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import com.expediagroup.beekeeper.cleanup.aws.S3PathCleaner;
import com.expediagroup.beekeeper.core.checker.IcebergTableChecker;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.model.PeriodDuration;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;

@ExtendWith(MockitoExtension.class)
public class UnreferencedPathHandlerTest {

  @Mock
  private HousekeepingPathRepository housekeepingPathRepository;
  @Mock
  private S3PathCleaner s3PathCleaner;
  @Mock
  private IcebergTableChecker icebergTableChecker;
  private LifecycleEventType lifecycleEventType = UNREFERENCED;

  private UnreferencedPathHandler handler;

  @BeforeEach
  public void initTest() {
    handler = new UnreferencedPathHandler(housekeepingPathRepository, s3PathCleaner, icebergTableChecker);
  }

  @Test
  public void verifyPathCleaner() {
    assertThat(s3PathCleaner).isInstanceOf(S3PathCleaner.class);
  }

  @Test
  public void verifyLifecycle() {
    assertThat(lifecycleEventType).isEqualTo(UNREFERENCED);
  }

  @Test
  public void verifyHousekeepingPathFetch() {
    LocalDateTime now = LocalDateTime.now();
    Pageable emptyPageable = PageRequest.of(0, 1);
    handler.findRecordsToClean(now, emptyPageable);
    verify(housekeepingPathRepository).findRecordsForCleanup(now, emptyPageable);
  }

  @Test
  public void cleanupContent_WithNullDatabaseNameAndTableName_ShouldDelegateToGenericPathHandler() {
    HousekeepingPath pathWithNulls = createHousekeepingPath("s3://bucket/null_table", null, null);

    handler.cleanupContent(pathWithNulls);

    verify(s3PathCleaner).cleanupPath(pathWithNulls);
    verify(housekeepingPathRepository).save(pathWithNulls);
    // Assert that the status is set to DELETED by the superclass
    assertThat(pathWithNulls.getHousekeepingStatus()).isEqualTo(HousekeepingStatus.DELETED);
  }

  @Test
  public void cleanupContent_IcebergTable_ShouldSkipCleanup() {
    HousekeepingPath icebergPath = createHousekeepingPath("s3://bucket/iceberg_table", "database", "iceberg_table");

    when(icebergTableChecker.isIcebergTable("database", "iceberg_table")).thenReturn(true);

    handler.cleanupContent(icebergPath);
    // verify that pathCleaner is not called and cleanup is skipped for Iceberg tables
    verify(s3PathCleaner, never()).cleanupPath(any(HousekeepingPath.class));
    verify(housekeepingPathRepository).save(icebergPath);
    assertThat(icebergPath.getHousekeepingStatus()).isEqualTo(HousekeepingStatus.SKIPPED);
  }

  @Test
  public void cleanupContent_NonIcebergTable_ShouldProceedWithCleanup() {
    HousekeepingPath nonIcebergPath = createHousekeepingPath("s3://bucket/non_iceberg_table", "database", "non_iceberg_table");

    // Mock icebergTbleChcker to return false
    when(icebergTableChecker.isIcebergTable("database", "non_iceberg_table")).thenReturn(false);

    handler.cleanupContent(nonIcebergPath);

    verify(s3PathCleaner).cleanupPath(nonIcebergPath);
    verify(housekeepingPathRepository).save(nonIcebergPath);
    assertThat(nonIcebergPath.getHousekeepingStatus()).isEqualTo(HousekeepingStatus.DELETED);
  }

  @Test
  public void cleanupContent_IcebergTableCheckThrowsException_ShouldSetStatusToFailed() {
    HousekeepingPath errorPath = createHousekeepingPath("s3://bucket/error_table", "database", "error_table");

    // Mock the IcebergTableChecker to throw an exception
    when(icebergTableChecker.isIcebergTable("database", "error_table"))
        .thenThrow(new RuntimeException("Iceberg check failed"));

    handler.cleanupContent(errorPath);

    verify(s3PathCleaner, never()).cleanupPath(any(HousekeepingPath.class));
    verify(housekeepingPathRepository).save(errorPath);
    assertThat(errorPath.getHousekeepingStatus()).isEqualTo(HousekeepingStatus.FAILED);
    assertThat(errorPath.getCleanupAttempts()).isEqualTo(1);
  }

  @Test
  public void cleanupContent_IcebergTable_ShouldNotIncrementCleanupAttempts() {
    HousekeepingPath icebergPath = createHousekeepingPath("s3://bucket/iceberg_table", "database", "iceberg_table");

    when(icebergTableChecker.isIcebergTable("database", "iceberg_table")).thenReturn(true);

    handler.cleanupContent(icebergPath);

    assertThat(icebergPath.getCleanupAttempts()).isEqualTo(0);
  }
  
  @Test
  public void cleanupContent_NonIcebergTable_ShouldIncrementCleanupAttempts() {
    HousekeepingPath nonIcebergPath = createHousekeepingPath("s3://bucket/non_iceberg_table", "database", "non_iceberg_table");

    when(icebergTableChecker.isIcebergTable("database", "non_iceberg_table")).thenReturn(false);

    handler.cleanupContent(nonIcebergPath);

    assertThat(nonIcebergPath.getCleanupAttempts()).isEqualTo(1);
  }

  @Test
  public void cleanupContent_MultiplePaths_ShouldHandleEachAccordingly() {
    HousekeepingPath icebergPath = createHousekeepingPath("s3://bucket/iceberg_table", "database", "iceberg_table");
    HousekeepingPath nonIcebergPath = createHousekeepingPath("s3://bucket/non_iceberg_table", "database", "non_iceberg_table");

    when(icebergTableChecker.isIcebergTable("database", "iceberg_table")).thenReturn(true);
    when(icebergTableChecker.isIcebergTable("database", "non_iceberg_table")).thenReturn(false);

    handler.cleanupContent(icebergPath);
    handler.cleanupContent(nonIcebergPath);

    // Iceberg Path: cleanup skipped
    verify(s3PathCleaner, never()).cleanupPath(icebergPath);
    verify(housekeepingPathRepository).save(icebergPath);
    assertThat(icebergPath.getHousekeepingStatus()).isEqualTo(HousekeepingStatus.SKIPPED);
    assertThat(icebergPath.getCleanupAttempts()).isEqualTo(0);

    // Non-Iceberg Path: cleanup proceeded
    verify(s3PathCleaner).cleanupPath(nonIcebergPath);
    verify(housekeepingPathRepository).save(nonIcebergPath);
    assertThat(nonIcebergPath.getHousekeepingStatus()).isEqualTo(HousekeepingStatus.DELETED);
    assertThat(nonIcebergPath.getCleanupAttempts()).isEqualTo(1);
  }

  private HousekeepingPath createHousekeepingPath(String path, String databaseName, String tableName) {
    return HousekeepingPath.builder()
        .path(path)
        .databaseName(databaseName)
        .tableName(tableName)
        .housekeepingStatus(HousekeepingStatus.SCHEDULED)
        .creationTimestamp(LocalDateTime.now())
        .cleanupDelay(PeriodDuration.of(Duration.ofDays(3))) // Example: 3 days delay
        .cleanupAttempts(0)
        .lifecycleType(UNREFERENCED.toString())
        .build();
  }
}
