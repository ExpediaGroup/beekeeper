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
package com.expediagroup.beekeeper.metadata.cleanup.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DELETED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.FAILED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.FAILED_TO_DELETE;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SKIPPED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import com.expediagroup.beekeeper.cleanup.aws.S3PathCleaner;
import com.expediagroup.beekeeper.cleanup.hive.HiveClient;
import com.expediagroup.beekeeper.cleanup.hive.HiveClientFactory;
import com.expediagroup.beekeeper.cleanup.hive.HiveMetadataCleaner;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;
import com.expediagroup.beekeeper.core.service.BeekeeperHistoryService;

@ExtendWith(MockitoExtension.class)
public class ExpiredMetadataHandlerTest {

  private @Mock HiveClientFactory hiveClientFactory;
  private @Mock HiveClient hiveClient;
  private @Mock HousekeepingMetadataRepository housekeepingMetadataRepository;
  private @Mock HiveMetadataCleaner hiveMetadataCleaner;
  private @Mock S3PathCleaner s3PathCleaner;
  private @Mock HousekeepingMetadata housekeepingMetadata;
  private @Mock BeekeeperHistoryService beekeeperHistoryService;

  private static final LifecycleEventType lifecycleEventType = EXPIRED;
  private static final String DATABASE = "database";
  private static final String TABLE_NAME = "tableName";
  private static final String VALID_TABLE_PATH = "s3://bucket/table";
  private static final String VALID_PARTITION_PATH = "s3://bucket/table/partition";
  private static final String INVALID_PATH = "s3://bucket";
  private static final String PARTITION_NAME = "event_date=2020-01-01/event_hour=0/event_type=A";
  private static final LocalDateTime CLEANUP_INSTANCE = LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);

  private ExpiredMetadataHandler expiredMetadataHandler;

  @BeforeEach
  public void init() {
    expiredMetadataHandler = new ExpiredMetadataHandler(hiveClientFactory, housekeepingMetadataRepository,
        hiveMetadataCleaner, s3PathCleaner, beekeeperHistoryService);
  }

  @Test
  public void verifyMetadataCleaner() {
    assertThat(hiveMetadataCleaner).isInstanceOf(HiveMetadataCleaner.class);
  }

  @Test
  public void verifyPathCleaner() {
    assertThat(s3PathCleaner).isInstanceOf(S3PathCleaner.class);
  }

  @Test
  public void verifyLifecycle() {
    assertThat(lifecycleEventType).isEqualTo(EXPIRED);
  }

  @Test
  public void verifyHousekeepingMetadataFetch() {
    LocalDateTime now = LocalDateTime.now();
    Pageable emptyPageable = PageRequest.of(0, 1);
    expiredMetadataHandler.findRecordsToClean(now, emptyPageable);
    verify(housekeepingMetadataRepository).findRecordsForCleanupByModifiedTimestamp(now, emptyPageable);
  }

  @Test
  public void typicalRunDroppingTable() {
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(housekeepingMetadata.getPartitionName()).thenReturn(null);
    when(hiveClient.getTableProperties(DATABASE, TABLE_NAME))
        .thenReturn(Collections.singletonMap("beekeeper.expired.data.table.deletion.enabled", "true"));
    when(housekeepingMetadata.getPath()).thenReturn(VALID_TABLE_PATH);
    when(housekeepingMetadata.getCleanupAttempts()).thenReturn(0);
    when(
        housekeepingMetadataRepository.countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE,
            TABLE_NAME))
        .thenReturn(Long.valueOf(0));
    when(hiveMetadataCleaner.tableExists(hiveClient, DATABASE, TABLE_NAME)).thenReturn(true);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);
    verify(hiveMetadataCleaner).dropTable(housekeepingMetadata, hiveClient);
    verify(s3PathCleaner).cleanupPath(housekeepingMetadata);
    verify(hiveMetadataCleaner, never()).dropPartition(housekeepingMetadata, hiveClient);
    verify(housekeepingMetadata).setCleanupAttempts(1);
    verify(housekeepingMetadata).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository).save(housekeepingMetadata);
    verify(beekeeperHistoryService).saveHistory(any(), eq(DELETED));
  }

  @Test
  public void catchesBeekeeperIcebergException() {
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(housekeepingMetadata.getPath()).thenReturn(VALID_TABLE_PATH);
    when(housekeepingMetadata.getCleanupAttempts()).thenReturn(0);
    doThrow(new com.expediagroup.beekeeper.core.error.BeekeeperIcebergException("Iceberg table"))
        .when(hiveClient).getTableProperties(DATABASE, TABLE_NAME);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);

    verify(housekeepingMetadata).setCleanupAttempts(1);
    verify(housekeepingMetadata).setHousekeepingStatus(SKIPPED);
    verify(housekeepingMetadataRepository).save(housekeepingMetadata);
    verify(beekeeperHistoryService, never()).saveHistory(any(), any());
  }

  @Test
  public void typicalDroppingTable_DryRun() {
    boolean dryRunEnabled = true;
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(housekeepingMetadata.getPartitionName()).thenReturn(null);
    when(hiveClient.getTableProperties(DATABASE, TABLE_NAME))
        .thenReturn(Collections.singletonMap("beekeeper.expired.data.table.deletion.enabled", "true"));
    when(housekeepingMetadata.getPath()).thenReturn(VALID_TABLE_PATH);
    when(hiveMetadataCleaner.tableExists(hiveClient, DATABASE, TABLE_NAME)).thenReturn(true);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, dryRunEnabled);
    verify(hiveMetadataCleaner).dropTable(housekeepingMetadata, hiveClient);
    verify(s3PathCleaner).cleanupPath(housekeepingMetadata);
    verify(hiveMetadataCleaner, never()).dropPartition(housekeepingMetadata, hiveClient);
    verify(housekeepingMetadata, never()).setCleanupAttempts(1);
    verify(housekeepingMetadata, never()).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository, never()).save(housekeepingMetadata);
    verify(beekeeperHistoryService, never()).saveHistory(any(), any());
  }

  @Test
  public void typicalRunDroppingPartition() {
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(housekeepingMetadata.getPartitionName()).thenReturn(PARTITION_NAME);
    when(housekeepingMetadata.getPath()).thenReturn(VALID_PARTITION_PATH);
    when(housekeepingMetadata.getCleanupAttempts()).thenReturn(0);

    when(hiveMetadataCleaner.dropPartition(Mockito.any(), Mockito.any())).thenReturn(true);
    when(hiveMetadataCleaner.tableExists(hiveClient, DATABASE, TABLE_NAME)).thenReturn(true);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);
    verify(s3PathCleaner).cleanupPath(housekeepingMetadata);
    verify(hiveMetadataCleaner, never()).dropTable(housekeepingMetadata, hiveClient);
    verify(housekeepingMetadata).setCleanupAttempts(1);
    verify(housekeepingMetadata).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository).save(housekeepingMetadata);
    verify(beekeeperHistoryService).saveHistory(any(), eq(DELETED));
  }

  @Test
  public void dontDropTableWithInvalidPath() {
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(housekeepingMetadata.getPartitionName()).thenReturn(null);
    when(housekeepingMetadata.getPath()).thenReturn(INVALID_PATH);
    when(housekeepingMetadataRepository.countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE,
        TABLE_NAME))
        .thenReturn(Long.valueOf(0));

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);
    verify(hiveMetadataCleaner, never()).dropTable(housekeepingMetadata, hiveClient);
    verify(s3PathCleaner, never()).cleanupPath(housekeepingMetadata);
    verify(hiveMetadataCleaner, never()).dropPartition(housekeepingMetadata, hiveClient);
    verify(housekeepingMetadata, never()).setCleanupAttempts(1);
    verify(housekeepingMetadata).setHousekeepingStatus(SKIPPED);
    verify(housekeepingMetadataRepository).save(housekeepingMetadata);
    verify(beekeeperHistoryService).saveHistory(any(), eq(SKIPPED));
  }

  @Test
  public void dontDropTableWithInvalidPath_DryRun() {
    boolean dryRunEnabled = true;
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(housekeepingMetadata.getPartitionName()).thenReturn(null);
    when(housekeepingMetadata.getPath()).thenReturn(INVALID_PATH);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, dryRunEnabled);
    verify(hiveMetadataCleaner, never()).dropTable(housekeepingMetadata, hiveClient);
    verify(s3PathCleaner, never()).cleanupPath(housekeepingMetadata);
    verify(hiveMetadataCleaner, never()).dropPartition(housekeepingMetadata, hiveClient);
    verify(housekeepingMetadata, never()).setCleanupAttempts(1);
    verify(housekeepingMetadata, never()).setHousekeepingStatus(SKIPPED);
    verify(housekeepingMetadataRepository, never()).save(housekeepingMetadata);
    verify(beekeeperHistoryService, never()).saveHistory(any(), any());
  }

  @Test
  public void dontDropTableWithExistingPartition() {
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(housekeepingMetadata.getPartitionName()).thenReturn(null);
    when(housekeepingMetadataRepository.countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE,
        TABLE_NAME))
        .thenReturn(Long.valueOf(1));

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);
    verify(hiveMetadataCleaner, never()).dropTable(housekeepingMetadata, hiveClient);
    verify(s3PathCleaner, never()).cleanupPath(housekeepingMetadata);
    verify(hiveMetadataCleaner, never()).dropPartition(housekeepingMetadata, hiveClient);
    verify(housekeepingMetadata, never()).setCleanupAttempts(1);
    verify(housekeepingMetadata, never()).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository, never()).save(housekeepingMetadata);
  }

  @Test
  public void dontDropTableOrPathWhenTableDoesntExist() {
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(housekeepingMetadata.getPartitionName()).thenReturn(null);
    when(housekeepingMetadata.getPath()).thenReturn(VALID_TABLE_PATH);
    when(hiveClient.getTableProperties(DATABASE, TABLE_NAME))
        .thenReturn(Collections.singletonMap("beekeeper.expired.data.table.deletion.enabled", "true"));
    when(housekeepingMetadata.getCleanupAttempts()).thenReturn(0);
    when(housekeepingMetadataRepository.countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE, TABLE_NAME))
        .thenReturn(Long.valueOf(0));
    when(hiveMetadataCleaner.tableExists(hiveClient, DATABASE, TABLE_NAME)).thenReturn(false);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);
    verify(hiveMetadataCleaner, never()).dropTable(housekeepingMetadata, hiveClient);
    verify(s3PathCleaner, never()).cleanupPath(housekeepingMetadata);
    verify(hiveMetadataCleaner, never()).dropPartition(housekeepingMetadata, hiveClient);
    verify(housekeepingMetadata).setCleanupAttempts(1);
    verify(housekeepingMetadata).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository).save(housekeepingMetadata);
    verify(beekeeperHistoryService).saveHistory(any(), eq(DELETED));
  }

  @Test
  public void dontDropPartitionWithInvalidPartitionPath() {
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getPartitionName()).thenReturn(PARTITION_NAME);
    when(housekeepingMetadata.getPath()).thenReturn(INVALID_PATH);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);
    verify(hiveMetadataCleaner, never()).dropPartition(housekeepingMetadata, hiveClient);
    verify(s3PathCleaner, never()).cleanupPath(housekeepingMetadata);
    verify(hiveMetadataCleaner, never()).dropTable(housekeepingMetadata, hiveClient);
    verify(housekeepingMetadata, never()).setCleanupAttempts(1);
    verify(housekeepingMetadata).setHousekeepingStatus(SKIPPED);
    verify(housekeepingMetadataRepository).save(housekeepingMetadata);
    verify(beekeeperHistoryService).saveHistory(any(), eq(SKIPPED));
  }

  @Test
  public void dontDropPartitionWhenTableDoesntExist() {
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(housekeepingMetadata.getCleanupAttempts()).thenReturn(0);
    when(housekeepingMetadata.getPartitionName()).thenReturn(PARTITION_NAME);
    when(housekeepingMetadata.getPath()).thenReturn(VALID_PARTITION_PATH);
    when(hiveMetadataCleaner.tableExists(hiveClient, DATABASE, TABLE_NAME)).thenReturn(false);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);
    verify(hiveMetadataCleaner, never()).dropPartition(housekeepingMetadata, hiveClient);
    verify(s3PathCleaner, never()).cleanupPath(housekeepingMetadata);
    verify(hiveMetadataCleaner, never()).dropTable(housekeepingMetadata, hiveClient);
    verify(housekeepingMetadata).setCleanupAttempts(1);
    verify(housekeepingMetadata).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository).save(housekeepingMetadata);
    verify(beekeeperHistoryService).saveHistory(any(), eq(DELETED));
  }

  @Test
  public void dontDropPathWhenPartitionDoesntExist() {
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(housekeepingMetadata.getPartitionName()).thenReturn(PARTITION_NAME);
    when(housekeepingMetadata.getPath()).thenReturn(VALID_PARTITION_PATH);
    when(hiveMetadataCleaner.dropPartition(Mockito.any(), Mockito.any())).thenReturn(false);
    when(hiveMetadataCleaner.tableExists(hiveClient, DATABASE, TABLE_NAME)).thenReturn(true);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);
    verify(s3PathCleaner, never()).cleanupPath(housekeepingMetadata);
    verify(hiveMetadataCleaner, never()).dropTable(housekeepingMetadata, hiveClient);
    verify(housekeepingMetadata).setCleanupAttempts(1);
    verify(housekeepingMetadata).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository).save(housekeepingMetadata);
    verify(beekeeperHistoryService).saveHistory(any(), eq(DELETED));
  }

  @Test
  public void expectedTableDropFailure() {
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(housekeepingMetadata.getPartitionName()).thenReturn(null);
    when(hiveClient.getTableProperties(DATABASE, TABLE_NAME))
        .thenReturn(Collections.singletonMap("beekeeper.expired.data.table.deletion.enabled", "true"));
    when(housekeepingMetadata.getPath()).thenReturn(VALID_TABLE_PATH);
    when(housekeepingMetadata.getCleanupAttempts()).thenReturn(0);
    when(
        housekeepingMetadataRepository.countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE, TABLE_NAME))
        .thenReturn(Long.valueOf(0));
    when(hiveMetadataCleaner.tableExists(hiveClient, DATABASE, TABLE_NAME)).thenReturn(true);
    doThrow(RuntimeException.class).when(hiveMetadataCleaner).dropTable(housekeepingMetadata, hiveClient);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);
    verify(housekeepingMetadata).setCleanupAttempts(1);
    verify(housekeepingMetadata).setHousekeepingStatus(FAILED);
    verify(housekeepingMetadataRepository).save(housekeepingMetadata);
    verify(beekeeperHistoryService).saveHistory(any(), eq(FAILED_TO_DELETE));
  }

  @Test
  public void expectedPathDeleteFailure() {
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(housekeepingMetadata.getPartitionName()).thenReturn(null);
    when(hiveClient.getTableProperties(DATABASE, TABLE_NAME))
        .thenReturn(Collections.singletonMap("beekeeper.expired.data.table.deletion.enabled", "true"));
    when(housekeepingMetadata.getPath()).thenReturn(VALID_TABLE_PATH);
    when(housekeepingMetadata.getCleanupAttempts()).thenReturn(0);
    when(hiveMetadataCleaner.tableExists(hiveClient, DATABASE, TABLE_NAME)).thenReturn(true);
    doThrow(RuntimeException.class).when(s3PathCleaner).cleanupPath(housekeepingMetadata);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);
    verify(housekeepingMetadata).setCleanupAttempts(1);
    verify(housekeepingMetadata).setHousekeepingStatus(FAILED);
    verify(housekeepingMetadataRepository).save(housekeepingMetadata);
    verify(beekeeperHistoryService).saveHistory(any(), eq(FAILED_TO_DELETE));
  }

  @Test
  public void expectedPartitionDropFailure() {
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(housekeepingMetadata.getPartitionName()).thenReturn(PARTITION_NAME);
    when(housekeepingMetadata.getPath()).thenReturn(VALID_PARTITION_PATH);
    when(housekeepingMetadata.getCleanupAttempts()).thenReturn(0);
    when(hiveMetadataCleaner.tableExists(hiveClient, DATABASE, TABLE_NAME)).thenReturn(true);
    doThrow(RuntimeException.class).when(hiveMetadataCleaner).dropPartition(housekeepingMetadata, hiveClient);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);
    verify(housekeepingMetadata).setCleanupAttempts(1);
    verify(housekeepingMetadata).setHousekeepingStatus(FAILED);
    verify(housekeepingMetadataRepository).save(housekeepingMetadata);
    verify(beekeeperHistoryService).saveHistory(any(), eq(FAILED_TO_DELETE));
  }

  @Test
  public void tableDeletionPropertyFalse_shouldNotDropTable() {
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(housekeepingMetadata.getPath()).thenReturn(VALID_TABLE_PATH);
    when(housekeepingMetadata.getPartitionName()).thenReturn(null);
    when(hiveClient.getTableProperties(DATABASE, TABLE_NAME))
        .thenReturn(Collections.singletonMap("beekeeper.expired.data.table.deletion.enabled", "false"));
    when(housekeepingMetadataRepository
        .countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE, TABLE_NAME))
        .thenReturn(0L);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);
    verify(hiveMetadataCleaner, never()).dropTable(housekeepingMetadata, hiveClient);
    verify(housekeepingMetadata).setHousekeepingStatus(SKIPPED);
    verify(housekeepingMetadata).setCleanupAttempts(1);
    verify(housekeepingMetadataRepository).save(housekeepingMetadata);
    verify(beekeeperHistoryService).saveHistory(any(), eq(SKIPPED));
  }

  @Test
  public void tableDeletionPropertyNotPresent_shouldDefaultToFalse() {
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(housekeepingMetadata.getPartitionName()).thenReturn(null);
    when(hiveClient.getTableProperties(DATABASE, TABLE_NAME)).thenReturn(Collections.emptyMap());
    when(housekeepingMetadata.getPath()).thenReturn(VALID_TABLE_PATH);
    when(housekeepingMetadata.getCleanupAttempts()).thenReturn(0);
    when(housekeepingMetadataRepository
        .countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE, TABLE_NAME))
        .thenReturn(0L);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);
    verify(hiveMetadataCleaner, never()).dropTable(housekeepingMetadata, hiveClient);
    verify(housekeepingMetadata).setCleanupAttempts(1);
    verify(housekeepingMetadata).setHousekeepingStatus(SKIPPED);
    verify(housekeepingMetadataRepository).save(housekeepingMetadata);
    verify(beekeeperHistoryService).saveHistory(any(), eq(SKIPPED));
  }
}
