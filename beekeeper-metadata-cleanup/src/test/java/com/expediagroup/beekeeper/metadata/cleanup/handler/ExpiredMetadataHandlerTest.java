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
package com.expediagroup.beekeeper.metadata.cleanup.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DELETED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.FAILED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import com.expediagroup.beekeeper.cleanup.aws.S3PathCleaner;
import com.expediagroup.beekeeper.cleanup.hive.HiveMetadataCleaner;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;
import com.expediagroup.beekeeper.metadata.cleanup.cleaner.ExpiredMetadataCleanup;

@ExtendWith(MockitoExtension.class)
public class ExpiredMetadataHandlerTest {

  private @Mock HousekeepingMetadataRepository housekeepingMetadataRepository;
  private @Mock HiveMetadataCleaner hiveMetadataCleaner;
  private @Mock S3PathCleaner s3PathCleaner;
  private LifecycleEventType lifecycleEventType = EXPIRED;

  private @Mock HousekeepingMetadata mockMetadata;
  private @Mock Pageable mockPageable;
  private @Mock Pageable nextPageable;
  private @Mock PageImpl<HousekeepingMetadata> mockPage;

  private MetadataHandler handler;

  private static final String DATABASE = "database";
  private static final String TABLE_NAME = "tableName";
  private static final String PARTITION_NAME = "event_date=2020-01-01/event_hour=0/event_type=A";
  private static final LocalDateTime CLEANUP_INSTANCE = LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);

  @BeforeEach
  public void init() {
    ExpiredMetadataCleanup expiredMetadataCleanup = new ExpiredMetadataCleanup(housekeepingMetadataRepository, hiveMetadataCleaner, s3PathCleaner);
    handler = new MetadataHandler(expiredMetadataCleanup);
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
    handler.findRecordsToClean(now, emptyPageable);
    verify(housekeepingMetadataRepository).findRecordsForCleanupByModifiedTimestamp(now, emptyPageable);
  }

//  @Test
//  public void verifyHousekeepingMetadataCountRecordFetch() {
//    String databaseName = "database";
//    String tableName = "table_name";
//    handler.countPartitionsForDatabaseAndTable(CLEANUP_INSTANCE, databaseName, "table_name", false);
//    verify(housekeepingMetadataRepository).countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(databaseName,
//        tableName);
//  }
//
//  @Test
//  public void verifyHousekeepingMetadataDryRunCountRecordFetch() {
//    String databaseName = "database";
//    String tableName = "table_name";
//    handler.countPartitionsForDatabaseAndTable(CLEANUP_INSTANCE, databaseName, "table_name", true);
//    verify(housekeepingMetadataRepository).countRecordsForDryRunWherePartitionIsNotNullOrExpired(CLEANUP_INSTANCE,
//        databaseName,
//        tableName);
//  }

  @Test
  public void typicalRunDroppingTable() {
    when(mockPage.getContent()).thenReturn(List.of(mockMetadata));
    when(mockMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(mockMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(mockMetadata.getPartitionName()).thenReturn(null);
    when(mockMetadata.getCleanupAttempts()).thenReturn(0);
    when(
        housekeepingMetadataRepository.countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE,
            TABLE_NAME))
        .thenReturn(Long.valueOf(0));
    when(hiveMetadataCleaner.tableExists(DATABASE, TABLE_NAME)).thenReturn(true);

    handler.processPage(mockPageable, CLEANUP_INSTANCE, mockPage, false);
    verify(hiveMetadataCleaner).dropTable(mockMetadata);
    verify(s3PathCleaner).cleanupPath(mockMetadata);
    verify(hiveMetadataCleaner, never()).dropPartition(mockMetadata);
    verify(mockMetadata).setCleanupAttempts(1);
    verify(mockMetadata).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository).save(mockMetadata);
    verify(mockPageable, never()).next();
  }

  @Test
  public void typicalRunDroppingPartition() {
    when(mockPage.getContent()).thenReturn(List.of(mockMetadata));
    when(mockMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(mockMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(mockMetadata.getPartitionName()).thenReturn(PARTITION_NAME);
    when(hiveMetadataCleaner.dropPartition(Mockito.any())).thenReturn(true);
    when(hiveMetadataCleaner.tableExists(DATABASE, TABLE_NAME)).thenReturn(true);

    handler.processPage(mockPageable, CLEANUP_INSTANCE, mockPage, false);
    verify(hiveMetadataCleaner).dropPartition(mockMetadata);
    verify(s3PathCleaner).cleanupPath(mockMetadata);
    verify(hiveMetadataCleaner, never()).dropTable(mockMetadata);
    verify(mockMetadata).setCleanupAttempts(1);
    verify(mockMetadata).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository).save(mockMetadata);
    verify(mockPageable, never()).next();
  }

  @Test
  public void tableWithExistingPartitionNotDeleted() {
    when(mockPage.getContent()).thenReturn(List.of(mockMetadata));
    when(mockMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(mockMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(mockMetadata.getPartitionName()).thenReturn(null);
    when(housekeepingMetadataRepository.countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE,
        TABLE_NAME))
        .thenReturn(Long.valueOf(1));

    handler.processPage(mockPageable, CLEANUP_INSTANCE, mockPage, false);
    verify(hiveMetadataCleaner, never()).dropTable(mockMetadata);
    verify(s3PathCleaner, never()).cleanupPath(mockMetadata);
    verify(hiveMetadataCleaner, never()).dropPartition(mockMetadata);
    verify(mockMetadata, never()).setCleanupAttempts(1);
    verify(mockMetadata, never()).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository, never()).save(mockMetadata);
    verify(mockPageable, never()).next();
  }

  @Test
  public void typicalDryRunDroppingTable() {
    when(mockPage.getContent()).thenReturn(List.of(mockMetadata));
    when(mockMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(mockMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(mockMetadata.getPartitionName()).thenReturn(null);
    when(mockPageable.next()).thenReturn(nextPageable);
    when(
        housekeepingMetadataRepository.countRecordsForDryRunWherePartitionIsNotNullOrExpired(CLEANUP_INSTANCE, DATABASE,
            TABLE_NAME))
        .thenReturn(Long.valueOf(0));
    when(hiveMetadataCleaner.tableExists(DATABASE, TABLE_NAME)).thenReturn(true);

    handler.processPage(mockPageable, CLEANUP_INSTANCE, mockPage, true);
    verify(hiveMetadataCleaner).dropTable(mockMetadata);
    verify(s3PathCleaner).cleanupPath(mockMetadata);
    verify(hiveMetadataCleaner, never()).dropPartition(mockMetadata);
    verifyNoMoreInteractions(mockMetadata);
    verify(mockMetadata, never()).setCleanupAttempts(1);
    verify(mockMetadata, never()).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository, never()).save(mockMetadata);
    verify(mockPageable).next();
  }

  @Test
  public void typicalDryRunDroppingPartition() {
    when(mockPage.getContent()).thenReturn(List.of(mockMetadata));
    when(mockMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(mockMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(mockPageable.next()).thenReturn(nextPageable);
    when(mockMetadata.getPartitionName()).thenReturn(PARTITION_NAME);
    when(hiveMetadataCleaner.tableExists(DATABASE, TABLE_NAME)).thenReturn(true);
    when(hiveMetadataCleaner.dropPartition(Mockito.any())).thenReturn(true);

    handler.processPage(mockPageable, CLEANUP_INSTANCE, mockPage, true);
    verify(hiveMetadataCleaner).dropPartition(mockMetadata);
    verify(s3PathCleaner).cleanupPath(mockMetadata);
    verify(hiveMetadataCleaner, never()).dropTable(mockMetadata);
    verifyNoMoreInteractions(mockMetadata);
    verify(mockMetadata, never()).setCleanupAttempts(1);
    verify(mockMetadata, never()).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository, never()).save(mockMetadata);
    verify(mockPageable).next();
  }

  @Test
  public void dontDropTableOrPathWhenTableDoesntExist() {
    when(mockPage.getContent()).thenReturn(List.of(mockMetadata));
    when(mockMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(mockMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(mockMetadata.getPartitionName()).thenReturn(null);
    when(mockMetadata.getCleanupAttempts()).thenReturn(0);
    when(
        housekeepingMetadataRepository.countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE,
            TABLE_NAME))
        .thenReturn(Long.valueOf(0));
    when(hiveMetadataCleaner.tableExists(DATABASE, TABLE_NAME)).thenReturn(false);

    handler.processPage(mockPageable, CLEANUP_INSTANCE, mockPage, false);
    verify(hiveMetadataCleaner, never()).dropTable(mockMetadata);
    verify(s3PathCleaner, never()).cleanupPath(mockMetadata);
    verify(hiveMetadataCleaner, never()).dropPartition(mockMetadata);
    verify(mockMetadata).setCleanupAttempts(1);
    verify(mockMetadata).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository).save(mockMetadata);
    verify(mockPageable, never()).next();
  }

  @Test
  public void dontDropPartitionWhenTableDoesntExist() {
    when(mockPage.getContent()).thenReturn(List.of(mockMetadata));
    when(mockMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(mockMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(mockMetadata.getCleanupAttempts()).thenReturn(0);
    when(mockMetadata.getPartitionName()).thenReturn(PARTITION_NAME);
    when(hiveMetadataCleaner.tableExists(DATABASE, TABLE_NAME)).thenReturn(false);

    handler.processPage(mockPageable, CLEANUP_INSTANCE, mockPage, false);
    verify(hiveMetadataCleaner, never()).dropPartition(mockMetadata);
    verify(s3PathCleaner, never()).cleanupPath(mockMetadata);
    verify(hiveMetadataCleaner, never()).dropTable(mockMetadata);
    verify(mockMetadata).setCleanupAttempts(1);
    verify(mockMetadata).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository).save(mockMetadata);
    verify(mockPageable, never()).next();
  }

  @Test
  public void dontDropPathWhenPartitionDoesntExist() {
    when(mockPage.getContent()).thenReturn(List.of(mockMetadata));
    when(mockMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(mockMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(mockMetadata.getPartitionName()).thenReturn(PARTITION_NAME);
    when(hiveMetadataCleaner.dropPartition(Mockito.any())).thenReturn(false);
    when(hiveMetadataCleaner.tableExists(DATABASE, TABLE_NAME)).thenReturn(true);

    handler.processPage(mockPageable, CLEANUP_INSTANCE, mockPage, false);
    verify(hiveMetadataCleaner).dropPartition(mockMetadata);
    verify(s3PathCleaner, never()).cleanupPath(mockMetadata);
    verify(hiveMetadataCleaner, never()).dropTable(mockMetadata);
    verify(mockMetadata).setCleanupAttempts(1);
    verify(mockMetadata).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository).save(mockMetadata);
    verify(mockPageable, never()).next();
  }

  @Test
  public void expectedTableDropFailure() {
    when(mockPage.getContent()).thenReturn(List.of(mockMetadata));
    when(mockMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(mockMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(mockMetadata.getPartitionName()).thenReturn(null);
    when(mockMetadata.getCleanupAttempts()).thenReturn(0);
    when(
        housekeepingMetadataRepository.countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE,
            TABLE_NAME))
        .thenReturn(Long.valueOf(0));
    when(hiveMetadataCleaner.tableExists(DATABASE, TABLE_NAME)).thenReturn(true);
    doThrow(RuntimeException.class).when(hiveMetadataCleaner).dropTable(mockMetadata);

    handler.processPage(mockPageable, CLEANUP_INSTANCE, mockPage, false);
    verify(mockMetadata).setCleanupAttempts(1);
    verify(mockMetadata).setHousekeepingStatus(FAILED);
    verify(housekeepingMetadataRepository).save(mockMetadata);
    verify(mockPageable, never()).next();
  }

  @Test
  public void expectedPathDeleteFailure() {
    when(mockPage.getContent()).thenReturn(List.of(mockMetadata));
    when(mockMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(mockMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(mockMetadata.getPartitionName()).thenReturn(null);
    when(mockMetadata.getCleanupAttempts()).thenReturn(0);
    when(hiveMetadataCleaner.tableExists(DATABASE, TABLE_NAME)).thenReturn(true);
    doThrow(RuntimeException.class).when(s3PathCleaner).cleanupPath(mockMetadata);

    handler.processPage(mockPageable, CLEANUP_INSTANCE, mockPage, false);
    verify(mockMetadata).setCleanupAttempts(1);
    verify(mockMetadata).setHousekeepingStatus(FAILED);
    verify(housekeepingMetadataRepository).save(mockMetadata);
    verify(mockPageable, never()).next();
  }

  @Test
  public void expectedPartitionDropFailure() {
    when(mockPage.getContent()).thenReturn(List.of(mockMetadata));
    when(mockMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(mockMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(mockMetadata.getPartitionName()).thenReturn(PARTITION_NAME);
    when(mockMetadata.getCleanupAttempts()).thenReturn(0);
    when(hiveMetadataCleaner.tableExists(DATABASE, TABLE_NAME)).thenReturn(true);
    doThrow(RuntimeException.class).when(hiveMetadataCleaner).dropPartition(mockMetadata);

    handler.processPage(mockPageable, CLEANUP_INSTANCE, mockPage, false);
    verify(mockMetadata).setCleanupAttempts(1);
    verify(mockMetadata).setHousekeepingStatus(FAILED);
    verify(housekeepingMetadataRepository).save(mockMetadata);
    verify(mockPageable, never()).next();
  }
}
