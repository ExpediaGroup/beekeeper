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

import static org.apache.commons.lang.math.NumberUtils.LONG_ZERO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DELETED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.FAILED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SKIPPED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

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

@ExtendWith(MockitoExtension.class)
public class ExpiredMetadataHandlerTest {

  private @Mock HiveClientFactory hiveClientFactory;
  private @Mock HiveClient hiveClient;
  private @Mock HousekeepingMetadataRepository housekeepingMetadataRepository;
  private @Mock HiveMetadataCleaner hiveMetadataCleaner;
  private @Mock S3PathCleaner s3PathCleaner;
  private @Mock HousekeepingMetadata housekeepingMetadata;

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
        hiveMetadataCleaner,
        s3PathCleaner);
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
    when(housekeepingMetadata.getCleanupAttempts()).thenReturn(0);
    when(
        housekeepingMetadataRepository.countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE,
            TABLE_NAME))
        .thenReturn(Long.valueOf(0));
    when(hiveMetadataCleaner.tableExists(hiveClient, DATABASE, TABLE_NAME)).thenReturn(false);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);
    verify(hiveMetadataCleaner, never()).dropTable(housekeepingMetadata, hiveClient);
    verify(s3PathCleaner, never()).cleanupPath(housekeepingMetadata);
    verify(hiveMetadataCleaner, never()).dropPartition(housekeepingMetadata, hiveClient);
    verify(housekeepingMetadata).setCleanupAttempts(1);
    verify(housekeepingMetadata).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository).save(housekeepingMetadata);
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
  }

  @Test
  public void expectedTableDropFailure() {
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(housekeepingMetadata.getPartitionName()).thenReturn(null);
    when(housekeepingMetadata.getPath()).thenReturn(VALID_TABLE_PATH);;
    when(housekeepingMetadata.getCleanupAttempts()).thenReturn(0);
    when(
        housekeepingMetadataRepository.countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE,
            TABLE_NAME))
        .thenReturn(Long.valueOf(0));
    when(hiveMetadataCleaner.tableExists(hiveClient, DATABASE, TABLE_NAME)).thenReturn(true);
    doThrow(RuntimeException.class).when(hiveMetadataCleaner).dropTable(housekeepingMetadata, hiveClient);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);
    verify(housekeepingMetadata).setCleanupAttempts(1);
    verify(housekeepingMetadata).setHousekeepingStatus(FAILED);
    verify(housekeepingMetadataRepository).save(housekeepingMetadata);
  }

  @Test
  public void expectedPathDeleteFailure() {
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(housekeepingMetadata.getPartitionName()).thenReturn(null);
    when(housekeepingMetadata.getPath()).thenReturn(VALID_TABLE_PATH);
    when(housekeepingMetadata.getCleanupAttempts()).thenReturn(0);
    when(hiveMetadataCleaner.tableExists(hiveClient, DATABASE, TABLE_NAME)).thenReturn(true);
    doThrow(RuntimeException.class).when(s3PathCleaner).cleanupPath(housekeepingMetadata);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);
    verify(housekeepingMetadata).setCleanupAttempts(1);
    verify(housekeepingMetadata).setHousekeepingStatus(FAILED);
    verify(housekeepingMetadataRepository).save(housekeepingMetadata);
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
  }

  @Test
  public void shouldSkipCleanupForIcebergTableByTableType() {
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);

    // Mock table properties to indicate Iceberg table by table_type
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("table_type", "ICEBERG");
    when(hiveClient.getTableProperties(DATABASE, TABLE_NAME)).thenReturn(tableProperties);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);
    verify(hiveMetadataCleaner, never()).dropTable(any(), any());
    verify(hiveMetadataCleaner, never()).dropPartition(any(), any());
    verify(s3PathCleaner, never()).cleanupPath(any());
    verify(housekeepingMetadata).setHousekeepingStatus(SKIPPED);
    verify(housekeepingMetadataRepository).save(housekeepingMetadata);
  }

  @Test
  public void shouldSkipCleanupForIcebergTableByFormat() {
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);

    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("format", "ICEBERG/PARQUET");
    when(hiveClient.getTableProperties(DATABASE, TABLE_NAME)).thenReturn(tableProperties);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);
    verify(hiveMetadataCleaner, never()).dropTable(any(), any());
    verify(hiveMetadataCleaner, never()).dropPartition(any(), any());
    verify(s3PathCleaner, never()).cleanupPath(any());
    verify(housekeepingMetadata).setHousekeepingStatus(SKIPPED);
    verify(housekeepingMetadataRepository).save(housekeepingMetadata);
  }

  @Test
  public void shouldSkipCleanupForIcebergTableByOutputFormat() {
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);

    // set storage descriptor properties to include Iceberg table by outputFormat
    Map<String, String> storageDescriptorProperties = new HashMap<>();
    storageDescriptorProperties.put("outputFormat", "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat");
    when(hiveClient.getStorageDescriptorProperties(DATABASE, TABLE_NAME)).thenReturn(storageDescriptorProperties);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);
    verify(hiveMetadataCleaner, never()).dropTable(any(), any());
    verify(hiveMetadataCleaner, never()).dropPartition(any(), any());
    verify(s3PathCleaner, never()).cleanupPath(any());
    verify(housekeepingMetadata).setHousekeepingStatus(SKIPPED);
    verify(housekeepingMetadataRepository).save(housekeepingMetadata);
  }

  @Test
  public void shouldProceedWithCleanupForNonIcebergTable() {
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(housekeepingMetadata.getPartitionName()).thenReturn(null);
    when(housekeepingMetadata.getPath()).thenReturn(VALID_TABLE_PATH);
    when(housekeepingMetadata.getCleanupAttempts()).thenReturn(0);
    when(housekeepingMetadataRepository.countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE, TABLE_NAME))
        .thenReturn(LONG_ZERO);

    Map<String, String> tableProperties = new HashMap<>(); // Mock table properties & outputFormat indicating a non-Iceberg table
    tableProperties.put("table_type", "MANAGED_TABLE");
    tableProperties.put("format", "PARQUET");
    when(hiveClient.getTableProperties(DATABASE, TABLE_NAME)).thenReturn(tableProperties);

    Map<String, String> storageDescriptorProperties = new HashMap<>();
    storageDescriptorProperties.put("outputFormat", "org.apache.mr.hive.HiveOutputFormat");
    when(hiveClient.getStorageDescriptorProperties(DATABASE, TABLE_NAME)).thenReturn(storageDescriptorProperties);
    // mock existing table to check the cleanup operations are only attempted on tables that actually exist
    when(hiveMetadataCleaner.tableExists(hiveClient, DATABASE, TABLE_NAME)).thenReturn(true);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);
    verify(hiveMetadataCleaner).dropTable(housekeepingMetadata, hiveClient);
    verify(s3PathCleaner).cleanupPath(housekeepingMetadata);
    verify(housekeepingMetadata).setCleanupAttempts(1);
    verify(housekeepingMetadata).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository).save(housekeepingMetadata);
  }

  @Test
  public void shouldHandleNullTableTypeAndFormatWithoutNPE() {
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(housekeepingMetadata.getPartitionName()).thenReturn(null);
    when(housekeepingMetadata.getPath()).thenReturn(VALID_TABLE_PATH);
    when(housekeepingMetadata.getCleanupAttempts()).thenReturn(0);
    when(housekeepingMetadataRepository.countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE, TABLE_NAME))
        .thenReturn(LONG_ZERO);

    // mock table properties and storage descriptor with null values for table_type, format and outputFormat
    Map<String, String> tableProperties = new HashMap<>();
    when(hiveClient.getTableProperties(DATABASE, TABLE_NAME)).thenReturn(tableProperties);
    Map<String, String> storageDescriptorProperties = new HashMap<>();
    when(hiveClient.getStorageDescriptorProperties(DATABASE, TABLE_NAME)).thenReturn(storageDescriptorProperties);
    when(hiveMetadataCleaner.tableExists(hiveClient, DATABASE, TABLE_NAME)).thenReturn(true);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);

    // should proceed with cleanup since no Iceberg properties were found
    verify(hiveMetadataCleaner).dropTable(housekeepingMetadata, hiveClient);
    verify(s3PathCleaner).cleanupPath(housekeepingMetadata);
    verify(housekeepingMetadata).setCleanupAttempts(1);
    verify(housekeepingMetadata).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository).save(housekeepingMetadata);
  }

  @Test
  public void shouldSkipCleanupForIcebergTableWithAllIndicators() {
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);

    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("table_type", "ICEBERG");
    tableProperties.put("format", "ICEBERG/PARQUET");
    when(hiveClient.getTableProperties(DATABASE, TABLE_NAME)).thenReturn(tableProperties);

    Map<String, String> storageDescriptorProperties = new HashMap<>();
    storageDescriptorProperties.put("outputFormat", "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat");
    when(hiveClient.getStorageDescriptorProperties(DATABASE, TABLE_NAME)).thenReturn(storageDescriptorProperties);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);
    verify(hiveMetadataCleaner, never()).dropTable(any(), any());
    verify(hiveMetadataCleaner, never()).dropPartition(any(), any());
    verify(s3PathCleaner, never()).cleanupPath(any());
    verify(housekeepingMetadata).setHousekeepingStatus(SKIPPED);
    verify(housekeepingMetadataRepository).save(housekeepingMetadata);
  }

  @Test
  public void shouldProceedWithCleanupWhenIcebergCheckThrowsException() {
    when(hiveClientFactory.newInstance()).thenReturn(hiveClient);
    when(housekeepingMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(housekeepingMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(housekeepingMetadata.getPartitionName()).thenReturn(null);
    when(housekeepingMetadata.getPath()).thenReturn(VALID_TABLE_PATH);
    when(housekeepingMetadata.getCleanupAttempts()).thenReturn(0);
    when(housekeepingMetadataRepository.countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE, TABLE_NAME))
        .thenReturn(LONG_ZERO);

    // Mock that fetching table properties throws an exception
    when(hiveClient.getTableProperties(DATABASE, TABLE_NAME))
        .thenThrow(new RuntimeException("Metastore unavailable"));

    // Mock that table exists
    when(hiveMetadataCleaner.tableExists(hiveClient, DATABASE, TABLE_NAME)).thenReturn(true);

    expiredMetadataHandler.cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);
    verify(hiveMetadataCleaner).dropTable(housekeepingMetadata, hiveClient);
    verify(s3PathCleaner).cleanupPath(housekeepingMetadata);
    verify(housekeepingMetadata).setCleanupAttempts(1);
    verify(housekeepingMetadata).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository).save(housekeepingMetadata);
  }
}
