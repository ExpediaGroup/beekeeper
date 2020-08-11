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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DELETED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.FAILED;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;

import com.expediagroup.beekeeper.cleanup.aws.S3PathCleaner;
import com.expediagroup.beekeeper.cleanup.hive.HiveMetadataCleaner;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;

@ExtendWith(MockitoExtension.class)
public class GenericMetadataHandlerTest {

  private @Mock HousekeepingMetadataRepository housekeepingMetadataRepository;
  private @Mock S3PathCleaner pathCleaner;
  private @Mock HiveMetadataCleaner metadataCleaner;
  private @Mock HousekeepingMetadata mockMetadata;
  private @Mock Pageable mockPageable;
  private @Mock Pageable nextPageable;
  private @Mock PageImpl<HousekeepingMetadata> mockPage;

  private ExpiredMetadataHandler handler;

  private static final String DATABASE = "database";
  private static final String TABLE_NAME = "tableName";
  private static final String PARTITION_NAME = "event_date=2020-01-01/event_hour=0/event_type=A";

  @BeforeEach
  public void init() {
    when(mockPage.getContent()).thenReturn(List.of(mockMetadata));
    when(mockMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(mockMetadata.getTableName()).thenReturn(TABLE_NAME);
    when(mockMetadata.getPartitionName()).thenReturn(null);

    handler = new ExpiredMetadataHandler(housekeepingMetadataRepository, metadataCleaner, pathCleaner);
  }

  @Test
  public void typicalRunDroppingTable() {
    when(mockMetadata.getCleanupAttempts()).thenReturn(0);
    when(
        housekeepingMetadataRepository.countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE, TABLE_NAME))
        .thenReturn(Long.valueOf(0));
    when(metadataCleaner.tableExists(DATABASE, TABLE_NAME)).thenReturn(true);

    Pageable pageable = handler.processPage(mockPageable, mockPage, false);
    verify(metadataCleaner).dropTable(mockMetadata);
    verify(pathCleaner).cleanupPath(mockMetadata);
    verify(metadataCleaner, never()).dropPartition(mockMetadata);
    verify(mockMetadata).setCleanupAttempts(1);
    verify(mockMetadata).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository).save(mockMetadata);
    assertThat(pageable).isEqualTo(pageable);
  }

  @Test
  public void typicalRunDroppingPartition() {
    when(mockMetadata.getPartitionName()).thenReturn(PARTITION_NAME);
    when(metadataCleaner.dropPartition(Mockito.any())).thenReturn(true);
    when(metadataCleaner.tableExists(DATABASE, TABLE_NAME)).thenReturn(true);

    Pageable pageable = handler.processPage(mockPageable, mockPage, false);
    verify(metadataCleaner).dropPartition(mockMetadata);
    verify(pathCleaner).cleanupPath(mockMetadata);
    verify(metadataCleaner, never()).dropTable(mockMetadata);
    verify(mockMetadata).setCleanupAttempts(1);
    verify(mockMetadata).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository).save(mockMetadata);
    assertThat(pageable).isEqualTo(pageable);
  }

  @Test
  public void tableWithExistingPartitionNotDeleted() {
    when(housekeepingMetadataRepository.countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE, TABLE_NAME))
        .thenReturn(Long.valueOf(1));

    Pageable pageable = handler.processPage(mockPageable, mockPage, false);
    verify(metadataCleaner, never()).dropTable(mockMetadata);
    verify(pathCleaner, never()).cleanupPath(mockMetadata);
    verify(metadataCleaner, never()).dropPartition(mockMetadata);
    verify(mockMetadata, never()).setCleanupAttempts(1);
    verify(mockMetadata, never()).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository, never()).save(mockMetadata);
    assertThat(pageable).isEqualTo(pageable);
  }

  @Test
  public void typicalDryRunCleaningTable() {
    when(mockPageable.next()).thenReturn(nextPageable);
    when(
        housekeepingMetadataRepository.countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE, TABLE_NAME))
        .thenReturn(Long.valueOf(0));
    when(metadataCleaner.tableExists(DATABASE, TABLE_NAME)).thenReturn(true);

    Pageable pageable = handler.processPage(mockPageable, mockPage, true);
    verify(metadataCleaner).dropTable(mockMetadata);
    verify(pathCleaner).cleanupPath(mockMetadata);
    verify(metadataCleaner, never()).dropPartition(mockMetadata);
    verifyNoMoreInteractions(mockMetadata);
    verify(mockMetadata, never()).setCleanupAttempts(1);
    verify(mockMetadata, never()).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository, never()).save(mockMetadata);
    assertThat(pageable).isEqualTo(nextPageable);
  }

  @Test
  public void typicalDryRunCleaningPartition() {
    PageImpl<HousekeepingMetadata> mockPageWithPartition = Mockito.mock(PageImpl.class);
    when(mockPageable.next()).thenReturn(nextPageable);
    when(mockMetadata.getPartitionName()).thenReturn(PARTITION_NAME);
    when(metadataCleaner.tableExists(DATABASE, TABLE_NAME)).thenReturn(true);
    when(metadataCleaner.dropPartition(Mockito.any())).thenReturn(true);

    Pageable pageable = handler.processPage(mockPageable, mockPage, true);
    verify(metadataCleaner).dropPartition(mockMetadata);
    verify(pathCleaner).cleanupPath(mockMetadata);
    verify(metadataCleaner, never()).dropTable(mockMetadata);
    verifyNoMoreInteractions(mockMetadata);
    verify(mockMetadata, never()).setCleanupAttempts(1);
    verify(mockMetadata, never()).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository, never()).save(mockMetadata);
    assertThat(pageable).isEqualTo(nextPageable);
  }

  @Test
  public void dontCleanupTableOrPathWhenTableDoesntExist() {
    when(mockMetadata.getCleanupAttempts()).thenReturn(0);
    when(
        housekeepingMetadataRepository.countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE, TABLE_NAME))
        .thenReturn(Long.valueOf(0));
    when(metadataCleaner.tableExists(DATABASE, TABLE_NAME)).thenReturn(false);

    Pageable pageable = handler.processPage(mockPageable, mockPage, false);
    verify(metadataCleaner, never()).dropTable(mockMetadata);
    verify(pathCleaner, never()).cleanupPath(mockMetadata);
    verify(metadataCleaner, never()).dropPartition(mockMetadata);
    verify(mockMetadata).setCleanupAttempts(1);
    verify(mockMetadata).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository).save(mockMetadata);
    assertThat(pageable).isEqualTo(pageable);
  }

  @Test
  public void dontCleanupPartitionWhenTableDoesntExist(){
    when(mockMetadata.getCleanupAttempts()).thenReturn(0);
    when(mockMetadata.getPartitionName()).thenReturn(PARTITION_NAME);
    when(metadataCleaner.tableExists(DATABASE, TABLE_NAME)).thenReturn(false);

    Pageable pageable = handler.processPage(mockPageable, mockPage, false);
    verify(metadataCleaner, never()).dropPartition(mockMetadata);
    verify(pathCleaner, never()).cleanupPath(mockMetadata);
    verify(metadataCleaner, never()).dropTable(mockMetadata);
    verify(mockMetadata).setCleanupAttempts(1);
    verify(mockMetadata).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository).save(mockMetadata);
    assertThat(pageable).isEqualTo(pageable);
  }

  @Test
  public void dontCleanupPathWhenPartitionDoesntExist() {
    when(metadataCleaner.dropPartition(Mockito.any())).thenReturn(false);
    when(mockMetadata.getPartitionName()).thenReturn(PARTITION_NAME);
    when(metadataCleaner.tableExists(DATABASE, TABLE_NAME)).thenReturn(true);

    Pageable pageable = handler.processPage(mockPageable, mockPage, false);
    verify(metadataCleaner).dropPartition(mockMetadata);
    verify(pathCleaner, never()).cleanupPath(mockMetadata);
    verify(metadataCleaner, never()).dropTable(mockMetadata);
    verify(mockMetadata).setCleanupAttempts(1);
    verify(mockMetadata).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository).save(mockMetadata);
    assertThat(pageable).isEqualTo(pageable);
  }

  @Test
  public void expectedTableCleanupFailure() {
    when(mockMetadata.getCleanupAttempts()).thenReturn(0);
    when(
        housekeepingMetadataRepository.countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(DATABASE, TABLE_NAME))
        .thenReturn(Long.valueOf(0));
    doThrow(RuntimeException.class).when(metadataCleaner).dropTable(mockMetadata);
    when(mockPage.getContent()).thenReturn(List.of(mockMetadata));
    when(metadataCleaner.tableExists(DATABASE, TABLE_NAME)).thenReturn(true);

    Pageable pageable = handler.processPage(mockPageable, mockPage, false);
    verify(mockMetadata).setCleanupAttempts(1);
    verify(mockMetadata).setHousekeepingStatus(FAILED);
    verify(housekeepingMetadataRepository).save(mockMetadata);
    assertThat(pageable).isEqualTo(pageable);
  }

  @Test
  public void expectedPathCleanupFailure() {
    when(mockMetadata.getCleanupAttempts()).thenReturn(0);
    doThrow(RuntimeException.class).when(pathCleaner).cleanupPath(mockMetadata);
    when(mockPage.getContent()).thenReturn(List.of(mockMetadata));
    when(metadataCleaner.tableExists(DATABASE, TABLE_NAME)).thenReturn(true);

    Pageable pageable = handler.processPage(mockPageable, mockPage, false);
    verify(mockMetadata).setCleanupAttempts(1);
    verify(mockMetadata).setHousekeepingStatus(FAILED);
    verify(housekeepingMetadataRepository).save(mockMetadata);
    assertThat(pageable).isEqualTo(pageable);
  }

  @Test
  public void expectedPartitionCleanupFailure() {
    PageImpl<HousekeepingMetadata> mockPageWithPartition = Mockito.mock(PageImpl.class);
    when(mockMetadata.getPartitionName()).thenReturn(PARTITION_NAME);
    when(mockMetadata.getCleanupAttempts()).thenReturn(0);
    doThrow(RuntimeException.class).when(metadataCleaner).dropPartition(mockMetadata);
    when(metadataCleaner.tableExists(DATABASE, TABLE_NAME)).thenReturn(true);

    Pageable pageable = handler.processPage(mockPageable, mockPage, false);
    verify(mockMetadata).setCleanupAttempts(1);
    verify(mockMetadata).setHousekeepingStatus(FAILED);
    verify(housekeepingMetadataRepository).save(mockMetadata);
    assertThat(pageable).isEqualTo(pageable);
  }
}
