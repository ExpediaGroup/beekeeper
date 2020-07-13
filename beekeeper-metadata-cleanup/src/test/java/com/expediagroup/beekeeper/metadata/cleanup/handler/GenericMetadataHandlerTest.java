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
import static org.mockito.Mockito.when;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DELETED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.FAILED;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;

import com.expediagroup.beekeeper.core.aws.S3PathCleaner;
import com.expediagroup.beekeeper.core.hive.HiveMetadataCleaner;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;

@ExtendWith(MockitoExtension.class)
public class GenericMetadataHandlerTest {

  @Mock
  private HousekeepingMetadataRepository housekeepingMetadataRepository;
  @Mock
  private S3PathCleaner pathCleaner;
  @Mock
  private HiveMetadataCleaner metadataCleaner;
  @Mock
  private HousekeepingMetadata mockMetadata;
  @Mock
  private Pageable mockPageable;
  @Mock
  private Pageable nextPageable;
  @Mock
  private PageImpl<HousekeepingMetadata> mockPage;
  @Mock
  private PageImpl<HousekeepingMetadata> nextPage;

  private ExpiredMetadataHandler handler;

  private static final String DATABASE = "database";
  private static final String TABLE_NAME = "tableName";

  @BeforeEach
  public void init() {
    when(mockPage.getContent()).thenReturn(List.of(mockMetadata));
    when(nextPage.getContent()).thenReturn(Collections.emptyList());

    when(mockMetadata.getDatabaseName()).thenReturn(DATABASE);
    when(mockMetadata.getTableName()).thenReturn(TABLE_NAME);


    when(housekeepingMetadataRepository.findRecordsForGivenDatabaseAndTable(DATABASE, TABLE_NAME, mockPageable))
        .thenReturn(mockPage);
    when(housekeepingMetadataRepository.findRecordsForGivenDatabaseAndTable(DATABASE, TABLE_NAME, nextPageable))
        .thenReturn(nextPage);
    when(mockPageable.next()).thenReturn(nextPageable);

    handler = new ExpiredMetadataHandler(housekeepingMetadataRepository, metadataCleaner, pathCleaner);
  }

  @Test
  public void typicalRunCleaningTable() {
    when(mockMetadata.getCleanupAttempts()).thenReturn(0);

    Pageable pageable = handler.processPage(mockPageable, mockPage, false);
    verify(metadataCleaner).cleanupMetadata(mockMetadata);
    // TODO
    // not implemented yet
    // verify(pathCleaner).cleanupPath(mockMetadata);
    verify(metadataCleaner, never()).cleanupPartition(mockMetadata);
    verify(mockMetadata).setCleanupAttempts(1);
    verify(mockMetadata).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository).save(mockMetadata);
    assertThat(pageable).isEqualTo(pageable);
  }

  @Test
  public void typicalRunCleaningPartition() {
    // return more than 1 for content size
    PageImpl<HousekeepingMetadata> mockPageWithPartition = Mockito.mock(PageImpl.class);
    when(housekeepingMetadataRepository.findRecordsForGivenDatabaseAndTable(DATABASE, TABLE_NAME, mockPageable))
        .thenReturn(mockPageWithPartition);
    when(mockPageWithPartition.getContent()).thenReturn(List.of(mockMetadata, mockMetadata));

    Pageable pageable = handler.processPage(mockPageable, mockPage, false);
    verify(metadataCleaner).cleanupPartition(mockMetadata);
    // TODO
    // verify(pathCleaner).cleanupPath(housekeepingPath);

    verify(metadataCleaner, never()).cleanupMetadata(mockMetadata);
    verify(mockMetadata).setCleanupAttempts(1);
    verify(mockMetadata).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository).save(mockMetadata);
    assertThat(pageable).isEqualTo(pageable);
  }

  @Test
  public void typicalDryRunCleaningTable() {
    Pageable pageable = handler.processPage(mockPageable, mockPage, true);
    verify(metadataCleaner).cleanupMetadata(mockMetadata);
    // TODO
    // not implemented yet
    // verify(pathCleaner).cleanupPath(mockMetadata);
    verify(metadataCleaner, never()).cleanupPartition(mockMetadata);

    Mockito.verifyNoMoreInteractions(mockMetadata);
    verify(mockMetadata, never()).setCleanupAttempts(1);
    verify(mockMetadata, never()).setHousekeepingStatus(DELETED);
    verify(housekeepingMetadataRepository, never()).save(mockMetadata);
    assertThat(pageable).isEqualTo(nextPageable);
  }

  @Test
  public void expectedMetadataCleanupFailure() {
    // fail with runtime exception
    when(mockMetadata.getCleanupAttempts()).thenReturn(0);
    doThrow(RuntimeException.class).when(metadataCleaner).cleanupMetadata(mockMetadata);
    when(mockPage.getContent()).thenReturn(List.of(mockMetadata));

    Pageable pageable = handler.processPage(mockPageable, mockPage, false);
    verify(mockMetadata).setCleanupAttempts(1);
    verify(mockMetadata).setHousekeepingStatus(FAILED);
    verify(housekeepingMetadataRepository).save(mockMetadata);
    assertThat(pageable).isEqualTo(pageable);
  }

  // TODO
  // fix when path cleaner is changed to accepting HousekeepingEntity
  // @Test
  // public void expectedPathCleanupFailure() {
  // // fail with runtime exception
  // when(mockMetadata.getCleanupAttempts()).thenReturn(0);
  // doThrow(RuntimeException.class).when(pathCleaner).cleanupPath(mockMetadata);
  // when(mockPage.getContent()).thenReturn(List.of(mockMetadata));
  //
  // Pageable pageable = handler.processPage(mockPageable, mockPage, false);
  // verify(mockMetadata).setCleanupAttempts(1);
  // verify(mockMetadata).setHousekeepingStatus(FAILED);
  // verify(housekeepingMetadataRepository).save(mockMetadata);
  // assertThat(pageable).isEqualTo(pageable);
  // }


}
