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
import static org.mockito.Mockito.verify;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.time.LocalDateTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import com.expediagroup.beekeeper.core.aws.S3PathCleaner;
import com.expediagroup.beekeeper.core.hive.HiveMetadataCleaner;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;

@ExtendWith(MockitoExtension.class)
public class ExpiredMetadataHandlerTest {

  @Mock
  private HousekeepingMetadataRepository metadataRepository;
  
  @Mock 
  private HiveMetadataCleaner hiveCleaner;

  @Mock
  private S3PathCleaner s3Cleaner;
  
  private ExpiredMetadataHandler handler;

  @BeforeEach
  public void init() {
    handler = new ExpiredMetadataHandler(metadataRepository, hiveCleaner, s3Cleaner);
  }

  @Test
  public void verifyMetadataCleaner() {
    assertThat(handler.getMetadataCleaner()).isInstanceOf(HiveMetadataCleaner.class);
  }

  @Test
  public void verifyPathCleaner() {
    assertThat(handler.getPathCleaner()).isInstanceOf(S3PathCleaner.class);
  }

  @Test
  public void verifyLifecycle() {
    assertThat(handler.getLifecycleType()).isEqualTo(EXPIRED);
  }

  @Test
  public void verifyHousekeepingMetadataFetch() {
    LocalDateTime now = LocalDateTime.now();
    Pageable emptyPageable = PageRequest.of(0, 1);
    handler.findRecordsToClean(now, emptyPageable);
    verify(metadataRepository).findRecordsForCleanupByModifiedTimestamp(now, emptyPageable);
  }

  @Test
  public void verifyHousekeepingMetadataMatchingRecordFetch() {
    LocalDateTime now = LocalDateTime.now();
    Pageable emptyPageable = PageRequest.of(0, 1);
    String databaseName = "database";
    String tableName = "table_name";
    handler.findMatchingRecords(databaseName, "table_name", emptyPageable);
    verify(metadataRepository).findRecordsForGivenDatabaseAndTable(databaseName, tableName, emptyPageable);
  }

}