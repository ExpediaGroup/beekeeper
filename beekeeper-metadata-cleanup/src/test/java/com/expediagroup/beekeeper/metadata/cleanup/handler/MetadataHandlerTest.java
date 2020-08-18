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

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.metadata.cleanup.cleaner.ExpiredMetadataCleanup;

@ExtendWith(MockitoExtension.class)
public class MetadataHandlerTest {

  private @Mock HousekeepingMetadata housekeepingMetadata;
  private @Mock Pageable mockPageable;
  private @Mock PageImpl<HousekeepingMetadata> mockPage;
  private @Mock ExpiredMetadataCleanup expiredMetadataCleanup;

  private static final LocalDateTime CLEANUP_INSTANCE = LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);

  private MetadataHandler handler;

  @BeforeEach
  public void init() {
    handler = new MetadataHandler(expiredMetadataCleanup);
  }

  @Test
  public void typicalCleanup() {
    when(mockPage.getContent()).thenReturn(List.of(housekeepingMetadata));

    handler.processPage(mockPageable, CLEANUP_INSTANCE, mockPage, false);
    verify(expiredMetadataCleanup).cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, false);
    verify(mockPageable, never()).next();
  }

  @Test
  public void typicalDryRunCleanup() {
    when(mockPage.getContent()).thenReturn(List.of(housekeepingMetadata));

    handler.processPage(mockPageable, CLEANUP_INSTANCE, mockPage, true);
    verify(expiredMetadataCleanup).cleanupMetadata(housekeepingMetadata, CLEANUP_INSTANCE, true);
    verify(mockPageable).next();
  }

  @Test
  public void typicalRecordFind() {
    handler.findRecordsToClean(CLEANUP_INSTANCE, mockPageable);
    verify(expiredMetadataCleanup).findRecordsToClean(CLEANUP_INSTANCE, mockPageable);
  }
}
