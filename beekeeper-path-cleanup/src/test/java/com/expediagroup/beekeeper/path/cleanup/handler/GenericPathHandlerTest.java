/**
 * Copyright (C) 2019-2025 Expedia, Inc.
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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
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

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;

import com.expediagroup.beekeeper.cleanup.aws.S3PathCleaner;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;
import com.expediagroup.beekeeper.core.service.BeekeeperHistoryService;

@ExtendWith(MockitoExtension.class)
public class GenericPathHandlerTest {

  @Mock
  private HousekeepingPathRepository housekeepingPathRepository;
  @Mock
  private S3PathCleaner pathCleaner;
  @Mock
  private BeekeeperHistoryService beekeeperHistoryService;
  @Mock
  private HousekeepingPath mockPath;
  @Mock
  private Pageable mockPageable;
  @Mock
  private Pageable nextPage;
  @Mock
  private PageImpl<HousekeepingPath> mockPage;
  private static final String VALID_TABLE_PATH = "s3://bucket/table";

  private UnreferencedPathHandler handler;

  @BeforeEach
  public void initTest() {
    handler = new UnreferencedPathHandler(housekeepingPathRepository, pathCleaner, beekeeperHistoryService);
    when(mockPath.getPath()).thenReturn(VALID_TABLE_PATH);
  }

  @Test
  public void typicalProcessDryRunPage() {
    when(mockPage.getContent()).thenReturn(List.of(mockPath));
    when(mockPageable.next()).thenReturn(nextPage);
    Pageable pageable = handler.processPage(mockPageable, mockPage, true);
    verify(pathCleaner).cleanupPath(mockPath);
    assertThat(pageable).isEqualTo(nextPage);
  }

  @Test
  public void typicalProcessPage() {
    when(mockPath.getCleanupAttempts()).thenReturn(0);
    when(mockPage.getContent()).thenReturn(List.of(mockPath));
    Pageable pageable = handler.processPage(mockPageable, mockPage, false);
    verify(pathCleaner).cleanupPath(mockPath);
    verify(mockPageable, never()).next();
    verify(mockPath).setCleanupAttempts(1);
    verify(mockPath).setHousekeepingStatus(DELETED);
    verify(housekeepingPathRepository).save(mockPath);
    verify(beekeeperHistoryService).saveHistory(any(), eq(DELETED));
    assertThat(pageable).isEqualTo(pageable);
  }

  @Test
  public void processPageFails() {
    when(mockPath.getCleanupAttempts()).thenReturn(0);
    doThrow(RuntimeException.class).when(pathCleaner).cleanupPath(mockPath);
    when(mockPage.getContent()).thenReturn(List.of(mockPath));
    Pageable pageable = handler.processPage(mockPageable, mockPage, false);
    verify(mockPageable, never()).next();
    verify(mockPath).setCleanupAttempts(1);
    verify(mockPath).setHousekeepingStatus(FAILED);
    verify(housekeepingPathRepository).save(mockPath);
    verify(beekeeperHistoryService).saveHistory(any(), eq(FAILED_TO_DELETE));
    assertThat(pageable).isEqualTo(pageable);
  }

  @Test
  public void processPageInvalidPath() {
    when(mockPath.getPath()).thenReturn("invalid");
    when(mockPage.getContent()).thenReturn(List.of(mockPath));
    Pageable pageable = handler.processPage(mockPageable, mockPage, false);
    verify(pathCleaner, never()).cleanupPath(mockPath);
    verify(mockPageable, never()).next();
    verify(mockPath, never()).setCleanupAttempts(1);
    verify(mockPath).setHousekeepingStatus(SKIPPED);
    verify(housekeepingPathRepository).save(mockPath);
    verify(beekeeperHistoryService).saveHistory(any(), eq(SKIPPED));
    assertThat(pageable).isEqualTo(pageable);
  }
}
