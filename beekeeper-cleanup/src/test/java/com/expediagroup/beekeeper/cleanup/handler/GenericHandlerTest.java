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
package com.expediagroup.beekeeper.cleanup.handler;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;

import com.expediagroup.beekeeper.cleanup.path.aws.S3PathCleaner;
import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;
import com.expediagroup.beekeeper.core.model.PathStatus;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;

@ExtendWith(MockitoExtension.class)
public class GenericHandlerTest {

  @Mock private HousekeepingPathRepository housekeepingPathRepository;
  @Mock private S3PathCleaner pathCleaner;
  @Mock private EntityHousekeepingPath mockPath;
  @Mock private Pageable mockPageable;
  @Mock private PageImpl<EntityHousekeepingPath> mockPage;

  private UnreferencedHandler handler;

  @BeforeEach
  public void initTest() {
    handler = new UnreferencedHandler(housekeepingPathRepository, pathCleaner);
  }

  @Test
  public void typicalProcessDryRunPage() {
    when(mockPage.getContent()).thenReturn(List.of(mockPath));
    handler.processPage(mockPageable, mockPage, true);
    verify(mockPageable).next();
    verify(pathCleaner).cleanupPath(mockPath);
  }

  @Test
  public void typicalProcessPage() {
    when(mockPath.getCleanupAttempts()).thenReturn(0);
    when(mockPage.getContent()).thenReturn(List.of(mockPath));
    handler.processPage(mockPageable, mockPage, false);
    verify(pathCleaner).cleanupPath(mockPath);
    verify(mockPageable, never()).next();
    verify(mockPath).setCleanupAttempts(1);
    verify(mockPath).setPathStatus(PathStatus.DELETED);
    verify(housekeepingPathRepository).save(mockPath);
  }

  @Test
  public void processPageFails() {
    when(mockPath.getCleanupAttempts()).thenReturn(0);
    doThrow(RuntimeException.class).when(pathCleaner).cleanupPath(mockPath);
    when(mockPage.getContent()).thenReturn(List.of(mockPath));
    handler.processPage(mockPageable, mockPage, false);
    verify(mockPageable, never()).next();
    verify(mockPath).setCleanupAttempts(1);
    verify(mockPath).setPathStatus(PathStatus.FAILED);
    verify(housekeepingPathRepository).save(mockPath);
  }
}
