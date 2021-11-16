/**
 * Copyright (C) 2019-2021 Expedia, Inc.
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
package com.expediagroup.beekeeper.metadata.cleanup.service;

import static org.mockito.Mockito.verify;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;

@ExtendWith(MockitoExtension.class)
public class MetadataRepositoryCleanupServiceTest {

  private @Mock HousekeepingMetadataRepository housekeepingMetadataRepository;

  @Test
  public void typical() {
    MetadataRepositoryCleanupService repositoryCleanupService = new MetadataRepositoryCleanupService(housekeepingMetadataRepository,
        2);
    LocalDateTime now = LocalDateTime.of(2021, 11, 12, 10, 10);
    repositoryCleanupService.cleanUp(now.toInstant(ZoneOffset.UTC));

    LocalDateTime referenceTime = LocalDateTime.of(2021, 11, 10, 10, 10);
    verify(housekeepingMetadataRepository).cleanUpOldDeletedRecords(referenceTime);
  }
}