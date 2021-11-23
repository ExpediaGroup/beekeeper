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

import static java.time.temporal.ChronoUnit.DAYS;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.springframework.transaction.annotation.Transactional;

import io.micrometer.core.annotation.Timed;

import com.expediagroup.beekeeper.cleanup.service.RepositoryCleanupService;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;

public class MetadataRepositoryCleanupService implements RepositoryCleanupService {

  private final HousekeepingMetadataRepository housekeepingMetadataRepository;
  private final int retentionPeriodInDays;

  public MetadataRepositoryCleanupService(
      HousekeepingMetadataRepository housekeepingMetadataRepository, int retentionPeriodInDays) {
    this.housekeepingMetadataRepository = housekeepingMetadataRepository;
    this.retentionPeriodInDays = retentionPeriodInDays;
  }

  @Override
  @Transactional
  @Timed("metadata-repository-cleanup-job")
  public void cleanUp(Instant referenceTime) {
    LocalDateTime instant = LocalDateTime.ofInstant(referenceTime, ZoneOffset.UTC);
    housekeepingMetadataRepository.cleanUpOldDeletedRecords(instant.minus(retentionPeriodInDays, DAYS));
  }
}
