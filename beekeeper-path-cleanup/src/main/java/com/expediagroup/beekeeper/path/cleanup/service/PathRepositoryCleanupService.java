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
package com.expediagroup.beekeeper.path.cleanup.service;

import static java.time.temporal.ChronoUnit.DAYS;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.springframework.transaction.annotation.Transactional;

import io.micrometer.core.annotation.Timed;

import com.expediagroup.beekeeper.cleanup.service.RepositoryCleanupService;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;

public class PathRepositoryCleanupService implements RepositoryCleanupService {

  private final HousekeepingPathRepository housekeepingPathRepository;
  private final int numberOfRetentionDays;

  public PathRepositoryCleanupService(
      HousekeepingPathRepository housekeepingPathRepository, int numberOfRetentionDays) {
    this.housekeepingPathRepository = housekeepingPathRepository;
    this.numberOfRetentionDays = numberOfRetentionDays;
  }

  @Override
  @Transactional
  @Timed("path-repository-cleanup-job")
  public void cleanUp(Instant referenceTime) {
    LocalDateTime instant = LocalDateTime.ofInstant(referenceTime, ZoneOffset.UTC);
    housekeepingPathRepository.cleanUpOldDeletedRecords(instant.minus(numberOfRetentionDays, DAYS));
  }
}
