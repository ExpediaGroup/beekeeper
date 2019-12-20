/**
 * Copyright (C) 2019 Expedia, Inc.
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
package com.expediagroup.beekeeper.cleanup.service;

import static java.lang.String.format;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.EnumMap;
import java.util.List;

import com.expediagroup.beekeeper.core.model.LifeCycleEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import io.micrometer.core.annotation.Timed;

import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;
import com.expediagroup.beekeeper.core.model.PathStatus;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;

public class PagingCleanupService implements CleanupService {

  private final Logger log = LoggerFactory.getLogger(PagingCleanupService.class);
  private final HousekeepingPathRepository housekeepingPathRepository;
  private final EnumMap<LifeCycleEventType,PathCleaner> pathCleanerMap;
  private final boolean dryRunEnabled;
  private final int pageSize;

  public PagingCleanupService(
          HousekeepingPathRepository housekeepingPathRepository,
          EnumMap<LifeCycleEventType, PathCleaner> pathCleanerMap,
          int pageSize,
          boolean dryRunEnabled
  ) {
    this.housekeepingPathRepository = housekeepingPathRepository;
    this.pathCleanerMap = pathCleanerMap;
    this.pageSize = pageSize;
    this.dryRunEnabled = dryRunEnabled;
  }

  @Override
  @Timed("cleanup-job")
  public void cleanUp(Instant referenceTime) {
    try {
      Pageable pageable = PageRequest.of(0, pageSize).first();
      LocalDateTime instant = LocalDateTime.ofInstant(referenceTime, ZoneOffset.UTC);

      Page<EntityHousekeepingPath> page = housekeepingPathRepository.findRecordsForCleanupByModifiedTimestamp(instant,
          pageable);

      while (!page.getContent().isEmpty()) {
        processPage(page.getContent());
        if (dryRunEnabled) {
          pageable = pageable.next();
        }
        page = housekeepingPathRepository.findRecordsForCleanupByModifiedTimestamp(instant, pageable);
      }
    } catch (Exception e) {
      throw new BeekeeperException(format("Cleanup failed for instant %s", referenceTime.toString()), e);
    }
  }

  private void processPage(List<EntityHousekeepingPath> pageContent) {
    if (dryRunEnabled) {
//      pageContent.forEach(pathCleaner::cleanupPath);
    } else {
      pageContent.forEach(this::cleanupContent);
    }
  }

  private void cleanupContent(EntityHousekeepingPath housekeepingPath) {
    try {
      LifeCycleEventType lifecycleType = LifeCycleEventType.valueOf(housekeepingPath.getLifecycleType());
      PathCleaner pathCleaner = pathCleanerMap.get(lifecycleType);
      log.info("Cleaning up path \"{}\" using \"{}\"", housekeepingPath.getPath(), pathCleaner.toString());

      if (pathCleaner.cleanupPath(housekeepingPath)) {
        log.info("Successfully deleted \"{}\" path.", housekeepingPath.getPath());
        updateAttemptsAndStatus(housekeepingPath, PathStatus.DELETED);
      } else {
        log.info("Skip deletion of path \"{}\" path", housekeepingPath.getPath());
      }
    } catch (Exception e) {
      updateAttemptsAndStatus(housekeepingPath, PathStatus.FAILED);
      log.warn("Unexpected exception deleting \"{}\"", housekeepingPath.getPath(), e);
    }
  }

  private void updateAttemptsAndStatus(EntityHousekeepingPath housekeepingPath, PathStatus status) {
    housekeepingPath.setCleanupAttempts(housekeepingPath.getCleanupAttempts() + 1);
    housekeepingPath.setPathStatus(status);
    housekeepingPathRepository.save(housekeepingPath);
  }
}
