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

import java.time.LocalDateTime;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.model.PathStatus;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;

public abstract class GenericHandler {

  private final Logger log = LoggerFactory.getLogger(GenericHandler.class);

  public abstract HousekeepingPathRepository getHousekeepingPathRepository();

  public abstract LifecycleEventType getLifecycleType();

  public abstract PathCleaner getPathCleaner();

  public abstract Page<EntityHousekeepingPath> findRecordsToClean(LocalDateTime instant, Pageable pageable);

  /**
   * Processes a pageable entityHouseKeepingPath page.
   *
   * @param pageable Pageable to iterate through for dryRun
   * @param page Page to get content from
   * @param dryRunEnabled Dry Run boolean flag
   * @implNote This parent handler expects the child's cleanupPath call to update & remove the record from this call such
   * that subsequent DB queries will not return the record. Hence why we only call next during dryRuns where no updates occur.
   * @implNote Note that we only expect pageable.next to be called on
   */
  public void processPage(Pageable pageable, Page<EntityHousekeepingPath> page, boolean dryRunEnabled) {
    List<EntityHousekeepingPath> pageContent = page.getContent();
    if (dryRunEnabled) {
      pageContent.forEach(this::cleanUpPath);
      pageable.next();
    } else {
      pageContent.forEach(this::cleanupContent);
    }
  }

  private void cleanUpPath(EntityHousekeepingPath housekeepingPath) {
    PathCleaner pathCleaner = getPathCleaner();
    pathCleaner.cleanupPath(housekeepingPath);
  }

  private void cleanupContent(EntityHousekeepingPath housekeepingPath) {
    try {
      log.info("Cleaning up path \"{}\"", housekeepingPath.getPath());
      cleanUpPath(housekeepingPath);
      updateAttemptsAndStatus(housekeepingPath, PathStatus.DELETED);
    } catch (Exception e) {
      updateAttemptsAndStatus(housekeepingPath, PathStatus.FAILED);
      log.warn("Unexpected exception deleting \"{}\"", housekeepingPath.getPath(), e);
    }
  }

  private void updateAttemptsAndStatus(EntityHousekeepingPath housekeepingPath, PathStatus status) {
    housekeepingPath.setCleanupAttempts(housekeepingPath.getCleanupAttempts() + 1);
    housekeepingPath.setPathStatus(status);
    getHousekeepingPathRepository().save(housekeepingPath);
  }
}
