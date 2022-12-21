/**
 * Copyright (C) 2019-2022 Expedia, Inc.
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

import java.time.LocalDateTime;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;

import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;
import com.expediagroup.beekeeper.core.validation.S3PathValidator;

public abstract class GenericPathHandler {

  private final Logger log = LoggerFactory.getLogger(GenericPathHandler.class);

  private final HousekeepingPathRepository housekeepingPathRepository;
  private final PathCleaner pathCleaner;

  public GenericPathHandler(HousekeepingPathRepository housekeepingPathRepository, PathCleaner pathCleaner) {
    this.housekeepingPathRepository = housekeepingPathRepository;
    this.pathCleaner = pathCleaner;
  }

  public abstract Slice<HousekeepingPath> findRecordsToClean(LocalDateTime instant, Pageable pageable);

  /**
   * Processes a pageable entityHouseKeepingPath page.
   *
   * @param pageable Pageable to iterate through for dryRun
   * @param page Page to get content from
   * @param dryRunEnabled Dry Run boolean flag
   * @implNote This parent handler expects the child's cleanupPath call to update & remove the record from this call
   *           such that subsequent DB queries will not return the record. Hence why we only call next during dryRuns
   *           where no updates occur.
   * @implNote Note that we only expect pageable.next to be called during a dry run.
   * @return Pageable to pass to query. In the case of dry runs, this is the next page.
   */
  public Pageable processPage(Pageable pageable, Slice<HousekeepingPath> page, boolean dryRunEnabled) {
    List<HousekeepingPath> pageContent = page.getContent();
    if (dryRunEnabled) {
      pageContent.forEach(this::cleanUpPath);
      return pageable.next();
    } else {
      pageContent.forEach(this::cleanupContent);
      return pageable;
    }
  }

  private boolean cleanUpPath(HousekeepingPath housekeepingPath) {
    if (S3PathValidator.validTablePath(housekeepingPath.getPath())) {
      pathCleaner.cleanupPath(housekeepingPath);
      return true;
    }
    log.warn("Will not clean up path \"{}\" because it is not valid.", housekeepingPath.getPath());
    return false;
  }

  private void cleanupContent(HousekeepingPath housekeepingPath) {
    try {
      log.info("Cleaning up path \"{}\"", housekeepingPath.getPath());
      if (cleanUpPath(housekeepingPath)) {
        updateAttemptsAndStatus(housekeepingPath, HousekeepingStatus.DELETED);
      } else {
        updateStatus(housekeepingPath, HousekeepingStatus.SKIPPED);
      }
    } catch (Exception e) {
      updateAttemptsAndStatus(housekeepingPath, HousekeepingStatus.FAILED);
      log.warn("Unexpected exception deleting \"{}\"", housekeepingPath.getPath(), e);
    }
  }

  private void updateAttemptsAndStatus(HousekeepingPath housekeepingPath, HousekeepingStatus status) {
    housekeepingPath.setCleanupAttempts(housekeepingPath.getCleanupAttempts() + 1);
    housekeepingPath.setHousekeepingStatus(status);
    housekeepingPathRepository.save(housekeepingPath);
  }

  private void updateStatus(HousekeepingPath housekeepingPath, HousekeepingStatus status) {
    housekeepingPath.setHousekeepingStatus(status);
    housekeepingPathRepository.save(housekeepingPath);
  }
}
