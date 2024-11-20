/**
 * Copyright (C) 2019-2023 Expedia, Inc.
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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.stereotype.Component;

import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.core.checker.IcebergTableChecker;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;

@Component
public class UnreferencedPathHandler extends GenericPathHandler {

  private final HousekeepingPathRepository housekeepingPathRepository;
  private final IcebergTableChecker icebergTableChecker;

  @Autowired
  public UnreferencedPathHandler(
      HousekeepingPathRepository housekeepingPathRepository,
      @Qualifier("s3PathCleaner") PathCleaner pathCleaner,
      IcebergTableChecker icebergTableChecker) {
    super(housekeepingPathRepository, pathCleaner);
    this.housekeepingPathRepository = housekeepingPathRepository;
    this.icebergTableChecker = icebergTableChecker;
  }

  @Override
  public Slice<HousekeepingPath> findRecordsToClean(LocalDateTime instant, Pageable pageable) {
    return housekeepingPathRepository.findRecordsForCleanup(instant, pageable);
  }

  @Override
  protected void cleanupContent(HousekeepingPath housekeepingPath) { // extends method from generic handler
    String databaseName = housekeepingPath.getDatabaseName();
    String tableName = housekeepingPath.getTableName();

    if (databaseName == null || tableName == null) {
      super.cleanupContent(housekeepingPath); // if no table info delegate process to parent class
      return;
    }

    try {
      if (icebergTableChecker.isIcebergTable(databaseName, tableName)) {
        updateStatus(housekeepingPath, HousekeepingStatus.SKIPPED);
        log.info("Skipped cleanup for Iceberg table: {}.{}", databaseName, tableName);
        return;
      }

      super.cleanupContent(housekeepingPath); // If not an Iceberg table, proceed with the default cleanup logic. is this ok??
    } catch (Exception e) {
      updateAttemptsAndStatus(housekeepingPath, HousekeepingStatus.FAILED); // Mark the path as FAILED
      log.warn("Failed to check if table {}.{} is Iceberg", databaseName, tableName, e);
    }
  }
}
