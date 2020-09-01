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

import static org.apache.commons.lang.math.NumberUtils.LONG_ZERO;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DELETED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.FAILED;

import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import com.expediagroup.beekeeper.cleanup.metadata.MetadataCleaner;
import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;

public class ExpiredMetadataHandler implements MetadataHandler {

  private final Logger log = LoggerFactory.getLogger(ExpiredMetadataHandler.class);

  private final HousekeepingMetadataRepository housekeepingMetadataRepository;
  private final MetadataCleaner metadataCleaner;
  private final PathCleaner pathCleaner;

  public ExpiredMetadataHandler(HousekeepingMetadataRepository housekeepingMetadataRepository,
      MetadataCleaner metadataCleaner, PathCleaner pathCleaner) {
    this.housekeepingMetadataRepository = housekeepingMetadataRepository;
    this.metadataCleaner = metadataCleaner;
    this.pathCleaner = pathCleaner;
  }

  @Override
  public Page<HousekeepingMetadata> findRecordsToClean(LocalDateTime instant, Pageable pageable) {
    return housekeepingMetadataRepository.findRecordsForCleanupByModifiedTimestamp(instant, pageable);
  }

  /**
   * Cleans up the HousekeepingMetadata records which have expired.
   *
   * @param housekeepingMetadata Record to cleanup
   * @param instant Instant the cleanup is happening
   * @param dryRunEnabled
   * @implNote HousekeepingMetadata records are not updated in dry-run mode.
   */
  @Override
  public void cleanupMetadata(HousekeepingMetadata housekeepingMetadata, LocalDateTime instant, boolean dryRunEnabled) {
    if (dryRunEnabled) {
      cleanup(housekeepingMetadata, instant, dryRunEnabled);
    } else {
      cleanupAndUpdate(housekeepingMetadata, instant, dryRunEnabled);
    }
  }

  private void cleanupAndUpdate(HousekeepingMetadata housekeepingMetadata, LocalDateTime instant,
      boolean dryRunEnabled) {
    try {
      boolean deleted = cleanup(housekeepingMetadata, instant, dryRunEnabled);
      if (deleted) {
        updateAttemptsAndStatus(housekeepingMetadata, DELETED);
      }
    } catch (Exception e) {
      updateAttemptsAndStatus(housekeepingMetadata, FAILED);
      log.warn("Unexpected exception when deleting metadata for table \"{}.{}\"",
          housekeepingMetadata.getDatabaseName(),
          housekeepingMetadata.getTableName(), e);
    }
  }

  private boolean cleanup(HousekeepingMetadata housekeepingMetadata, LocalDateTime instant,
      boolean dryRunEnabled) {
    String partitionName = housekeepingMetadata.getPartitionName();
    if (partitionName != null) {
      log.info("Cleaning up partition \"{}\" for table \"{}.{}\".", partitionName,
          housekeepingMetadata.getDatabaseName(), housekeepingMetadata.getTableName());
      cleanupPartition(housekeepingMetadata, metadataCleaner, pathCleaner);
      return true;
    } else {
      Long partitionCount = countPartitionsForDatabaseAndTable(instant, housekeepingMetadata.getDatabaseName(),
          housekeepingMetadata.getTableName(), dryRunEnabled);
      if (partitionCount.equals(LONG_ZERO)) {
        log.info("Cleaning up table \"{}.{}\".",
            housekeepingMetadata.getDatabaseName(), housekeepingMetadata.getTableName());
        cleanUpTable(housekeepingMetadata, metadataCleaner, pathCleaner);
        return true;
      }
    }
    return false;
  }

  private void cleanUpTable(HousekeepingMetadata housekeepingMetadata, MetadataCleaner metadataCleaner,
      PathCleaner pathCleaner) {
    String databaseName = housekeepingMetadata.getDatabaseName();
    String tableName = housekeepingMetadata.getTableName();
    if (metadataCleaner.tableExists(databaseName, tableName)) {
      metadataCleaner.dropTable(housekeepingMetadata);
      pathCleaner.cleanupPath(housekeepingMetadata);
    } else {
      log.info("Cannot drop table \"{}.{}\". Table does not exist.", databaseName, tableName);
    }
  }

  private void cleanupPartition(HousekeepingMetadata housekeepingMetadata, MetadataCleaner metadataCleaner,
      PathCleaner pathCleaner) {
    String databaseName = housekeepingMetadata.getDatabaseName();
    String tableName = housekeepingMetadata.getTableName();
    if (metadataCleaner.tableExists(databaseName, tableName)) {
      boolean partitionDeleted = metadataCleaner.dropPartition(housekeepingMetadata);
      if (partitionDeleted) {
        pathCleaner.cleanupPath(housekeepingMetadata);
      }
    } else {
      log.info("Cannot drop partition \"{}\" from table \"{}.{}\". Table does not exist.",
          housekeepingMetadata.getPartitionName(), databaseName, tableName);
    }
  }

  private void updateAttemptsAndStatus(HousekeepingMetadata housekeepingMetadata, HousekeepingStatus status) {
    housekeepingMetadata.setCleanupAttempts(housekeepingMetadata.getCleanupAttempts() + 1);
    housekeepingMetadata.setHousekeepingStatus(status);
    housekeepingMetadataRepository.save(housekeepingMetadata);
  }

  private Long countPartitionsForDatabaseAndTable(LocalDateTime instant, String databaseName, String tableName,
      boolean dryRunEnabled) {
    if (dryRunEnabled) {
      return housekeepingMetadataRepository.countRecordsForDryRunWherePartitionIsNotNullOrExpired(instant, databaseName,
          tableName);
    }
    return housekeepingMetadataRepository.countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(databaseName,
        tableName);
  }
}
