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

import java.time.LocalDateTime;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import com.expediagroup.beekeeper.core.metadata.MetadataCleaner;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.path.PathCleaner;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;

public abstract class GenericMetadataHandler {

  private final Logger log = LoggerFactory.getLogger(GenericMetadataHandler.class);

  public abstract HousekeepingMetadataRepository getHousekeepingMetadataRepository();

  public abstract LifecycleEventType getLifecycleType();

  public abstract MetadataCleaner getMetadataCleaner();

  public abstract PathCleaner getPathCleaner();

  public abstract Page<HousekeepingMetadata> findRecordsToClean(LocalDateTime instant, Pageable pageable);

  public abstract Page<HousekeepingMetadata> findMatchingRecords(
      String databaseName,
      String tableName,
      Pageable pageable);

  /*-
   * Example entries:
   * no: db  | tblname | path  | part vals
   * 1   db1 | tbl1    | path1 | null             <- unpartitioned
   * 2   db1 | tbl2    | path2 | null             <- partitioned
   * 3   db1 | tbl2    | path2 | year=2020,hour=2 <- partition for above 
   * 
   * Assume the above records are returned, the table entry 1 (unpartitioned) will be dropped. 
   * For entry 2, tbl2 will not be dropped, since the number of records for db1.tbl2 = 2. 
   * For entry 3, this partition will be dropped from the table. Now the repository looks like:
   * 
   * no: db  | tblname | path  | part vals
   * 1   db1 | tbl2    | path2 | null             <- partitioned
   * 
   * and this time, when entry 1 is returned, the number of records for db1.tbl2 = 1, so this table is dropped. 
   * 
   */

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
  public Pageable processPage(Pageable pageable, Page<HousekeepingMetadata> page, boolean dryRunEnabled) {
    List<HousekeepingMetadata> pageContent = page.getContent();

    if (dryRunEnabled) {
      pageContent.forEach(metadata -> cleanupMetadata(metadata, pageable));
      return pageable.next();
    } else {
      pageContent.forEach(metadata -> cleanupContent(metadata, pageable));
      return pageable;
    }
  }

  private boolean cleanupMetadata(HousekeepingMetadata housekeepingMetadata, Pageable pageable) {
    MetadataCleaner metadataCleaner = getMetadataCleaner();
    PathCleaner pathCleaner = getPathCleaner();
    int numberOfRecords = getRecordCountForDatabaseAndTable(housekeepingMetadata.getDatabaseName(),
        housekeepingMetadata.getTableName(), pageable);

    String partitionName = housekeepingMetadata.getPartitionName();
    if (partitionName == null && numberOfRecords == 1) {
      cleanUpTable(housekeepingMetadata, metadataCleaner, pathCleaner);
      return true;
    }
    if (partitionName != null) {
      cleanupPartition(housekeepingMetadata, metadataCleaner, pathCleaner);
      return true;
    }
    return false;
  }

  private void cleanUpTable(
      HousekeepingMetadata housekeepingMetadata,
      MetadataCleaner metadataCleaner,
      PathCleaner pathCleaner) {
    metadataCleaner.cleanupMetadata(housekeepingMetadata);
    pathCleaner.cleanupPath(housekeepingMetadata);
  }

  private void cleanupPartition(
      HousekeepingMetadata housekeepingMetadata,
      MetadataCleaner metadataCleaner,
      PathCleaner pathCleaner) {
    metadataCleaner.cleanupPartition(housekeepingMetadata);
    pathCleaner.cleanupPath(housekeepingMetadata);
  }

  private void cleanupContent(HousekeepingMetadata housekeepingMetadata, Pageable pageable) {
    try {
      log
          .info("Cleaning up metadata for table \"{}.{}\"", housekeepingMetadata.getDatabaseName(),
              housekeepingMetadata.getTableName());
      boolean deleted = cleanupMetadata(housekeepingMetadata, pageable);
      if (deleted) {
        updateAttemptsAndStatus(housekeepingMetadata, HousekeepingStatus.DELETED);
      }
    } catch (Exception e) {
      updateAttemptsAndStatus(housekeepingMetadata, HousekeepingStatus.FAILED);
      log
          .warn("Unexpected exception when deleting metadata for table \"{}.{}\"",
              housekeepingMetadata.getDatabaseName(),
              housekeepingMetadata.getTableName(), e);
    }
  }

  private void updateAttemptsAndStatus(HousekeepingMetadata housekeepingMetadata, HousekeepingStatus status) {
    housekeepingMetadata.setCleanupAttempts(housekeepingMetadata.getCleanupAttempts() + 1);
    housekeepingMetadata.setHousekeepingStatus(status);
    getHousekeepingMetadataRepository().save(housekeepingMetadata);
  }

  private int getRecordCountForDatabaseAndTable(String databaseName, String tableName, Pageable pageable) {
    Page<HousekeepingMetadata> page = findMatchingRecords(databaseName, tableName, pageable);
    int recordCount = 0;
    while (!page.getContent().isEmpty()) {
      recordCount += page.getContent().size();
      page = findMatchingRecords(databaseName, tableName, pageable.next());
    }
    return recordCount;
  }

}
