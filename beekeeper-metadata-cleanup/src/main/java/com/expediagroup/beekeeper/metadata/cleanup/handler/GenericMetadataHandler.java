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

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;
import com.expediagroup.beekeeper.metadata.cleanup.cleaner.MetadataCleaner;

public abstract class GenericMetadataHandler {

  private final Logger log = LoggerFactory.getLogger(GenericMetadataHandler.class);

  public abstract HousekeepingMetadataRepository getHousekeepingMetadataRepository();

  public abstract LifecycleEventType getLifecycleType();

  public abstract MetadataCleaner getMetadataCleaner();

  public abstract Page<HousekeepingMetadata> findRecordsToClean(LocalDateTime instant, Pageable pageable);

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
      pageContent.forEach(this::cleanUpTable);
      return pageable.next();
    } else {
      pageContent.forEach(this::cleanupContent);
      return pageable;
    }
  }

  private void cleanUpTable(HousekeepingMetadata housekeepingMetadata) {
    MetadataCleaner metadataCleaner = getMetadataCleaner();
    // metadataCleaner.cleanupMetadata(housekeepingMetadata);

    // partVals = housekeepingMetadata.getPartVals();
    // if (partVals == null){
    // // unpartitioned table. Can drop

    // although - could also just be the table entry for the partitioned table.
    // might need to do a check to see if there are any other entries with the same db.tbl name
    // if no - delete. is either unpartitioned or a partitioned table with no partitions

    // metadataCleaner.cleanupMetadata(housekeepingMetadata);
    // } else {
    // // partitioned table. Drop partition

    // drop the partition from the table
    // schedule the partition location for deletion
    // check to see if there are any other entries with the same db.tbl name
    // if no - drop table.

    // TODO
    // schedule the path to be deleted
    // add it to the other table
    // need to make sure that the drop table event is ignored by beekeeper

    // need to know what path should be deleted, not currently in the HousekeepingMetadata table
  }

  private void cleanupContent(HousekeepingMetadata housekeepingMetadata) {
    try {
      log.info("Cleaning up metadata \"{}.{}\"", housekeepingMetadata.getDatabaseName(), housekeepingMetadata.getTableName());
      cleanUpTable(housekeepingMetadata);

      updateAttemptsAndStatus(housekeepingMetadata, HousekeepingStatus.DELETED);

    } catch (Exception e) {
      updateAttemptsAndStatus(housekeepingMetadata, HousekeepingStatus.FAILED);
      log
          .warn("Unexpected exception when deleting metadata \"{}.{}\"", housekeepingMetadata.getDatabaseName(),
              housekeepingMetadata.getTableName(), e);
    }
  }

  private void updateAttemptsAndStatus(HousekeepingMetadata housekeepingMetadata, HousekeepingStatus status) {
    housekeepingMetadata.setCleanupAttempts(housekeepingMetadata.getCleanupAttempts() + 1);
    housekeepingMetadata.setHousekeepingStatus(status);
    getHousekeepingMetadataRepository().save(housekeepingMetadata);
  }
}