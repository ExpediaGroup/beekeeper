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
package com.expediagroup.beekeeper.scheduler.service;

import static java.lang.String.format;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.time.LocalDateTime;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.monitoring.TimedTaggable;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;

@Service
public class ExpiredHousekeepingMetadataSchedulerService implements SchedulerService {

  private static final Logger log = LoggerFactory.getLogger(ExpiredHousekeepingMetadataSchedulerService.class);
  private static final LifecycleEventType LIFECYCLE_EVENT_TYPE = EXPIRED;

  private final HousekeepingMetadataRepository housekeepingMetadataRepository;

  @Autowired
  public ExpiredHousekeepingMetadataSchedulerService(HousekeepingMetadataRepository housekeepingMetadataRepository) {
    this.housekeepingMetadataRepository = housekeepingMetadataRepository;
  }

  @Override
  public LifecycleEventType getLifecycleEventType() {
    return LIFECYCLE_EVENT_TYPE;
  }

  @Override
  @TimedTaggable("metadata-scheduled")
  public void scheduleForHousekeeping(HousekeepingEntity housekeepingEntity) {
    HousekeepingMetadata housekeepingMetadata = createOrUpdateHousekeepingMetadata(
        (HousekeepingMetadata) housekeepingEntity);
    try {
      housekeepingMetadataRepository.save(housekeepingMetadata);
      log.info(format("Successfully scheduled %s", housekeepingMetadata));
    } catch (Exception e) {
      throw new BeekeeperException(format("Unable to schedule %s", housekeepingMetadata), e);
    }
  }

  private HousekeepingMetadata createOrUpdateHousekeepingMetadata(HousekeepingMetadata housekeepingMetadata) {
    Optional<HousekeepingMetadata> housekeepingMetadataOptional = housekeepingMetadataRepository.findRecordForCleanupByDbTableAndPartitionName(
        housekeepingMetadata.getDatabaseName(), housekeepingMetadata.getTableName(),
        housekeepingMetadata.getPartitionName());

    if (housekeepingMetadataOptional.isEmpty()) {
      if (housekeepingMetadata.getPartitionName() != null) {
        updateTableCleanupTimestamp(housekeepingMetadata);
      }
      return housekeepingMetadata;
    }

    HousekeepingMetadata existingHousekeepingMetadata = housekeepingMetadataOptional.get();
    existingHousekeepingMetadata.setPath(housekeepingMetadata.getPath());
    existingHousekeepingMetadata.setHousekeepingStatus(housekeepingMetadata.getHousekeepingStatus());
    existingHousekeepingMetadata.setCleanupDelay(housekeepingMetadata.getCleanupDelay());
    existingHousekeepingMetadata.setClientId(housekeepingMetadata.getClientId());

    if (isPartitionedTable(housekeepingMetadata)) {
      updateTableCleanupTimestampToMax(existingHousekeepingMetadata);
    }

    return existingHousekeepingMetadata;
  }

  /**
   * When the cleanup delay of a table with partitions is altered, the delay should be updated but the cleanup
   * timestamp should be the max timestamp of any of the partitions which the table has.
   *
   * e.g. if the cleanup delay was 10 but now its being updated to 2, the cleanup timestamp should match any partition
   * with delay 10 (or above) to prevent premature attempts to cleanup the table.
   *
   * @param housekeepingMetadata
   */
  private void updateTableCleanupTimestampToMax(HousekeepingMetadata housekeepingMetadata) {
    LocalDateTime currentCleanupTimestamp = housekeepingMetadata.getCleanupTimestamp();
    LocalDateTime maxCleanupTimestamp = housekeepingMetadataRepository.findMaximumCleanupTimestampForDbAndTable(
        housekeepingMetadata.getDatabaseName(), housekeepingMetadata.getTableName());

    if (maxCleanupTimestamp != null && maxCleanupTimestamp.isAfter(currentCleanupTimestamp)) {
      log.info("Updating entry for \"{}.{}\". Cleanup timestamp is now \"{}\".", housekeepingMetadata.getDatabaseName(),
          housekeepingMetadata.getTableName(), maxCleanupTimestamp);
      housekeepingMetadata.setCleanupTimestamp(maxCleanupTimestamp);
    }
  }

  /**
   * When a new partition is added to a table, check to see if the new cleanup delay will be later than the current
   * cleanup delay for the table.
   * The cleanup timestamp of a partitioned table should be equivalent to that of the last partition which will be
   * dropped to prevent premature attempts to cleanup the table.
   *
   * @param partitionMetadata
   */
  private void updateTableCleanupTimestamp(HousekeepingMetadata partitionMetadata) {
    HousekeepingMetadata tableMetadata = housekeepingMetadataRepository.findRecordForCleanupByDbTableAndPartitionName(
        partitionMetadata.getDatabaseName(), partitionMetadata.getTableName(), null).get();

    LocalDateTime partitionCleanupTimestamp = partitionMetadata.getCleanupTimestamp();
    LocalDateTime currentCleanupTimestamp = tableMetadata.getCleanupTimestamp();

    if (partitionCleanupTimestamp.isAfter(currentCleanupTimestamp)) {
      log.info("Updating entry for \"{}.{}\". Cleanup timestamp is now \"{}\".", tableMetadata.getDatabaseName(),
          tableMetadata.getTableName(), partitionCleanupTimestamp);
      tableMetadata.setCleanupTimestamp(partitionCleanupTimestamp);
      housekeepingMetadataRepository.save(tableMetadata);
    }
  }

  private boolean isPartitionedTable(HousekeepingMetadata housekeepingMetadata) {
    Long numPartitions = housekeepingMetadataRepository.countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(
        housekeepingMetadata.getDatabaseName(), housekeepingMetadata.getTableName());

    return numPartitions > 0 && housekeepingMetadata.getPartitionName() == null;
  }
}
