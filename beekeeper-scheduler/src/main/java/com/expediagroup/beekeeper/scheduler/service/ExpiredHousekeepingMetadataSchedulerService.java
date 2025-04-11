/**
 * Copyright (C) 2019-2025 Expedia, Inc.
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

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.FAILED_TO_SCHEDULE;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.model.PeriodDuration;
import com.expediagroup.beekeeper.core.monitoring.TimedTaggable;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;
import com.expediagroup.beekeeper.core.service.BeekeeperHistoryService;
import com.expediagroup.beekeeper.scheduler.hive.HiveClient;
import com.expediagroup.beekeeper.scheduler.hive.HiveClientFactory;
import com.expediagroup.beekeeper.scheduler.hive.PartitionInfo;

@Service
public class ExpiredHousekeepingMetadataSchedulerService implements SchedulerService {

  private static final Logger log = LoggerFactory.getLogger(ExpiredHousekeepingMetadataSchedulerService.class);
  private static final LifecycleEventType LIFECYCLE_EVENT_TYPE = EXPIRED;

  private final HousekeepingMetadataRepository housekeepingMetadataRepository;
  private final BeekeeperHistoryService beekeeperHistoryService;
  private final HiveClientFactory hiveClientFactory;
  private final Clock clock;

  @Autowired
  public ExpiredHousekeepingMetadataSchedulerService(HousekeepingMetadataRepository housekeepingMetadataRepository,
      BeekeeperHistoryService beekeeperHistoryService, HiveClientFactory hiveClientFactory) {
    this.housekeepingMetadataRepository = housekeepingMetadataRepository;
    this.beekeeperHistoryService = beekeeperHistoryService;
    this.hiveClientFactory = hiveClientFactory;
    this.clock = Clock.systemDefaultZone();
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
      log.info("Successfully scheduled {}", housekeepingMetadata);
      saveHistory(housekeepingMetadata, SCHEDULED);
    } catch (Exception e) {
      saveHistory(housekeepingMetadata, FAILED_TO_SCHEDULE);
      throw new BeekeeperException(format("Unable to schedule %s", housekeepingMetadata), e);
    }
  }

  private HousekeepingMetadata createOrUpdateHousekeepingMetadata(HousekeepingMetadata housekeepingMetadata) {
    Optional<HousekeepingMetadata> housekeepingMetadataOptional = housekeepingMetadataRepository.findRecordForCleanupByDbTableAndPartitionName(
        housekeepingMetadata.getDatabaseName(), housekeepingMetadata.getTableName(),
        housekeepingMetadata.getPartitionName());

    if (housekeepingMetadataOptional.isEmpty()) {
      handleNewMetadata(housekeepingMetadata);
      return housekeepingMetadata;
    }
    HousekeepingMetadata existingHousekeepingMetadata = housekeepingMetadataOptional.get();
    updateExistingMetadata(existingHousekeepingMetadata, housekeepingMetadata);

    if (existingHousekeepingMetadata.getPartitionName() == null) {
      handlerAlterTable(existingHousekeepingMetadata);
    }

    return existingHousekeepingMetadata;
  }

  private void handleNewMetadata(HousekeepingMetadata housekeepingMetadata) {
    if (housekeepingMetadata.getPartitionName() != null) {
      updateTableCleanupTimestamp(housekeepingMetadata);
    } else {
      scheduleTablePartitions(housekeepingMetadata);
    }
  }

  private void handlerAlterTable(HousekeepingMetadata existingHousekeepingMetadata) {
    List<HousekeepingMetadata> scheduledPartitions = housekeepingMetadataRepository.findRecordsForCleanupByDbAndTableName(
        existingHousekeepingMetadata.getDatabaseName(), existingHousekeepingMetadata.getTableName());
    scheduleMissingPartitions(existingHousekeepingMetadata, scheduledPartitions);
    updateTableCleanupTimestampToMax(existingHousekeepingMetadata);
    if (isActionableUpdate(existingHousekeepingMetadata, scheduledPartitions)) {
      updateScheduledPartitions(existingHousekeepingMetadata, scheduledPartitions);
    }
  }

  /**
   * When an alteration to the table occurs pre-existing partitions should be scheduled.
   *
   * @param tableMetadata Entity that stored the cleanup delay.
   */
  private void scheduleTablePartitions(HousekeepingMetadata tableMetadata) {
    log.info("Scheduling all partitions for table {}.{}", tableMetadata.getDatabaseName(),
        tableMetadata.getTableName());
    Map<String, String> partitionNamesAndPaths = retrieveTablePartitions(tableMetadata.getDatabaseName(),
        tableMetadata.getTableName());
    schedule(partitionNamesAndPaths, tableMetadata);
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

  private void updateExistingMetadata(HousekeepingMetadata existingMetadata, HousekeepingMetadata newMetadata) {
    existingMetadata.setPath(newMetadata.getPath());
    existingMetadata.setHousekeepingStatus(newMetadata.getHousekeepingStatus());
    existingMetadata.setCleanupDelay(newMetadata.getCleanupDelay());
    existingMetadata.setClientId(newMetadata.getClientId());
  }

  /**
   * Compares all partitions on the table with any that are currently scheduled.
   * If any partitions on the table are missing, they will be scheduled.
   */
  private void scheduleMissingPartitions(HousekeepingMetadata tableMetadata,
      List<HousekeepingMetadata> scheduledPartitions) {
    Map<String, String> unscheduledPartitionNames = findUnscheduledPartitionNames(tableMetadata, scheduledPartitions);
    if (unscheduledPartitionNames.isEmpty()) {
      log.info("All table partitions have already been scheduled.");
      return;
    }
    schedule(unscheduledPartitionNames, tableMetadata);
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

  private boolean isActionableUpdate(HousekeepingMetadata metadata, List<HousekeepingMetadata> scheduledPartitions) {
    if(scheduledPartitions.isEmpty()) {
      return false;
    }
    PeriodDuration tableCleanupDelay = metadata.getCleanupDelay();

    HousekeepingMetadata scheduledPartition = scheduledPartitions.get(0);
    PeriodDuration metadataCleanupDelay = scheduledPartition.getCleanupDelay();

    return (!tableCleanupDelay.equals(metadataCleanupDelay));
  }

  private void updateScheduledPartitions(HousekeepingMetadata metadata,
      List<HousekeepingMetadata> partitions) {
    log.info("Updating scheduled partitions.");
    partitions.forEach(partition -> {
      partition.setCleanupDelay(metadata.getCleanupDelay());
      housekeepingMetadataRepository.save(partition);
      beekeeperHistoryService.saveHistory(partition, SCHEDULED);
    });
  }

  private Map<String, String> findUnscheduledPartitionNames(HousekeepingMetadata tableMetadata,
      List<HousekeepingMetadata> scheduledPartitions) {
    Map<String, String> tablePartitionNamesAndPaths = retrieveTablePartitions(tableMetadata.getDatabaseName(),
        tableMetadata.getTableName());

    Set<String> scheduledPartitionNames = scheduledPartitions.stream()
        .map(HousekeepingMetadata::getPartitionName)
        .collect(Collectors.toSet());

    return tablePartitionNamesAndPaths.entrySet().stream()
        .filter(entry -> !scheduledPartitionNames.contains(entry.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Map<String, String> retrieveTablePartitions(String database, String tableName) {
    try (HiveClient hiveClient = hiveClientFactory.newInstance()) {
      Map<String, PartitionInfo> partitionInfo = hiveClient.getTablePartitionsInfo(database, tableName);
      return partitionInfo.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getPath()));
    }
  }

  private void schedule(Map<String, String> partitionNamesAndPaths, HousekeepingMetadata tableMetadata) {
    partitionNamesAndPaths.forEach((partitionName, path) -> {
      HousekeepingMetadata partitionMetadata = createNewMetadata(tableMetadata, partitionName, path);

      housekeepingMetadataRepository.save(partitionMetadata);
      beekeeperHistoryService.saveHistory(partitionMetadata, SCHEDULED);
    });
    log.info("Scheduled {} partitions for table {}.{}", partitionNamesAndPaths.size(), tableMetadata.getDatabaseName(),
        tableMetadata.getTableName());
  }

  private HousekeepingMetadata createNewMetadata(HousekeepingMetadata tableMetadata, String partitionName,
      String path) {
    return HousekeepingMetadata
        .builder()
        .housekeepingStatus(SCHEDULED)
        .creationTimestamp(LocalDateTime.now(clock))
        .cleanupDelay(tableMetadata.getCleanupDelay())
        .lifecycleType(LIFECYCLE_EVENT_TYPE.toString())
        .path(path)
        .databaseName(tableMetadata.getDatabaseName())
        .tableName(tableMetadata.getTableName())
        .partitionName(partitionName)
        .build();
  }

  private void saveHistory(HousekeepingMetadata housekeepingMetadata, HousekeepingStatus status) {
    beekeeperHistoryService.saveHistory(housekeepingMetadata, status);
  }
}
