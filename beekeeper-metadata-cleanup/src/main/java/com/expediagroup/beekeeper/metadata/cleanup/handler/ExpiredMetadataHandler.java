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
package com.expediagroup.beekeeper.metadata.cleanup.handler;

import static org.apache.commons.lang.math.NumberUtils.LONG_ZERO;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DELETED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.FAILED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.FAILED_TO_DELETE;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SKIPPED;

import java.time.LocalDateTime;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;

import com.expediagroup.beekeeper.cleanup.metadata.CleanerClient;
import com.expediagroup.beekeeper.cleanup.metadata.CleanerClientFactory;
import com.expediagroup.beekeeper.cleanup.metadata.MetadataCleaner;
import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.core.error.BeekeeperIcebergException;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;
import com.expediagroup.beekeeper.core.service.BeekeeperHistoryService;
import com.expediagroup.beekeeper.core.validation.S3PathValidator;

public class ExpiredMetadataHandler implements MetadataHandler {

  private final Logger log = LoggerFactory.getLogger(ExpiredMetadataHandler.class);
  private static final String TABLE_DELETION_PROPERTY = "beekeeper.expired.data.table.deletion.enabled";

  private final CleanerClientFactory cleanerClientFactory;
  private final HousekeepingMetadataRepository housekeepingMetadataRepository;
  private final MetadataCleaner metadataCleaner;
  private final PathCleaner pathCleaner;
  private final BeekeeperHistoryService historyService;

  public ExpiredMetadataHandler(
      CleanerClientFactory cleanerClientFactory,
      HousekeepingMetadataRepository housekeepingMetadataRepository,
      MetadataCleaner metadataCleaner,
      PathCleaner pathCleaner,
      BeekeeperHistoryService historyService) {
    this.cleanerClientFactory = cleanerClientFactory;
    this.housekeepingMetadataRepository = housekeepingMetadataRepository;
    this.metadataCleaner = metadataCleaner;
    this.pathCleaner = pathCleaner;
    this.historyService = historyService;
  }

  @Override
  public Slice<HousekeepingMetadata> findRecordsToClean(LocalDateTime instant, Pageable pageable) {
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
    try (CleanerClient client = cleanerClientFactory.newInstance()) {
      boolean deleted = cleanup(client, housekeepingMetadata, instant, dryRunEnabled);
      if (deleted && !dryRunEnabled) {
        updateAttemptsAndStatus(housekeepingMetadata, DELETED);
        saveHistory(housekeepingMetadata, DELETED, dryRunEnabled);
      }
    } catch (BeekeeperIcebergException e) {
      updateAttemptsAndStatus(housekeepingMetadata, SKIPPED);
      String logMessage = String.format("Table \"%s.%s\" is skipped because it is iceberg or could not be identified.",
          housekeepingMetadata.getDatabaseName(), housekeepingMetadata.getTableName());
      log.info(logMessage);
      log.debug(logMessage, e);
    } catch (Exception e) {
      updateAttemptsAndStatus(housekeepingMetadata, FAILED);
      String logMessage = String.format("Unexpected exception when deleting metadata for table \"%s.%s\".",
          housekeepingMetadata.getDatabaseName(), housekeepingMetadata.getTableName());
      log.info(logMessage);
      log.debug(logMessage, e);
      saveHistory(housekeepingMetadata, FAILED_TO_DELETE, dryRunEnabled);
      log
          .warn("Unexpected exception when deleting metadata for table \"{}.{}\"",
              housekeepingMetadata.getDatabaseName(), housekeepingMetadata.getTableName(), e);
    }
  }

  private boolean cleanup(
      CleanerClient client,
      HousekeepingMetadata housekeepingMetadata,
      LocalDateTime instant,
      boolean dryRunEnabled) {
    String partitionName = housekeepingMetadata.getPartitionName();
    if (partitionName != null) {
      return cleanupPartition(client, housekeepingMetadata, dryRunEnabled);
    } else {
      Long partitionCount = countPartitionsForDatabaseAndTable(instant, housekeepingMetadata.getDatabaseName(),
          housekeepingMetadata.getTableName(), dryRunEnabled);
      if (partitionCount.equals(LONG_ZERO)) {
        return cleanUpTable(client, housekeepingMetadata, dryRunEnabled);
      }
    }
    return false;
  }

  private boolean cleanUpTable(CleanerClient client, HousekeepingMetadata housekeepingMetadata, boolean dryRunEnabled) {
    if (!S3PathValidator.validTablePath(housekeepingMetadata.getPath())) {
      log.warn("Will not clean up table path \"{}\" because it is not valid.", housekeepingMetadata.getPath());
      updateStatus(housekeepingMetadata, SKIPPED, dryRunEnabled);
      saveHistory(housekeepingMetadata, SKIPPED, dryRunEnabled);
      return false;
    }
    String databaseName = housekeepingMetadata.getDatabaseName();
    String tableName = housekeepingMetadata.getTableName();

    if (!isTableDeletionEnabled(client, databaseName, tableName)) {
      log.info("Skipping table drop for '{}.{}' as table deletion is disabled.", databaseName, tableName);
      updateAttemptsAndStatus(housekeepingMetadata, SKIPPED);
      saveHistory(housekeepingMetadata, SKIPPED, dryRunEnabled);
      return false;
    }

    log.info("Cleaning up metadata for \"{}.{}\"", databaseName, tableName);
    if (metadataCleaner.tableExists(client, databaseName, tableName)) {
      metadataCleaner.dropTable(housekeepingMetadata, client);
      pathCleaner.cleanupPath(housekeepingMetadata);
    } else {
      log.info("Cannot drop table \"{}.{}\". Table does not exist.", databaseName, tableName);
    }
    return true;
  }

  private boolean cleanupPartition(
      CleanerClient client,
      HousekeepingMetadata housekeepingMetadata,
      boolean dryRunEnabled) {
    if (!S3PathValidator.validPartitionPath(housekeepingMetadata.getPath())) {
      log.warn("Will not clean up partition path \"{}\" because it is not valid.", housekeepingMetadata.getPath());
      updateStatus(housekeepingMetadata, SKIPPED, dryRunEnabled);
      saveHistory(housekeepingMetadata, SKIPPED, dryRunEnabled);
      return false;
    }
    String databaseName = housekeepingMetadata.getDatabaseName();
    String tableName = housekeepingMetadata.getTableName();
    log.info("Cleaning up metadata for \"{}.{}\"", databaseName, tableName);
    if (metadataCleaner.tableExists(client, databaseName, tableName)) {
      boolean partitionDeleted = metadataCleaner.dropPartition(housekeepingMetadata, client);
      if (partitionDeleted) {
        pathCleaner.cleanupPath(housekeepingMetadata);
      }
    } else {
      log
          .info("Cannot drop partition \"{}\" from table \"{}.{}\". Table does not exist.",
              housekeepingMetadata.getPartitionName(), databaseName, tableName);
    }
    return true;
  }

  private void updateAttemptsAndStatus(HousekeepingMetadata housekeepingMetadata, HousekeepingStatus status) {
    housekeepingMetadata.setCleanupAttempts(housekeepingMetadata.getCleanupAttempts() + 1);
    housekeepingMetadata.setHousekeepingStatus(status);
    housekeepingMetadataRepository.save(housekeepingMetadata);
  }

  private void updateStatus(
      HousekeepingMetadata housekeepingMetadata,
      HousekeepingStatus status,
      boolean dryRunEnabled) {
    if (dryRunEnabled) {
      return;
    }
    housekeepingMetadata.setHousekeepingStatus(status);
    housekeepingMetadataRepository.save(housekeepingMetadata);
  }

  private Long countPartitionsForDatabaseAndTable(
      LocalDateTime instant,
      String databaseName,
      String tableName,
      boolean dryRunEnabled) {
    if (dryRunEnabled) {
      return housekeepingMetadataRepository
          .countRecordsForDryRunWherePartitionIsNotNullOrExpired(instant, databaseName, tableName);
    }
    return housekeepingMetadataRepository
        .countRecordsForGivenDatabaseAndTableWherePartitionIsNotNull(databaseName, tableName);
  }

  private void saveHistory(HousekeepingMetadata metadata, HousekeepingStatus housekeepingStatus,
      boolean dryRunEnabled) {
    if (dryRunEnabled) {
      return;
    }
    historyService.saveHistory(metadata, housekeepingStatus);
  }

  private boolean isTableDeletionEnabled(CleanerClient client, String databaseName, String tableName) {
    Map<String, String> tableProperties = client.getTableProperties(databaseName, tableName);
    String tableDeletionProperty = tableProperties.get(TABLE_DELETION_PROPERTY);
    if (tableDeletionProperty != null) {
      return Boolean.parseBoolean(tableDeletionProperty);
    }
    return false;
  }
}
