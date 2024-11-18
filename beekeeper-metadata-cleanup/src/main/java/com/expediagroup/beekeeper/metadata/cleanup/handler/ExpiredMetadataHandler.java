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
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SKIPPED;

import java.time.LocalDateTime;
import java.util.Locale;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;

import com.expediagroup.beekeeper.cleanup.metadata.CleanerClient;
import com.expediagroup.beekeeper.cleanup.metadata.CleanerClientFactory;
import com.expediagroup.beekeeper.cleanup.metadata.MetadataCleaner;
import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;
import com.expediagroup.beekeeper.core.validation.S3PathValidator;

public class ExpiredMetadataHandler implements MetadataHandler {

  private final Logger log = LoggerFactory.getLogger(ExpiredMetadataHandler.class);

  private final CleanerClientFactory cleanerClientFactory;
  private final HousekeepingMetadataRepository housekeepingMetadataRepository;
  private final MetadataCleaner metadataCleaner;
  private final PathCleaner pathCleaner;

  public ExpiredMetadataHandler(
      CleanerClientFactory cleanerClientFactory,
      HousekeepingMetadataRepository housekeepingMetadataRepository,
      MetadataCleaner metadataCleaner,
      PathCleaner pathCleaner) {
    this.cleanerClientFactory = cleanerClientFactory;
    this.housekeepingMetadataRepository = housekeepingMetadataRepository;
    this.metadataCleaner = metadataCleaner;
    this.pathCleaner = pathCleaner;
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
      }
    } catch (Exception e) {
      updateAttemptsAndStatus(housekeepingMetadata, FAILED);
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

    String databaseName = housekeepingMetadata.getDatabaseName();
    String tableName = housekeepingMetadata.getTableName();

    // Check if the table is an Iceberg table, if so, skip cleanup
    if (isIcebergTable(client, databaseName, tableName)) {
      log.info("Skipping cleanup for Iceberg table '{}.{}'.", databaseName, tableName);
      updateStatus(housekeepingMetadata, SKIPPED, dryRunEnabled);
      return false;
    }

    String partitionName = housekeepingMetadata.getPartitionName();
    if (partitionName != null) {
      return cleanupPartition(client, housekeepingMetadata, dryRunEnabled);
    } else {
      Long partitionCount = countPartitionsForDatabaseAndTable(instant, databaseName, tableName, dryRunEnabled);
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
      return false;
    }
    String databaseName = housekeepingMetadata.getDatabaseName();
    String tableName = housekeepingMetadata.getTableName();
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

  private boolean isIcebergTable(CleanerClient client, String databaseName, String tableName) {
    try {
      Map<String, String> tableProperties = client.getTableProperties(databaseName, tableName);
      String tableType = tableProperties != null ? tableProperties.get("table_type") : null;
      String format = tableProperties != null ? tableProperties.get("format") : null;

      boolean isIcebergByType = tableType != null && tableType.toLowerCase().contains("iceberg");
      boolean isIcebergByFormat = format != null && format.toLowerCase().contains("iceberg");

      Map<String, String> storageDescriptor = client.getStorageDescriptorProperties(databaseName, tableName);
      String outputFormat = storageDescriptor != null ? storageDescriptor.get("outputFormat") : null;
      boolean isIcebergByOutputFormat = outputFormat != null && outputFormat.toLowerCase().contains("iceberg");

      return isIcebergByType || isIcebergByFormat || isIcebergByOutputFormat;
    } catch (Exception e) {
      log.warn("Exception while checking if table is Iceberg: {}", e.getMessage());
      return false;
    }
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
}
