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
package com.expediagroup.beekeeper.metadata.cleanup.service;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DISABLED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

import io.micrometer.core.annotation.Timed;

import com.expediagroup.beekeeper.cleanup.metadata.CleanerClient;
import com.expediagroup.beekeeper.cleanup.metadata.CleanerClientFactory;
import com.expediagroup.beekeeper.cleanup.service.DisableTablesService;
import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;

public class MetadataDisableTablesService implements DisableTablesService {

  private static final Logger log = LoggerFactory.getLogger(MetadataDisableTablesService.class);

  private final CleanerClientFactory cleanerClientFactory;
  private final HousekeepingMetadataRepository housekeepingMetadataRepository;
  private final boolean dryRunEnabled;

  public MetadataDisableTablesService(
      CleanerClientFactory cleanerClientFactory,
      HousekeepingMetadataRepository housekeepingMetadataRepository,
      boolean dryRunEnabled) {
    this.cleanerClientFactory = cleanerClientFactory;
    this.housekeepingMetadataRepository = housekeepingMetadataRepository;
    this.dryRunEnabled = dryRunEnabled;
  }

  @Override
  @Timed("metadata-disable-tables-job")
  @Transactional
  public void disable() {
    List<HousekeepingMetadata> activeTables = housekeepingMetadataRepository.findActiveTables();
    activeTables.forEach(this::handleTable);
  }

  private void handleTable(HousekeepingMetadata table) {
    if (!tableHasBeekeeperProperty(table)) {
      log.info("Disabling table {}.{}", table.getDatabaseName(), table.getTableName());
      if (!dryRunEnabled) {
        housekeepingMetadataRepository
            .deleteScheduledOrFailedPartitionRecordsForTable(table.getDatabaseName(), table.getTableName());
        table.setHousekeepingStatus(DISABLED);
        housekeepingMetadataRepository.save(table);
      }
    }
  }

  private boolean tableHasBeekeeperProperty(HousekeepingMetadata metadata) {
    try (CleanerClient client = cleanerClientFactory.newInstance()) {
      Map<String, String> properties = client.getTableProperties(metadata.getDatabaseName(), metadata.getTableName());
      String beekeeperProperty = properties.get(EXPIRED.getTableParameterName());
      return "true".equals(beekeeperProperty);
    } catch (IOException e) {
      throw new BeekeeperException("Can't instantiate cleaner client.", e);
    }
  }
}
