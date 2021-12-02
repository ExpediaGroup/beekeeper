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
package com.expediagroup.beekeeper.metadata.cleanup.service;

import static java.lang.String.format;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;

import io.micrometer.core.annotation.Timed;

import com.expediagroup.beekeeper.cleanup.service.CleanupService;
import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.metadata.cleanup.handler.MetadataHandler;

public class PagingMetadataCleanupService implements CleanupService {

  private static final Logger log = LoggerFactory.getLogger(PagingMetadataCleanupService.class);

  private final List<MetadataHandler> metadataHandlers;
  private final boolean dryRunEnabled;
  private final int pageSize;

  public PagingMetadataCleanupService(
      List<MetadataHandler> metadataHandlers,
      int pageSize,
      boolean dryRunEnabled) {
    this.metadataHandlers = metadataHandlers;
    this.pageSize = pageSize;
    this.dryRunEnabled = dryRunEnabled;
  }

  @Override
  @Timed("metadata-cleanup-job")
  public void cleanUp(Instant referenceTime) {
    try {
      metadataHandlers.forEach(handler -> pagingCleanup(handler, referenceTime));
    } catch (Exception e) {
      throw new BeekeeperException(format("Metadata cleanup failed for instant %s", referenceTime.toString()), e);
    }
  }

  /**
   * @param handler MetadataHandler which will cleanup the records
   * @param referenceTime Instant at which the cleanup is taking place
   * @implNote No updates occur to records during dry runs.
   */
  @Transactional
  private void pagingCleanup(MetadataHandler handler, Instant referenceTime) {
    Pageable pageable = PageRequest.of(0, pageSize).first();

    LocalDateTime instant = LocalDateTime.ofInstant(referenceTime, ZoneOffset.UTC);
    Page<HousekeepingMetadata> page = handler.findRecordsToClean(instant, pageable);
    Map<String, Boolean> tableToProperty = new HashMap<>();

    while (!page.getContent().isEmpty()) {
      pageable = processPage(handler, pageable, instant, page, tableToProperty);
      page = handler.findRecordsToClean(instant, pageable);
    }
  }

  private Pageable processPage(MetadataHandler handler, Pageable pageable, LocalDateTime instant,
      Page<HousekeepingMetadata> page, Map<String, Boolean> tableToProperty) {
    Set<String> tablesToBeDisabled = new HashSet<>();
    page.getContent()
        .forEach(
            metadata -> processRecord(handler, instant, metadata, tableToProperty, tablesToBeDisabled));
    tablesToBeDisabled.forEach(table -> handleTable(handler, table));
    if (dryRunEnabled) {
      return pageable.next();
    }
    return pageable;
  }

  private void handleTable(MetadataHandler handler, String table) {
    if (dryRunEnabled) {
      log.info("Disabling table {}", table);
    } else {
      handler.disableTable(table.split("\\.")[0], table.split("\\.")[1]);
    }
  }

  private void processRecord(MetadataHandler handler, LocalDateTime instant,
      HousekeepingMetadata metadata, Map<String, Boolean> tableToProperty, Set<String> tablesToBeDisabled) {
    String tableName = metadata.getDatabaseName() + "." + metadata.getTableName();
    if (tablesToBeDisabled.contains(tableName)) {
      return;
    }
    Boolean tableEnabled = tableToProperty.get(tableName);
    if (tableEnabled == null) {
      tableEnabled = handler.tableHasBeekeeperProperty(metadata);
      tableToProperty.put(tableName, tableEnabled);
    }
    if (tableEnabled) {
      handler.cleanupMetadata(metadata, instant, dryRunEnabled);
    } else {
      tablesToBeDisabled.add(tableName);
    }
  }
}
