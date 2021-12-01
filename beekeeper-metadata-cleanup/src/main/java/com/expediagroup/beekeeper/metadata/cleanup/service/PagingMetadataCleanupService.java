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
      pageable = processPage(handler, pageable, instant, page, dryRunEnabled, tableToProperty);
      page = handler.findRecordsToClean(instant, pageable);
    }
  }

  private Pageable processPage(MetadataHandler handler, Pageable pageable, LocalDateTime instant,
      Page<HousekeepingMetadata> page,
      boolean dryRunEnabled, Map<String, Boolean> tableToProperty) {
    Set<String> disabledTables = new HashSet<>();
    page.getContent()
        .forEach(metadata -> processRecord(handler, instant, dryRunEnabled, metadata, tableToProperty, disabledTables));
    disabledTables.forEach(table -> handler.handleDisabledTable(table.split("\\.")[0], table.split("\\.")[1]));
    if (dryRunEnabled) {
      return pageable.next();
    }
    return pageable;
  }

  private void processRecord(MetadataHandler handler, LocalDateTime instant, boolean dryRunEnabled,
      HousekeepingMetadata metadata, Map<String, Boolean> tableToProperty, Set<String> disabledTables) {
    String tableName = metadata.getDatabaseName() + "." + metadata.getTableName();
    if (disabledTables.contains(tableName)) {
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
      disabledTables.add(tableName);
    }
  }
}
