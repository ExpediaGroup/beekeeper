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
import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;

import io.micrometer.core.annotation.Timed;

import com.expediagroup.beekeeper.cleanup.service.CleanupService;
import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.metadata.cleanup.handler.GenericMetadataHandler;

public class PagingMetadataCleanupService implements CleanupService {

  private final List<GenericMetadataHandler> metadataHandlers;
  private final boolean dryRunEnabled;
  private final int pageSize;

  public PagingMetadataCleanupService(
      List<GenericMetadataHandler> metadataHandlers,
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

  @Transactional
  private void pagingCleanup(GenericMetadataHandler handler, Instant referenceTime) {
    Pageable pageable = PageRequest.of(0, pageSize).first();

    LocalDateTime instant = LocalDateTime.ofInstant(referenceTime, ZoneOffset.UTC);
    Page<HousekeepingMetadata> page = handler.findRecordsToClean(instant, pageable);

    while (!page.getContent().isEmpty()) {
      pageable = handler.processPage(pageable, instant, page, dryRunEnabled);
      page = handler.findRecordsToClean(instant, pageable);
    }
  }

}
