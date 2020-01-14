/**
 * Copyright (C) 2019 Expedia, Inc.
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
package com.expediagroup.beekeeper.cleanup.service;

import static java.lang.String.format;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;

import io.micrometer.core.annotation.Timed;

import com.expediagroup.beekeeper.cleanup.handler.GenericHandler;
import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;

public class PagingCleanupService implements CleanupService {

  private final List<GenericHandler> pathHandlers;
  private final boolean dryRunEnabled;
  private final int pageSize;

  public PagingCleanupService(List<GenericHandler> pathHandlers, int pageSize, boolean dryRunEnabled) {
    this.pathHandlers = pathHandlers;
    this.pageSize = pageSize;
    this.dryRunEnabled = dryRunEnabled;
  }

  @Override
  @Timed("cleanup-job")
  public void cleanUp(Instant referenceTime) {
    try {
      pathHandlers.stream()
          .map(handler -> getAllPagesForHandler(handler, referenceTime))
          .flatMap(List::stream)
          .forEach(pageHelper -> pageHelper.handler.processPage(pageHelper.paths, dryRunEnabled));
    } catch (Exception e) {
      throw new BeekeeperException(format("Cleanup failed for instant %s", referenceTime.toString()), e);
    }
  }

  @Transactional
  private List<PageHelper> getAllPagesForHandler(GenericHandler handler, Instant referenceTime) {
    List<PageHelper> paths = new ArrayList<>();
    Pageable pageable = PageRequest.of(0, pageSize).first();

    LocalDateTime instant = LocalDateTime.ofInstant(referenceTime, ZoneOffset.UTC);
    Page<EntityHousekeepingPath> page = handler.findRecordsToClean(instant, pageable);

    while (!page.getContent().isEmpty()) {
      paths.add(new PageHelper(handler, page.getContent()));
      pageable = pageable.next();
      page = handler.findRecordsToClean(instant, pageable);
    }

    return paths;
  }

  private class PageHelper {

    GenericHandler handler;
    List<EntityHousekeepingPath> paths;

    PageHelper(GenericHandler handler, List<EntityHousekeepingPath> paths) {
      this.handler = handler;
      this.paths = paths;
    }
  }
}
