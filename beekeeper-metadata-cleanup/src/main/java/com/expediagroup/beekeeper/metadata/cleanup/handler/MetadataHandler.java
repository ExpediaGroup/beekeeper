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

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.metadata.cleanup.cleaner.MetadataCleanup;

public class MetadataHandler {

  private final MetadataCleanup metadataCleanup;

  public MetadataHandler(MetadataCleanup metadataCleanup) {
    this.metadataCleanup = metadataCleanup;
  }

  /**
   * Processes a pageable HouseKeepingMetadata page.
   *
   * @param pageable Pageable to iterate through for dryRun
   * @param page Page to get content from
   * @param dryRunEnabled Dry Run boolean flag
   * @return Pageable to pass to query. In the case of dry runs, this is the next page.
   * @implNote This parent handler expects the child's cleanupMetadata call to update & remove the record from this call
   * such that subsequent DB queries will not return the record. Hence why we only call next during dryRuns
   * where no updates occur.
   * @implNote Note that we only expect pageable.next to be called during a dry run.
   */
  public Pageable processPage(Pageable pageable, LocalDateTime instant, Page<HousekeepingMetadata> page,
      boolean dryRunEnabled) {
    List<HousekeepingMetadata> pageContent = page.getContent();
    if (dryRunEnabled) {
      pageContent.forEach(metadata -> metadataCleanup.cleanupMetadata(metadata, instant, true));
      return pageable.next();
    } else {
      pageContent.forEach(metadata -> metadataCleanup.cleanupContent(metadata, instant, false));
      return pageable;
    }
  }

  public Page<HousekeepingMetadata> findRecordsToClean(LocalDateTime instant, Pageable pageable) {
    return metadataCleanup.findRecordsToClean(instant, pageable);
  }
}
