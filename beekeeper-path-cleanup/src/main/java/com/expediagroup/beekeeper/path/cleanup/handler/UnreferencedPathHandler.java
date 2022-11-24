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
package com.expediagroup.beekeeper.path.cleanup.handler;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.time.LocalDateTime;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.stereotype.Component;

import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;

@Component
public class UnreferencedPathHandler extends GenericPathHandler {

  private final HousekeepingPathRepository housekeepingPathRepository;

  @Autowired
  public UnreferencedPathHandler(
      HousekeepingPathRepository housekeepingPathRepository,
      @Qualifier("s3PathCleaner") PathCleaner pathCleaner
  ) {
    super(housekeepingPathRepository, pathCleaner, UNREFERENCED);
    this.housekeepingPathRepository = housekeepingPathRepository;
  }

  @Override
  public Slice<HousekeepingPath> findRecordsToClean(LocalDateTime instant, Pageable pageable) {
    return housekeepingPathRepository.findRecordsForCleanupByModifiedTimestamp(instant, pageable);
  }
}
