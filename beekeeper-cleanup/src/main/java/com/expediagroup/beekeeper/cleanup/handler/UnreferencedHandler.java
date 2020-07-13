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
package com.expediagroup.beekeeper.cleanup.handler;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.time.LocalDateTime;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.path.PathCleaner;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;

@Component
public class UnreferencedHandler extends GenericHandler {

  private final PathCleaner pathCleaner;
  private final HousekeepingPathRepository housekeepingPathRepository;

  @Autowired
  public UnreferencedHandler(
      HousekeepingPathRepository housekeepingPathRepository,
      @Qualifier("s3PathCleaner") PathCleaner pathCleaner
  ) {
    this.housekeepingPathRepository = housekeepingPathRepository;
    this.pathCleaner = pathCleaner;
  }

  @Override
  public LifecycleEventType getLifecycleType() {
    return UNREFERENCED;
  }

  @Override
  public PathCleaner getPathCleaner() { return pathCleaner; }

  @Override
  public HousekeepingPathRepository getHousekeepingPathRepository() { return housekeepingPathRepository; }

  @Override
  public Page<HousekeepingPath> findRecordsToClean(LocalDateTime instant, Pageable pageable) {
    return housekeepingPathRepository.findRecordsForCleanupByModifiedTimestamp(instant, pageable);
  }
}
