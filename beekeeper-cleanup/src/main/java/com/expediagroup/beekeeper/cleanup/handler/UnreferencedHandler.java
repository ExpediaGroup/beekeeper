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

import java.time.LocalDateTime;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;

@Component
public class UnreferencedHandler extends GenericHandler {

  private static final LifecycleEventType EVENT_TYPE = LifecycleEventType.UNREFERENCED;
  private PathCleaner pathCleaner;

  @Autowired
  public UnreferencedHandler(@Qualifier("s3PathCleaner") PathCleaner pathCleaner) {
    this.pathCleaner = pathCleaner;
  }

  @Override
  public LifecycleEventType getLifecycleType() {
    return EVENT_TYPE;
  }

  @Override
  public PathCleaner getPathCleaner() { return pathCleaner; }

  @Override
  public Page<EntityHousekeepingPath> findRecordsToClean(LocalDateTime instant, Pageable pageable) {
    return housekeepingPathRepository.findRecordsForCleanupByModifiedTimestamp(instant, pageable);
  }
}
