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
package com.expediagroup.beekeeper.scheduler.service;

import static java.lang.String.format;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.monitoring.TimedTaggable;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;

@Service
public class UnreferencedHousekeepingPathSchedulerService implements SchedulerService {

  private static final Logger log = LoggerFactory.getLogger(UnreferencedHousekeepingPathSchedulerService.class);
  private static final LifecycleEventType LIFECYCLE_EVENT_TYPE = UNREFERENCED;

  private final HousekeepingPathRepository housekeepingPathRepository;

  @Autowired
  public UnreferencedHousekeepingPathSchedulerService(HousekeepingPathRepository housekeepingPathRepository) {
    this.housekeepingPathRepository = housekeepingPathRepository;
  }

  @Override
  public LifecycleEventType getLifecycleEventType() {
    return LIFECYCLE_EVENT_TYPE;
  }

  @Override
  @TimedTaggable("paths-scheduled")
  public void scheduleForHousekeeping(HousekeepingEntity housekeepingEntity) {
    HousekeepingPath housekeepingPath = (HousekeepingPath) housekeepingEntity;
    try {
      housekeepingPathRepository.save(housekeepingPath);
      log.info(format("Successfully scheduled %s", housekeepingPath));
    } catch (Exception e) {
      throw new BeekeeperException(format("Unable to schedule %s", housekeepingPath), e);
    }
  }
}
