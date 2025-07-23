/**
 * Copyright (C) 2019-2025 Expedia, Inc.
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

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.FAILED_TO_SCHEDULE;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.monitoring.TimedTaggable;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;
import com.expediagroup.beekeeper.core.service.BeekeeperHistoryService;

@Service
public class UnreferencedHousekeepingPathSchedulerService implements SchedulerService {

  private static final Logger log = LoggerFactory.getLogger(UnreferencedHousekeepingPathSchedulerService.class);
  private static final LifecycleEventType LIFECYCLE_EVENT_TYPE = UNREFERENCED;

  private final HousekeepingPathRepository housekeepingPathRepository;
  private final BeekeeperHistoryService beekeeperHistoryService;

  @Autowired
  public UnreferencedHousekeepingPathSchedulerService(HousekeepingPathRepository housekeepingPathRepository,
      BeekeeperHistoryService beekeeperHistoryService) {
    this.housekeepingPathRepository = housekeepingPathRepository;
    this.beekeeperHistoryService = beekeeperHistoryService;
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
      saveHistory(housekeepingPath, SCHEDULED);
    } catch (Exception e) {
      saveHistory(housekeepingPath, FAILED_TO_SCHEDULE);
      throw new BeekeeperException(format("Unable to schedule %s", housekeepingPath), e);
    }
  }

  private void saveHistory(HousekeepingPath housekeepingPath, HousekeepingStatus status) {
    beekeeperHistoryService.saveHistory(housekeepingPath, status);
  }
}
