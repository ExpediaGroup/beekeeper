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
package com.expediagroup.beekeeper.scheduler.apiary.service;

import static java.lang.String.format;

import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.monitoring.TimedTaggable;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;

@Service
public class PathSchedulerService implements SchedulerService {

  private static final PageRequest SINGLE_RECORD = PageRequest.of(0, 1);
  private final HousekeepingPathRepository housekeepingPathRepository;

  @Autowired
  public PathSchedulerService(HousekeepingPathRepository housekeepingPathRepository) {
    this.housekeepingPathRepository = housekeepingPathRepository;
  }

  @Override
  @TimedTaggable("paths-scheduled")
  public void scheduleForHousekeeping(HousekeepingPath cleanUpPath) {
    try {
      housekeepingPathRepository.save((EntityHousekeepingPath) cleanUpPath);
    } catch (Exception e) {
      throw new BeekeeperException(format("Unable to schedule path '%s' for deletion", cleanUpPath.getPath()), e);
    }
  }

  @Override
  @TimedTaggable("paths-scheduled-expiration")
  public void scheduleExpiration(HousekeepingPath expirationPath) {
    try {
      Page<EntityHousekeepingPath> existingPath = housekeepingPathRepository.findExpiredRecordByDatabaseAndTableName(
          expirationPath.getDatabaseName(), expirationPath.getTableName(), SINGLE_RECORD);

      EntityHousekeepingPath pathToSave = (EntityHousekeepingPath) expirationPath;

      if (!existingPath.isEmpty()) {
        EntityHousekeepingPath existing = existingPath.get().collect(Collectors.toList()).get(0);
        existing.setPath(expirationPath.getPath());
        existing.setCleanupDelay(expirationPath.getCleanupDelay());
        existing.setCleanupTimestamp(expirationPath.getCleanupTimestamp());
        pathToSave = existing;
      }

      housekeepingPathRepository.save(pathToSave);
    } catch (Exception e) {
      throw new BeekeeperException(format("Unable to schedule path '%s' for expiration", expirationPath.getPath()), e);
    }
  }
}
