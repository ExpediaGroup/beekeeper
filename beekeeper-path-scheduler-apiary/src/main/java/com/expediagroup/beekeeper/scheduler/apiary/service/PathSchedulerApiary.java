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

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.expediagroup.beekeeper.core.model.LifeCycleEventType.EXPIRED;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.PathEventReader;
import com.expediagroup.beekeeper.scheduler.apiary.model.PathEvents;
import com.expediagroup.beekeeper.scheduler.service.SchedulerService;

@Component
public class PathSchedulerApiary {

  private final PathEventReader pathEventReader;
  private final SchedulerService pathSchedulerService;
  private final HousekeepingPathRepository housekeepingPathRepository;

  @Autowired
  public PathSchedulerApiary(PathEventReader pathEventReader, SchedulerService pathSchedulerService, HousekeepingPathRepository housekeepingPathRepository) {
    this.pathEventReader = pathEventReader;
    this.pathSchedulerService = pathSchedulerService;
    this.housekeepingPathRepository = housekeepingPathRepository;
  }

  public void schedulePath() {
    Optional<PathEvents> pathToBeScheduled = pathEventReader.read();
    if (pathToBeScheduled.isPresent()) {
      PathEvents pathEvent = pathToBeScheduled.get();
      List<HousekeepingPath> paths = pathEvent.getHousekeepingPaths();

      for (HousekeepingPath path : paths) {
        if ( path.getCleanupType().equalsIgnoreCase(EXPIRED.toString()) ) {
          try {
            housekeepingPathRepository.cleanupOldExpiredRows(path.getDatabaseName(), path.getTableName());
          } catch (Exception e) {
              throw new BeekeeperException(format("Unable to cleanup before scheduling '%s' for deletion", path.getPath()), e);
          }
        }
      }

      for (HousekeepingPath path : paths) {
        try {
          pathSchedulerService.scheduleForHousekeeping(path);
        } catch (Exception e) {
          throw new BeekeeperException(format(
            "Unable to schedule path '%s' for deletion, this message will go back on the queue", path.getPath()), e);
        }
      }
      pathEventReader.delete(pathEvent);
    }
  }

  public void close() throws IOException {
    pathEventReader.close();
  }
}
