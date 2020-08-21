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
package com.expediagroup.beekeeper.scheduler.apiary.service;

import static java.lang.String.format;

import java.io.IOException;
import java.util.EnumMap;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.BeekeeperEventReader;
import com.expediagroup.beekeeper.scheduler.apiary.model.BeekeeperEvent;
import com.expediagroup.beekeeper.scheduler.service.SchedulerService;

@Component
public class PathSchedulerApiary {

  private static final Logger log = LoggerFactory.getLogger(PathSchedulerApiary.class);
  private final BeekeeperEventReader beekeeperEventReader;
  private final EnumMap<LifecycleEventType, SchedulerService> schedulerServiceMap;

  @Autowired
  public PathSchedulerApiary(
      BeekeeperEventReader beekeeperEventReader,
      EnumMap<LifecycleEventType, SchedulerService> schedulerServiceMap) {
    this.beekeeperEventReader = beekeeperEventReader;
    this.schedulerServiceMap = schedulerServiceMap;
  }

  @Transactional
  public void scheduleBeekeeperEvent() {
    Optional<BeekeeperEvent> pathToBeScheduled = beekeeperEventReader.read();
    if (pathToBeScheduled.isEmpty()) {
      return;
    }
    BeekeeperEvent beekeeperEvent = pathToBeScheduled.get();

    log.info("Housekeeping event created. Paths are: ");
    List<HousekeepingPath> paths = beekeeperEvent.getHousekeepingPaths();

    for (HousekeepingPath path : paths) {
      try {
        LifecycleEventType pathEventType = LifecycleEventType.valueOf(path.getLifecycleType());
        SchedulerService scheduler = schedulerServiceMap.get(pathEventType);
        log.info("Path to be scheduled: " + path.getPath());
        scheduler.scheduleForHousekeeping(path);
      } catch (Exception e) {
        log.info("EXCEPTION thrown: ", e);
        throw new BeekeeperException(
            format("Unable to schedule path '%s' for deletion, this message will go back on the queue", path.getPath()),
            e);
      }
    }

    beekeeperEventReader.delete(beekeeperEvent);
  }

  public void close() throws IOException {
    beekeeperEventReader.close();
  }
}
