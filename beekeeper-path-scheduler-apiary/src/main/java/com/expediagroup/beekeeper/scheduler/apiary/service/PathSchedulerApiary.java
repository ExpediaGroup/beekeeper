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

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.BeekeeperEventReader;
import com.expediagroup.beekeeper.scheduler.apiary.model.BeekeeperEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import org.springframework.transaction.annotation.Transactional;

@Component
public class PathSchedulerApiary {

  private final BeekeeperEventReader beekeeperEventReader;
  private final SchedulerService pathSchedulerService;

  @Autowired
  public PathSchedulerApiary(BeekeeperEventReader beekeeperEventReader, SchedulerService pathSchedulerService) {
    this.beekeeperEventReader = beekeeperEventReader;
    this.pathSchedulerService = pathSchedulerService;
  }

  @Transactional
  public void schedulePath() {
    Optional<BeekeeperEvent> pathToBeScheduled = beekeeperEventReader.read();

    if (!pathToBeScheduled.isPresent()) {
      return;
    }

    BeekeeperEvent pathEvent = pathToBeScheduled.get();
    List<HousekeepingPath> paths = pathEvent.getHousekeepingPaths();

    for(HousekeepingPath path: paths) {
      try {
        if (path.getLifecycleType().equalsIgnoreCase(EXPIRED.toString())) {
          pathSchedulerService.scheduleExpiration(path);
        } else {
          pathSchedulerService.scheduleForHousekeeping(path);
        }
      } catch (Exception e) {
        throw new BeekeeperException(format("Unable to schedule/update path '%s' for deletion, this message will go back on the queue", path.getPath()), e);
      }
    }
    beekeeperEventReader.delete(pathEvent);
  }

  public void close() throws IOException {
    beekeeperEventReader.close();
  }
}
