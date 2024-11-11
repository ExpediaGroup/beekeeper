/**
 * Copyright (C) 2019-2024 Expedia, Inc.
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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.scheduler.apiary.filter.IcebergTableListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.BeekeeperEventReader;
import com.expediagroup.beekeeper.scheduler.apiary.model.BeekeeperEvent;
import com.expediagroup.beekeeper.scheduler.service.SchedulerService;

// class scheduling housekeeping events based on Beekeeper events

/**
 * original flow of the class:
 * read beekeperevent → for every housekeepingentity → determine lifecycleeventtype → schedule housekeeping → delete event
 *
 * updated flow of the class:
 * read beekeperevent → extract messageevent → extract listenerevent → determine lifecycleeventtype →
 *     if iceberg table → ignore & delete event
 *     else → for every housekeepingentity → determine lifecycleeventtype → schedule housekeeping → delete event
 *
 */

@Component
public class SchedulerApiary {

  private final BeekeeperEventReader beekeeperEventReader;
  private final EnumMap<LifecycleEventType, SchedulerService> schedulerServiceMap;
  private final IcebergTableListenerEventFilter icebergTableListenerEventFilter;

  @Autowired
  public SchedulerApiary(
      BeekeeperEventReader beekeeperEventReader,
      EnumMap<LifecycleEventType, SchedulerService> schedulerServiceMap,
      IcebergTableListenerEventFilter icebergTableListenerEventFilter
  ) {
    this.beekeeperEventReader = beekeeperEventReader;
    this.schedulerServiceMap = schedulerServiceMap;
    this.icebergTableListenerEventFilter = icebergTableListenerEventFilter;
  }

  @Transactional
  public void scheduleBeekeeperEvent() {
    Optional<BeekeeperEvent> beekeeperEventOptional = beekeeperEventReader.read();
    if (beekeeperEventOptional.isEmpty()) {
      return;
    }

    // extract the messageEvent from beekeeperEvent so we can extract ListenerEvent
    // to provide information about the event inc table params to pass to icebergTableListenerEventFilter
    BeekeeperEvent beekeeperEvent = beekeeperEventOptional.get();
    MessageEvent messageEvent = beekeeperEvent.getMessageEvent();
    ListenerEvent listenerEvent = messageEvent.getEvent();

    List<HousekeepingEntity> housekeepingEntities = beekeeperEvent.getHousekeepingEntities();

    // we didn't check if housekeepingEntities was empty before, might lead to silent failures?
    if (housekeepingEntities.isEmpty()) {
      throw new BeekeeperException("No housekeeping entities found in the event");
    }
    LifecycleEventType lifecycleEventType = LifecycleEventType.valueOf(housekeepingEntities.get(0).getLifecycleType());

    // apply Iceberg table filter
    if (icebergTableListenerEventFilter.isFiltered(listenerEvent, lifecycleEventType)) {
      // delete event and skip processing
      beekeeperEventReader.delete(beekeeperEvent);
      return;
    }

    for (HousekeepingEntity entity : housekeepingEntities) {
      try {
        LifecycleEventType eventType = LifecycleEventType.valueOf(entity.getLifecycleType());
        SchedulerService scheduler = schedulerServiceMap.get(eventType);
        scheduler.scheduleForHousekeeping(entity);
      } catch (Exception e) {
        throw new BeekeeperException(format(
            "Unable to schedule %s deletion for entity, this message will go back on the queue",
            entity.getLifecycleType()), e);
      }
    }

    beekeeperEventReader.delete(beekeeperEvent);
  }

  /**
   * Was thinking I can extract some of the logic into this to simplify main method, thoughts?
   *
   *   private LifecycleEventType getLifecycleEventType(BeekeeperEvent beekeeperEvent) {
   *     List<HousekeepingEntity> housekeepingEntities = beekeeperEvent.getHousekeepingEntities();
   *     if (!housekeepingEntities.isEmpty()) {
   *       String lifecycleType = housekeepingEntities.get(0).getLifecycleType();
   *       return LifecycleEventType.valueOf(lifecycleType);
   *     }
   *     // Handle the case where there are no housekeeping entities
   *     throw new BeekeeperException("No housekeeping entities found in the event");
   *   }
   *
   */

  public void close() throws IOException {
    beekeeperEventReader.close();
  }
}
