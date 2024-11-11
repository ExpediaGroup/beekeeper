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
package com.expediagroup.beekeeper.scheduler.apiary.handler;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;

import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.scheduler.apiary.filter.ListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.generator.HousekeepingEntityGenerator;

public class MessageEventHandler {

  private static final Logger log = LoggerFactory.getLogger(MessageEventHandler.class);
  private static final String CLIENT_ID = "apiary-metastore-event";
  private final LifecycleEventType lifecycleEventType;
  private final HousekeepingEntityGenerator generator;
  private final List<ListenerEventFilter> filters;

  public MessageEventHandler(HousekeepingEntityGenerator generator, List<ListenerEventFilter> filters) {
    this.generator = generator;
    this.filters = filters;
    this.lifecycleEventType = generator.getLifecycleEventType();
  }

  public List<HousekeepingEntity> handleMessage(MessageEvent event) {
    ListenerEvent listenerEvent = event.getEvent();
    if (shouldFilterMessage(listenerEvent)) {
      return Collections.emptyList();
    }
    return generateHousekeepingEntities(listenerEvent);
  }

  private boolean shouldFilterMessage(ListenerEvent listenerEvent) {
    return filters.stream().anyMatch(filter -> filter.isFiltered(listenerEvent, lifecycleEventType));
  }

  private List<HousekeepingEntity> generateHousekeepingEntities(ListenerEvent listenerEvent) {
    return generator.generate(listenerEvent, CLIENT_ID);
  }

  public List<ListenerEventFilter> getFilters() {
    return filters;
  }
}
