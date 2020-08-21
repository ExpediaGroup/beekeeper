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
package com.expediagroup.beekeeper.scheduler.apiary.handler;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;

import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.scheduler.apiary.filter.EventTypeListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.ListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.MetadataOnlyListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.TableParameterListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.WhitelistedListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.model.EventModel;

@Component
public class UnreferencedMessageHandler extends MessageEventHandler {

  private static final Logger log = LoggerFactory.getLogger(UnreferencedMessageHandler.class);
  private static final LifecycleEventType LIFECYCLE_EVENT_TYPE = LifecycleEventType.UNREFERENCED;

  private final List<ListenerEventFilter> filters;

  @Autowired
  public UnreferencedMessageHandler(
      @Value("${properties.apiary.cleanup-delay-property-key}") String hivePropertyKey,
      @Value("${properties.beekeeper.default-cleanup-delay}") String cleanupDelay) {
    super(cleanupDelay, hivePropertyKey, LIFECYCLE_EVENT_TYPE);
    filters = List
        .of(new EventTypeListenerEventFilter(), new MetadataOnlyListenerEventFilter(),
            new TableParameterListenerEventFilter(), new WhitelistedListenerEventFilter());
  }

  public UnreferencedMessageHandler(
      @Value("${properties.apiary.cleanup-delay-property-key}") String hivePropertyKey,
      @Value("${properties.beekeeper.default-cleanup-delay}") String cleanupDelay,
      List<ListenerEventFilter> filters) {
    super(cleanupDelay, hivePropertyKey, LIFECYCLE_EVENT_TYPE);
    this.filters = filters;
  }

  @Override
  protected List<ListenerEventFilter> getFilters() {
    return filters;
  }

  @Override
  protected List<EventModel> generateEventModels(ListenerEvent event) {
    List<EventModel> eventPaths = new ArrayList<>();

    switch (event.getEventType()) {
    case ALTER_PARTITION:
      log.info("Found an ALTER_PARTITION event.");
      eventPaths.add(new EventModel(LIFECYCLE_EVENT_TYPE, ((AlterPartitionEvent) event).getOldPartitionLocation()));
      break;
    case ALTER_TABLE:
      eventPaths.add(new EventModel(LIFECYCLE_EVENT_TYPE, ((AlterTableEvent) event).getOldTableLocation()));
      break;
    case DROP_TABLE:
      eventPaths.add(new EventModel(LIFECYCLE_EVENT_TYPE, ((DropTableEvent) event).getTableLocation()));
      break;
    case DROP_PARTITION:
      eventPaths.add(new EventModel(LIFECYCLE_EVENT_TYPE, ((DropPartitionEvent) event).getPartitionLocation()));
      break;
    }

    log.info("Event Paths size: " + eventPaths.size());

    return eventPaths;
  }
}
