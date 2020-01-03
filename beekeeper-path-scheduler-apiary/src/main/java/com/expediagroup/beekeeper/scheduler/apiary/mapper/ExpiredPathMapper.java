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
package com.expediagroup.beekeeper.scheduler.apiary.mapper;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.expedia.apiary.extensions.receiver.common.event.AddPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.CreateTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;

import com.expediagroup.beekeeper.scheduler.apiary.model.EventModel;

@Component
public class ExpiredPathMapper extends MessageEventMapper {

  private static final Logger log = LoggerFactory.getLogger(ExpiredPathMapper.class);

  @Autowired
  ExpiredPathMapper(
      @Value("${properties.beekeeper.default-expired-cleanup-delay}") String cleanupDelay,
      @Value("${properties.apiary.expired-cleanup-delay-property-key}") String hivePropertyKey
  ) {
    super(cleanupDelay, hivePropertyKey, EXPIRED);
  }

  @Override
  protected List<EventModel> generateEventModels(ListenerEvent listenerEvent) {
    Boolean tableWatchingExpired = checkIfTablePropertyExists(listenerEvent.getTableParameters());
    List<EventModel> eventPaths = new ArrayList<>();

    if (!tableWatchingExpired) {
      return eventPaths;
    }

    switch (listenerEvent.getEventType()) {
    case ALTER_PARTITION:
      eventPaths.add(new EventModel(lifecycleEventType, ((AlterPartitionEvent) listenerEvent).getTableLocation()));
      break;
    case ALTER_TABLE:
      eventPaths.add(new EventModel(lifecycleEventType, ((AlterTableEvent) listenerEvent).getTableLocation()));
      break;
    case CREATE_TABLE:
      eventPaths.add(new EventModel(lifecycleEventType, ((CreateTableEvent) listenerEvent).getTableLocation()));
      break;
    case ADD_PARTITION:
      eventPaths.add(new EventModel(lifecycleEventType, ((AddPartitionEvent) listenerEvent).getTableLocation()));
      break;
    }

    return eventPaths;
  }
}
