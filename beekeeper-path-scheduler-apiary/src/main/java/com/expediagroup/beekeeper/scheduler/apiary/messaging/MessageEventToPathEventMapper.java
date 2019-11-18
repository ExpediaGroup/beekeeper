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
package com.expediagroup.beekeeper.scheduler.apiary.messaging;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.EventType;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;

import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;
import com.expediagroup.beekeeper.core.model.PathStatus;
import com.expediagroup.beekeeper.scheduler.apiary.model.PathEvent;

@Component
public class MessageEventToPathEventMapper {

  private static final Logger log = LoggerFactory.getLogger(MessageEventToPathEventMapper.class);
  private static final String CLIENT_ID = "apiary-metastore-event";

  @Value("${properties.beekeeper.default-cleanup-delay}")
  private String defaultCleanupDelay;

  @Value("${properties.apiary.cleanup-delay-property-key}")
  private String cleanupDelayPropertyKey;

  public Optional<PathEvent> map(MessageEvent messageEvent) {
    ListenerEvent listenerEvent = messageEvent.getEvent();
    EventType eventType = listenerEvent.getEventType();

    EntityHousekeepingPath.Builder builder = new EntityHousekeepingPath.Builder()
        .pathStatus(PathStatus.SCHEDULED)
        .creationTimestamp(LocalDateTime.now())
        .cleanupDelay(extractCleanupDelay(listenerEvent))
        .clientId(CLIENT_ID)
        .tableName(listenerEvent.getTableName())
        .databaseName(listenerEvent.getDbName());

    switch (eventType) {
    case ALTER_PARTITION:
      AlterPartitionEvent alterPartitionEvent = (AlterPartitionEvent) listenerEvent;
      builder.path(alterPartitionEvent.getOldPartitionLocation());
      break;
    case ALTER_TABLE:
      AlterTableEvent alterTableEvent = (AlterTableEvent) listenerEvent;
      builder.path(alterTableEvent.getOldTableLocation());
      break;
    case DROP_PARTITION:
      DropPartitionEvent dropPartitionEvent = (DropPartitionEvent) listenerEvent;
      builder.path(dropPartitionEvent.getPartitionLocation());
      break;
    case DROP_TABLE:
      DropTableEvent dropTableEvent = (DropTableEvent) listenerEvent;
      builder.path(dropTableEvent.getTableLocation());
      break;
    default:
      log.error("Unexpected event type: " + eventType);
      break;
    }

    return Optional.of(new PathEvent(builder.build(), messageEvent));
  }

  private Duration extractCleanupDelay(ListenerEvent listenerEvent) {
    String tableCleanupDelay = listenerEvent.getTableParameters()
      .getOrDefault(cleanupDelayPropertyKey, defaultCleanupDelay);
    try {
      return Duration.parse(tableCleanupDelay);
    } catch (DateTimeParseException e) {
      log.error("Text '{}' cannot be parsed to a Duration for table '{}.{}'. Using default setting.",
        tableCleanupDelay, listenerEvent.getDbName(), listenerEvent.getTableName());
      return Duration.parse(defaultCleanupDelay);
    }
  }
}
