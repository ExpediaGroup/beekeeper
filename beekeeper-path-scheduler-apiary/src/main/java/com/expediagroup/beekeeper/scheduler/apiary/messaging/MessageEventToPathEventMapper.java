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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.time.format.DateTimeParseException;
import java.util.Optional;

import com.expediagroup.beekeeper.core.model.CleanupType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AddPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.CreateTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.EventType;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;

import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;

import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;
import com.expediagroup.beekeeper.core.model.PathStatus;
import com.expediagroup.beekeeper.scheduler.apiary.model.PathEvents;

@Component
public class MessageEventToPathEventMapper {

  private static final Logger log = LoggerFactory.getLogger(MessageEventToPathEventMapper.class);
  private static final String CLIENT_ID = "apiary-metastore-event";
  private static final String FOREVER = "P356D";

  @Value("${properties.beekeeper.default-cleanup-delay}")
  private String defaultCleanupDelay;

  @Value("${properties.beekeeper.default-expired-cleanup-delay}")
  private String defaultExpiredCleanupDelay;

  @Value("${properties.apiary.cleanup-delay-property-key}")
  private String cleanupDelayPropertyKey;

  @Value("${properties.apiary.expired-cleanup-delay-property-key}")
  private String expiredCleanupDelayPropertyKey;

  public Optional<PathEvents> map(MessageEvent messageEvent) {
    ListenerEvent listenerEvent = messageEvent.getEvent();
    EventType eventType = listenerEvent.getEventType();

    PathEvents pathEvent = null;

    Map<String, String> tableParameters = listenerEvent.getTableParameters();
    Boolean isUnreferenced = CleanupType.UNREFERENCED.getBoolean(tableParameters);
    Boolean isExpired = CleanupType.EXPIRED.getBoolean(tableParameters);

    List<HousekeepingPath> paths = new ArrayList<>();
    boolean isMetadataUpdate = false;
    EntityHousekeepingPath.Builder builder = new EntityHousekeepingPath.Builder()
        .pathStatus(PathStatus.SCHEDULED)
        .creationTimestamp(LocalDateTime.now())
        .cleanupDelay(extractCleanupDelay(listenerEvent))
        .clientId(CLIENT_ID)
        .tableName(listenerEvent.getTableName())
        .databaseName(listenerEvent.getDbName());

    switch (eventType) {
    case CREATE_TABLE:
      CreateTableEvent createTableEvent = (CreateTableEvent) listenerEvent;
      paths.add(generateExpiredPath(listenerEvent,createTableEvent.getTableLocation()));
      pathEvent = new PathEvents(paths, messageEvent);
      break;
    case ADD_PARTITION:
      AddPartitionEvent addPartitionEvent = (AddPartitionEvent) listenerEvent;
      paths.add(generateExpiredPath(listenerEvent,addPartitionEvent.getTableLocation()));
      pathEvent = new PathEvents(paths, messageEvent);
      break;
    case ALTER_PARTITION:
      AlterPartitionEvent alterPartitionEvent = (AlterPartitionEvent) listenerEvent;
      isMetadataUpdate = isMetadataUpdate(
              alterPartitionEvent.getOldPartitionLocation(),
              alterPartitionEvent.getPartitionLocation());
      if ( isUnreferenced && !isMetadataUpdate) {
        paths.add(generateUnreferencedPath(listenerEvent, alterPartitionEvent.getOldPartitionLocation()));
      }
      if ( isExpired ) {
        paths.add(generateExpiredPath(listenerEvent, alterPartitionEvent.getTableLocation()));
      }
      pathEvent = new PathEvents(paths, messageEvent);
      break;
    case ALTER_TABLE:
      AlterTableEvent alterTableEvent = (AlterTableEvent) listenerEvent;
      isMetadataUpdate = isMetadataUpdate(
              alterTableEvent.getOldTableLocation(),
              alterTableEvent.getTableLocation());
      if ( isUnreferenced && !isMetadataUpdate) {
        paths.add(generateUnreferencedPath(listenerEvent, alterTableEvent.getOldTableLocation()));
      }
      if ( isExpired ) {
        paths.add(generateExpiredPath(listenerEvent, alterTableEvent.getTableLocation()));
      }
      pathEvent = new PathEvents(paths, messageEvent);
      break;
    case DROP_PARTITION:
      DropPartitionEvent dropPartitionEvent = (DropPartitionEvent) listenerEvent;
      paths.add(generateUnreferencedPath(listenerEvent,dropPartitionEvent.getPartitionLocation()));
      pathEvent = new PathEvents(paths, messageEvent);
      break;
    case DROP_TABLE:
      DropTableEvent dropTableEvent = (DropTableEvent) listenerEvent;
      paths.add(generateUnreferencedPath(listenerEvent,dropTableEvent.getTableLocation()));
      pathEvent = new PathEvents(paths, messageEvent);
      break;
    default:
      log.error("Unexpected event type: " + eventType);
      break;
    }

    return Optional.ofNullable(pathEvent);
  }

  private EntityHousekeepingPath generateExpiredPath(ListenerEvent listenerEvent, String cleanupLocation) {
    Map<String,String> tableParams = listenerEvent.getTableParameters();
    String cleanupDelay = tableParams.getOrDefault(expiredCleanupDelayPropertyKey, defaultExpiredCleanupDelay);
    return generatePath(CleanupType.EXPIRED,cleanupDelay,listenerEvent,cleanupLocation);
  }

  private EntityHousekeepingPath generateUnreferencedPath(ListenerEvent listenerEvent, String cleanupLocation) {
    Map<String,String> tableParams = listenerEvent.getTableParameters();
    String cleanupDelay = tableParams.getOrDefault(cleanupDelayPropertyKey, defaultCleanupDelay);
    return generatePath(CleanupType.UNREFERENCED,cleanupDelay,listenerEvent,cleanupLocation);
  }

  private EntityHousekeepingPath generatePath(
          CleanupType cleanupType,
          String cleanupDelay,
          ListenerEvent listenerEvent,
          String cleanupLocation) {

    EntityHousekeepingPath.Builder builder = new EntityHousekeepingPath.Builder()
            .pathStatus(PathStatus.SCHEDULED)
            .creationTimestamp(LocalDateTime.now())
            .cleanupDelay(Duration.parse(cleanupDelay))
            .cleanupType(cleanupType.name())
            .clientId(CLIENT_ID)
            .tableName(listenerEvent.getTableName())
            .databaseName(listenerEvent.getDbName())
            .path(cleanupLocation);

    return builder.build();
  }

  private boolean isMetadataUpdate(String oldLocation, String location) {
    return location == null || oldLocation == null || oldLocation.equals(location);
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
