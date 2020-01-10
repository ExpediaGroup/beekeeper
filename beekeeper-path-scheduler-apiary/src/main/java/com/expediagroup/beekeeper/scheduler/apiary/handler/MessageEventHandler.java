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
package com.expediagroup.beekeeper.scheduler.apiary.handler;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;

import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.model.PathStatus;
import com.expediagroup.beekeeper.scheduler.apiary.filter.ListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.model.EventModel;

@Component
public abstract class MessageEventHandler {

  private static final Logger log = LoggerFactory.getLogger(MessageEventHandler.class);
  private final String CLIENT_ID = "apiary-metastore-event";
  private final LifecycleEventType lifecycleEventType;
  private final String cleanupDelay;
  private final String hivePropertyKey;
  private final List<Class<? extends ListenerEventFilter>> validFilterClasses;
  @Autowired private List<ListenerEventFilter> allFilters;

  MessageEventHandler(
      String cleanupDelay,
      String hivePropertyKey,
      LifecycleEventType lifecycleEventType,
      List<Class<? extends ListenerEventFilter>> validFilterClasses
  ) {
    this.cleanupDelay = cleanupDelay;
    this.hivePropertyKey = hivePropertyKey;
    this.lifecycleEventType = lifecycleEventType;
    this.validFilterClasses = validFilterClasses;
  }

  public List<HousekeepingPath> handleMessage(MessageEvent event) {
    ListenerEvent listenerEvent = event.getEvent();

    if (!shouldFilterMessage(listenerEvent)) {
      return Collections.emptyList();
    }

    return generateHouseKeepingPaths(listenerEvent);
  }

  /**
   * Get's a boolean flag if the given tableparameters exist for this mapper
   *
   * @param tableParameters
   * @return boolean
   */
  boolean checkIfTablePropertyExists(Map<String, String> tableParameters) {
    LifecycleEventType type = getLifecycleEventType();
    return Boolean.valueOf(tableParameters.get(type.getTableParameterName()));
  }

  abstract List<EventModel> generateEventModels(ListenerEvent listenerEvent);

  private List<ListenerEventFilter> getValidFilters() {
    return allFilters.stream()
        .filter(filter -> validFilterClasses.contains(filter.getClass()))
        .collect(Collectors.toList());
  }

  private boolean shouldFilterMessage(ListenerEvent listenerEvent) {
    return getValidFilters().stream().anyMatch(filter -> filter.filter(listenerEvent));
  }

  /**
   * Generates housekeeping paths for a given event.
   *
   * @param listenerEvent Listener event from the current message
   * @return list of housekeeping paths. This can be an empty list if there are no valid paths.
   */
  private List<HousekeepingPath> generateHouseKeepingPaths(ListenerEvent listenerEvent) {
    return generateEventModels(listenerEvent).stream()
        .map(event -> generatePath(event, listenerEvent))
        .collect(Collectors.toList());
  }

  private final String getHivePropertyKey() { return hivePropertyKey; }

  private final String getCleanupDelay() { return cleanupDelay; }

  private final LifecycleEventType getLifecycleEventType() { return lifecycleEventType; }

  /**
   * Generates a path to clean up via EntityHousekeepingPath.Builder
   *
   * @param event The eventmodel holding data about this path
   * @param listenerEvent The current event we're generating this path for
   * @return EntityHouseKeepingPath Path object for the given parameters
   */
  private final EntityHousekeepingPath generatePath(EventModel event, ListenerEvent listenerEvent) {
    EntityHousekeepingPath.Builder builder = new EntityHousekeepingPath.Builder()
        .pathStatus(PathStatus.SCHEDULED)
        .creationTimestamp(LocalDateTime.now())
        .cleanupDelay(extractCleanupDelay(listenerEvent))
        .lifecycleType(event.lifecycleEvent.name())
        .clientId(CLIENT_ID)
        .tableName(listenerEvent.getTableName())
        .databaseName(listenerEvent.getDbName())
        .path(event.cleanupPath);

    return builder.build();
  }

  /**
   * Extracts the cleanup delay from the given event.
   * If the cleanupDelay on the event cannot be parsed, use the predefined default value.
   *
   * @param listenerEvent Current event from Apiary
   * @return Duration Parsed Duration object from the event or the default value.
   */
  private final Duration extractCleanupDelay(ListenerEvent listenerEvent) {
    String propertyKey = getHivePropertyKey();
    String defaultValue = getCleanupDelay();
    String tableCleanupDelay = listenerEvent.getTableParameters().getOrDefault(propertyKey, defaultValue);

    try {
      return Duration.parse(tableCleanupDelay);
    } catch (DateTimeParseException e) {
      log.error("Text '{}' cannot be parsed to a Duration for table '{}.{}'. Using default setting.",
          tableCleanupDelay, listenerEvent.getDbName(), listenerEvent.getTableName());
      return Duration.parse(defaultValue);
    }
  }
}
