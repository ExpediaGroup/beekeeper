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
import java.util.*;
import java.time.format.DateTimeParseException;

import com.expediagroup.beekeeper.core.model.*;
import com.expediagroup.beekeeper.scheduler.apiary.model.ApiaryLifeCycleEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;
import com.expediagroup.beekeeper.scheduler.apiary.model.BeekeeperEvent;

import javax.annotation.PostConstruct;

import static com.expediagroup.beekeeper.core.model.LifeCycleEventType.EXPIRED;
import static com.expediagroup.beekeeper.core.model.LifeCycleEventType.UNREFERENCED;

@Component
public class MessageEventToBeekeeperEventMapper {

  private static final Logger log = LoggerFactory.getLogger(MessageEventToBeekeeperEventMapper.class);
  private static final String CLIENT_ID = "apiary-metastore-event";
  private static final String FOREVER = "P356D";

  @Value("${properties.beekeeper.default-cleanup-delay}")
  private String defaultUnreferencedDelay;

  @Value("${properties.apiary.cleanup-delay-property-key}")
  private String unreferencedDelayPropertyKey;

  @Value("${properties.beekeeper.default-expired-cleanup-delay}")
  private String defaultExpiredCleanupDelay;

  @Value("${properties.apiary.expired-cleanup-delay-property-key}")
  private String expiredDelayPropertyKey;

  private HashMap<LifeCycleEventType, LifeCycleConfiguration> lifeCycleMap;

  @Autowired
  public MessageEventToBeekeeperEventMapper() {
    lifeCycleMap = new HashMap<LifeCycleEventType, LifeCycleConfiguration>();
  }

  @PostConstruct
  public void init() {
    lifeCycleMap.put(EXPIRED, new LifeCycleConfiguration(EXPIRED, expiredDelayPropertyKey, defaultExpiredCleanupDelay));
    lifeCycleMap.put(UNREFERENCED, new LifeCycleConfiguration(UNREFERENCED, unreferencedDelayPropertyKey, defaultUnreferencedDelay));
  }

  /**
   * Maps a given event into PathEvents for Beekeeper to cleanup
   * @param messageEvent Current message event from Apiary SQS
   * @return Optional<PathEvents> Optional return of list of PathEvents
   */
  public Optional<BeekeeperEvent> map(MessageEvent messageEvent) {
    ListenerEvent listenerEvent = messageEvent.getEvent();

    // Get ApiaryEvent Type and map it to the corresponding ApiaryLifeCycleEvent in Beekeeper
    ApiaryLifeCycleEvent eventType = ApiaryLifeCycleEvent.valueOf(listenerEvent.getEventType().name());

    // Get if the event is of a certain lifecycle
    // TODO: This should be gutted for something more generic. As it stands now it isn't super extensible
    Map<String, String> tableParameters = listenerEvent.getTableParameters();
    Boolean isUnreferenced = lifeCycleMap.get(UNREFERENCED).getBoolean(tableParameters);
    Boolean isExpired = lifeCycleMap.get(EXPIRED).getBoolean(tableParameters);

    // Generate eventModels for the Lifecycle from the current event and if it is referenced or expired.
    // TODO: This could probably be genericized down more to not have to pass isUnreferenced / isExpired to the gather call.
    ArrayList<ApiaryLifeCycleEvent.EventModel> eventPaths = eventType.gatherEventPaths(listenerEvent, isUnreferenced, isExpired);

    // Loop through the event paths & convert them to HouseKeepingPaths
    List<HousekeepingPath> paths = new ArrayList<>();
    eventPaths.forEach(event -> paths.add(generatePath(event.lifeCycleEvent, listenerEvent, event.cleanupPath)));

    // If we have HouseKeepingPaths, return new PathEvents else - null
    BeekeeperEvent beekeeperEvent = paths.size() > 0 ? new BeekeeperEvent(paths, messageEvent) : null;
    return Optional.ofNullable(beekeeperEvent);
  }

  /**
   * Generates a path to clean up via EntityHousekeepingPath.Builder
   * @param lifeCycleType The type of lifecycle cleanup event this path is
   * @param listenerEvent The current event we're generating this path for
   * @param cleanupLocation The path of the current item to cleanup
   * @return EntityHouseKeepingPath Path object for the given parameters
   */
  private EntityHousekeepingPath generatePath(LifeCycleEventType lifeCycleType, ListenerEvent listenerEvent, String cleanupLocation) {
    EntityHousekeepingPath.Builder builder = new EntityHousekeepingPath.Builder()
            .pathStatus(PathStatus.SCHEDULED)
            .creationTimestamp(LocalDateTime.now())
            .cleanupDelay(extractCleanupDelay(listenerEvent, lifeCycleType))
            .lifeCycleType(lifeCycleType.name())
            .apiaryEventType(listenerEvent.getEventType().name())
            .clientId(CLIENT_ID)
            .tableName(listenerEvent.getTableName())
            .databaseName(listenerEvent.getDbName())
            .path(cleanupLocation);

    return builder.build();
  }

  /**
   * Checks if the current event is a metadata event.
   * This is determined if the old location string is the same as the new location string.
   * @param oldLocation Old event location
   * @param location New Event location
   * @return boolean True if old == new event location. False otherwise.
   */
  private boolean isMetadataUpdate(String oldLocation, String location) {
    return location == null || oldLocation == null || oldLocation.equals(location);
  }


  /**
   * Extracts the cleanup delay from the given event.
   * If the cleanupDelay on the event cannot be parsed, use the predefined default value.
   * @param listenerEvent Current event from Apiary
   * @param eventType LifeCycle Event Type to properly get the hive property & default deletion delay
   * @return Duration Parsed Duration object from the event or the default value.
   */
  private Duration extractCleanupDelay(ListenerEvent listenerEvent, LifeCycleEventType eventType) {
    LifeCycleConfiguration lifeCycle = lifeCycleMap.get(eventType);
    String propertyKey = lifeCycle.getHivePropertyKey();
    String defaultValue = lifeCycle.getDefaultDeletionDelay();
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
