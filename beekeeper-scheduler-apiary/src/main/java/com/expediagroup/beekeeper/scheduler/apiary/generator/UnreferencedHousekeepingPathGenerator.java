/**
 * Copyright (C) 2019-2021 Expedia, Inc.
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
package com.expediagroup.beekeeper.scheduler.apiary.generator;

import static java.lang.String.format;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.scheduler.apiary.generator.utils.CleanupDelayExtractor;

public class UnreferencedHousekeepingPathGenerator implements HousekeepingEntityGenerator {

  private static final Logger log = LoggerFactory.getLogger(UnreferencedHousekeepingPathGenerator.class);

  public static final String UNREFERENCED_DATA_RETENTION_PERIOD_PROPERTY_KEY = "beekeeper.unreferenced.data.retention.period";
  private static final LifecycleEventType LIFECYCLE_EVENT_TYPE = UNREFERENCED;

  private final CleanupDelayExtractor cleanupDelayExtractor;
  private final Clock clock;

  public UnreferencedHousekeepingPathGenerator(String cleanupDelay) {
    this(new CleanupDelayExtractor(UNREFERENCED_DATA_RETENTION_PERIOD_PROPERTY_KEY, cleanupDelay),
        Clock.systemDefaultZone());
  }

  @VisibleForTesting
  UnreferencedHousekeepingPathGenerator(CleanupDelayExtractor cleanupDelayExtractor, Clock clock) {
    this.cleanupDelayExtractor = cleanupDelayExtractor;
    this.clock = clock;
  }

  @Override
  public List<HousekeepingEntity> generate(ListenerEvent listenerEvent, String clientId) {
    List<HousekeepingEntity> housekeepingEntities = new ArrayList<>();

    switch (listenerEvent.getEventType()) {
    case ALTER_PARTITION:
      housekeepingEntities.add(generateHousekeepingEntity(listenerEvent, clientId,
          ((AlterPartitionEvent) listenerEvent).getOldPartitionLocation()));
      break;
    case ALTER_TABLE:
      housekeepingEntities.add(generateHousekeepingEntity(listenerEvent, clientId,
          ((AlterTableEvent) listenerEvent).getOldTableLocation()));
      break;
    case DROP_TABLE:
      housekeepingEntities.add(generateHousekeepingEntity(listenerEvent, clientId,
          ((DropTableEvent) listenerEvent).getTableLocation()));
      break;
    case DROP_PARTITION:
      housekeepingEntities.add(generateHousekeepingEntity(listenerEvent, clientId,
          ((DropPartitionEvent) listenerEvent).getPartitionLocation()));
      break;
    default:
      throw new BeekeeperException(format("No handler case for %s event type", listenerEvent.getEventType()));
    }

    return housekeepingEntities;
  }

  @Override
  public LifecycleEventType getLifecycleEventType() {
    return LIFECYCLE_EVENT_TYPE;
  }

  public HousekeepingEntity generateHousekeepingEntity(ListenerEvent listenerEvent, String clientId,
      String cleanupPath) {
    Duration cleanupDelay = cleanupDelayExtractor.extractCleanupDelay(listenerEvent);
    HousekeepingPath.HousekeepingPathBuilder builder = HousekeepingPath.builder()
        .housekeepingStatus(SCHEDULED)
        .creationTimestamp(LocalDateTime.now(clock))
        .cleanupDelay(cleanupDelay)
        .lifecycleType(LIFECYCLE_EVENT_TYPE.toString())
        .clientId(clientId)
        .tableName(listenerEvent.getTableName())
        .databaseName(listenerEvent.getDbName())
        .path(cleanupPath);

    return builder.build();
  }
}
