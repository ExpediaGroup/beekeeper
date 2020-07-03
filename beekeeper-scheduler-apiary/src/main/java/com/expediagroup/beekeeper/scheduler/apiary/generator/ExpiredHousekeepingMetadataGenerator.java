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
package com.expediagroup.beekeeper.scheduler.apiary.generator;

import static java.lang.String.format;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.scheduler.apiary.generator.utils.CleanupDelayExtractor;

public class ExpiredHousekeepingMetadataGenerator implements HousekeepingEntityGenerator {

  private static final LifecycleEventType LIFECYCLE_EVENT_TYPE = EXPIRED;
  private static final String EXPIRED_DATA_RETENTION_PERIOD_PROPERTY_KEY = "beekeeper.expired.data.retention.period";
  private final CleanupDelayExtractor cleanupDelayExtractor;
  private final Clock clock;

  public ExpiredHousekeepingMetadataGenerator(String cleanupDelay) {
    this(new CleanupDelayExtractor(EXPIRED_DATA_RETENTION_PERIOD_PROPERTY_KEY, cleanupDelay),
        Clock.systemDefaultZone());
  }

  @VisibleForTesting
  ExpiredHousekeepingMetadataGenerator(CleanupDelayExtractor cleanupDelayExtractor, Clock clock) {
    this.cleanupDelayExtractor = cleanupDelayExtractor;
    this.clock = clock;
  }

  @Override
  public List<HousekeepingEntity> generate(ListenerEvent listenerEvent, String clientId) {
    List<HousekeepingEntity> housekeepingEntities = new ArrayList<>();

    switch (listenerEvent.getEventType()) {
    case ALTER_TABLE:
      housekeepingEntities.add(generateHousekeepingEntity(listenerEvent, clientId));
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

  private HousekeepingEntity generateHousekeepingEntity(ListenerEvent listenerEvent, String clientId) {
    Duration cleanupDelay = cleanupDelayExtractor.extractCleanupDelay(listenerEvent);
    HousekeepingMetadata.Builder builder = new HousekeepingMetadata.Builder()
        .housekeepingStatus(SCHEDULED)
        .creationTimestamp(LocalDateTime.now(clock))
        .cleanupDelay(cleanupDelay)
        .lifecycleType(LIFECYCLE_EVENT_TYPE.toString())
        .clientId(clientId)
        .tableName(listenerEvent.getTableName())
        .databaseName(listenerEvent.getDbName());

    return builder.build();
  }
}
