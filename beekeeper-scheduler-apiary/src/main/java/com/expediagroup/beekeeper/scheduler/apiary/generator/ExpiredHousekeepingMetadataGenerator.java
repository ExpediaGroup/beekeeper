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
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import com.expedia.apiary.extensions.receiver.common.event.AddPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.CreateTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.scheduler.apiary.generator.utils.CleanupDelayExtractor;

public class ExpiredHousekeepingMetadataGenerator implements HousekeepingEntityGenerator {

  private static final Logger log = LoggerFactory.getLogger(ExpiredHousekeepingMetadataGenerator.class);

  public static final String EXPIRED_DATA_RETENTION_PERIOD_PROPERTY_KEY = "beekeeper.expired.data.retention.period";
  private static final LifecycleEventType LIFECYCLE_EVENT_TYPE = EXPIRED;

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
    case CREATE_TABLE:
      housekeepingEntities.add(generateHousekeepingEntity((CreateTableEvent) listenerEvent, clientId));
      break;
    case ALTER_TABLE:
      housekeepingEntities.add(generateHousekeepingEntity((AlterTableEvent) listenerEvent, clientId));
      break;
    case ADD_PARTITION:
      housekeepingEntities.add(generateHousekeepingEntity((AddPartitionEvent) listenerEvent, clientId));
      break;
    case ALTER_PARTITION:
      housekeepingEntities.add(generateHousekeepingEntity((AlterPartitionEvent) listenerEvent, clientId));
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

  private HousekeepingEntity generateHousekeepingEntity(CreateTableEvent listenerEvent, String clientId) {
    return generateHousekeepingEntity(listenerEvent, clientId, listenerEvent.getTableLocation(), null);
  }

  private HousekeepingEntity generateHousekeepingEntity(AlterTableEvent listenerEvent, String clientId) {
    return generateHousekeepingEntity(listenerEvent, clientId, listenerEvent.getTableLocation(), null);
  }

  private HousekeepingEntity generateHousekeepingEntity(AddPartitionEvent listenerEvent, String clientId) {
    String partitionName = generatePartitionName(Lists.newArrayList(listenerEvent.getPartitionKeys().keySet()),
        listenerEvent.getPartitionValues());
    return generateHousekeepingEntity(listenerEvent, clientId, listenerEvent.getPartitionLocation(), partitionName);
  }

  private HousekeepingEntity generateHousekeepingEntity(AlterPartitionEvent listenerEvent, String clientId) {
    String partitionName = generatePartitionName(Lists.newArrayList(listenerEvent.getPartitionKeys().keySet()),
        listenerEvent.getPartitionValues());
    return generateHousekeepingEntity(listenerEvent, clientId, listenerEvent.getPartitionLocation(), partitionName);
  }

  private HousekeepingEntity generateHousekeepingEntity(ListenerEvent listenerEvent, String clientId, String path,
      String partitionName) {
    Duration cleanupDelay = cleanupDelayExtractor.extractCleanupDelay(listenerEvent);
    HousekeepingMetadata.HousekeepingMetadataBuilder builder = HousekeepingMetadata.builder()
        .housekeepingStatus(SCHEDULED)
        .creationTimestamp(LocalDateTime.now(clock))
        .cleanupDelay(cleanupDelay)
        .lifecycleType(LIFECYCLE_EVENT_TYPE.toString())
        .clientId(clientId)
        .path(path)
        .databaseName(listenerEvent.getDbName())
        .tableName(listenerEvent.getTableName())
        .partitionName(partitionName);

    return builder.build();
  }

  /**
   * Method to merge partition keys and values to create the partition name.
   *
   * @param keys List of partition keys e.g. ["event_date", "event_hour"].
   * @param values List of partition values e.g ["2020-01-01", "0"].
   * @return Partition name e.g. event_date=2020-01-01/event_hour=0
   */
  private String generatePartitionName(List<String> keys, List<String> values) {
    return IntStream.range(0, keys.size())
        .mapToObj(i -> keys.get(i) + "=" + values.get(i))
        .collect(Collectors.joining("/"));
  }
}
