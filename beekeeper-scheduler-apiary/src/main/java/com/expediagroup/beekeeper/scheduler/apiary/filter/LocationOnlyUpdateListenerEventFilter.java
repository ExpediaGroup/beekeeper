/**
 * Copyright (C) 2019-2026 Expedia, Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.expediagroup.beekeeper.scheduler.apiary.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.EventType;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;

import com.expediagroup.beekeeper.core.model.LifecycleEventType;

public class LocationOnlyUpdateListenerEventFilter implements ListenerEventFilter {

  private static final Logger log =
      LoggerFactory.getLogger(LocationOnlyUpdateListenerEventFilter.class);
  public static final String METRIC_NAME = "location-only-update-filtered";

  private final LocationNormalizer locationNormalizer;
  private final MeterRegistry meterRegistry;

  public LocationOnlyUpdateListenerEventFilter(MeterRegistry meterRegistry) {
    this(new LocationNormalizer(), meterRegistry);
  }

  public LocationOnlyUpdateListenerEventFilter(
      LocationNormalizer locationNormaliser, MeterRegistry meterRegistry) {
    this.locationNormalizer = locationNormaliser;
    this.meterRegistry = meterRegistry;
  }

  @Override
  public boolean isFiltered(ListenerEvent listenerEvent, LifecycleEventType lifecycleEventType) {
    EventType eventType = listenerEvent.getEventType();
    boolean filtered;
    switch (eventType) {
      case ALTER_PARTITION:
        AlterPartitionEvent alterPartitionEvent = (AlterPartitionEvent) listenerEvent;
        filtered =
            isLocationSame(
                alterPartitionEvent.getOldPartitionLocation(),
                alterPartitionEvent.getPartitionLocation());
        break;
      case ALTER_TABLE:
        AlterTableEvent alterTableEvent = (AlterTableEvent) listenerEvent;
        filtered =
            isLocationSame(
                alterTableEvent.getOldTableLocation(), alterTableEvent.getTableLocation());
        break;
      default:
        return false;
    }

    if (filtered) {
      reportFilteredEvent(listenerEvent, eventType);
    }
    return filtered;
  }

  private void reportFilteredEvent(ListenerEvent listenerEvent, EventType eventType) {
    log.info(
        "Filtered out {} event for \"{}.{}\" because the old and new locations are the same (no-op alter/rename).",
        eventType,
        listenerEvent.getDbName(),
        listenerEvent.getTableName());
    Counter.builder(METRIC_NAME)
        .tags(Tags.of("eventType", eventType.toString()))
        .register(meterRegistry)
        .increment();
  }

  private boolean isLocationSame(String oldLocation, String location) {
    if (location == null || oldLocation == null) {
      return true;
    }
    String normalizedOldLocation = locationNormalizer.normalize(oldLocation);
    String normalizedLocation = locationNormalizer.normalize(location);
    return normalizedOldLocation.equals(normalizedLocation);
  }
}
