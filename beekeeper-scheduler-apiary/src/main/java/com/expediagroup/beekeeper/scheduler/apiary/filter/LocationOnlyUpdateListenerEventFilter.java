/**
 * Copyright (C) 2019-2023 Expedia, Inc.
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
package com.expediagroup.beekeeper.scheduler.apiary.filter;

import org.springframework.stereotype.Component;

import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.EventType;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;

import com.expediagroup.beekeeper.core.model.LifecycleEventType;

@Component
public class LocationOnlyUpdateListenerEventFilter implements ListenerEventFilter {
  
  private final LocationNormalizer locationNormalizer;
  
  public LocationOnlyUpdateListenerEventFilter () {
    this.locationNormalizer = new LocationNormalizer();
  }

  public LocationOnlyUpdateListenerEventFilter (LocationNormalizer locationNormaliser) {
    this.locationNormalizer = locationNormaliser;
  }

  
  @Override
  public boolean isFiltered(ListenerEvent listenerEvent, LifecycleEventType lifecycleEventType) {
    EventType eventType = listenerEvent.getEventType();
    switch (eventType) {
    case ALTER_PARTITION:
      AlterPartitionEvent alterPartitionEvent = (AlterPartitionEvent) listenerEvent;
      return isLocationSame(alterPartitionEvent.getOldPartitionLocation(),
          alterPartitionEvent.getPartitionLocation());
    case ALTER_TABLE:
      AlterTableEvent alterTableEvent = (AlterTableEvent) listenerEvent;
      return isLocationSame(alterTableEvent.getOldTableLocation(), alterTableEvent.getTableLocation());
    default:
      return false;
    }
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
