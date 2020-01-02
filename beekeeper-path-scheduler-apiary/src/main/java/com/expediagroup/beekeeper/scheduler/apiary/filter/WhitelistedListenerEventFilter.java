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
package com.expediagroup.beekeeper.scheduler.apiary.filter;

import java.util.Arrays;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.expedia.apiary.extensions.receiver.common.event.EventType;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;

@Component
public class WhitelistedListenerEventFilter implements ListenerEventFilter {

  private static final String BEEKEEPER_HIVE_EVENT_WHITELIST = "beekeeper.hive.event.whitelist";

  @Override
  public boolean filter(ListenerEvent listenerEvent) {
    Map<String, String> tableParameters = listenerEvent.getTableParameters();
    if (tableParameters != null && tableParameters.get(BEEKEEPER_HIVE_EVENT_WHITELIST) != null) {
      return !isWhitelisted(listenerEvent, tableParameters.get(BEEKEEPER_HIVE_EVENT_WHITELIST));
    }
    return !isDefaultBehaviour(listenerEvent);
  }

  private boolean isWhitelisted(ListenerEvent listenerEvent, String whitelist) {
    return Arrays.stream(whitelist.split(","))
      .map(String::trim)
      .anyMatch(whitelistedEvent -> whitelistedEvent.equalsIgnoreCase(listenerEvent.getEventType()
        .toString()));
  }

  private boolean isDefaultBehaviour(ListenerEvent listenerEvent) {
    EventType eventType = listenerEvent.getEventType();
    return eventType == EventType.ALTER_PARTITION || eventType == EventType.ALTER_TABLE;
  }
}
