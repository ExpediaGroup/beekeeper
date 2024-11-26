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
package com.expediagroup.beekeeper.scheduler.apiary.filter;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;

@Component
public class TableParameterListenerEventFilter implements ListenerEventFilter {

  private static final Logger log = LoggerFactory.getLogger(TableParameterListenerEventFilter.class);

  @Override
  public boolean isFiltered(ListenerEvent listenerEvent, LifecycleEventType lifecycleEventType) {
    Map<String, String> tableParameters = listenerEvent.getTableParameters();

    // Log the table params
    if (tableParameters != null && !tableParameters.isEmpty()) {
      log.info("Processing table parameters for event: {}", tableParameters);
      log.debug("Detailed table parameters: {}", tableParameters);
    } else {
      log.info("No table parameters found for event.");
    }

    if (tableParameters == null) {
      return true;
    }

    return !Boolean.parseBoolean(tableParameters.get(lifecycleEventType.getTableParameterName()));
  }
}

