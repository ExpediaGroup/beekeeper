/**
 * Copyright (C) 2019-2024 Expedia, Inc.
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;

import java.util.Locale;
import java.util.Map;

@Component
public class IcebergTableListenerEventFilter implements ListenerEventFilter {

  private static final Logger log = LogManager.getLogger(IcebergTableListenerEventFilter.class);

  private static final String METADATA_LOCATION_KEY = "metadata_location";
  private static final String TABLE_TYPE_KEY = "table_type";
  private static final String TABLE_TYPE_ICEBERG_VALUE = "iceberg";

  @Override
  public boolean isFiltered(ListenerEvent event, LifecycleEventType type) {
    Map<String, String> tableParameters = event.getTableParameters();

    if (tableParameters != null) {
      String metadataLocation = tableParameters.getOrDefault(METADATA_LOCATION_KEY, "");
      String tableType = tableParameters.getOrDefault(TABLE_TYPE_KEY, "");

      boolean hasMetadataLocation = !metadataLocation.trim().isEmpty();
      boolean isIcebergType = tableType.toLowerCase().contains(TABLE_TYPE_ICEBERG_VALUE);

      if (hasMetadataLocation || isIcebergType) {
        log.info("Iceberg table '{}.{}' is not currently supported in Beekeeper.",
            event.getDbName(), event.getTableName());
        return true;
      }
    }
    return false;
  }
}


