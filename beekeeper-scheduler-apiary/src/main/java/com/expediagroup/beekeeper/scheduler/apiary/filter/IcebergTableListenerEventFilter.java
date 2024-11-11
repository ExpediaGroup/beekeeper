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

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;

import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.scheduler.service.predicate.IsIcebergTablePredicate;

// Class to intercept and filter events received and determine whether they should be processed or not
@Component
public class IcebergTableListenerEventFilter implements ListenerEventFilter {

  private static final Logger log = LogManager.getLogger(IcebergTableListenerEventFilter.class);

  private final IsIcebergTablePredicate isIcebergTablePredicate;

  @Autowired
  public IcebergTableListenerEventFilter(IsIcebergTablePredicate predicate) {
    this.isIcebergTablePredicate = predicate;
    // inject and assign predicate
  }

  @Override
  public boolean isFiltered(ListenerEvent event, LifecycleEventType type) {
    Map<String, String> tableParameters = event.getTableParameters();
    if (isIcebergTablePredicate.test(tableParameters)) {
      log.info("Ignoring Iceberg table '{}.{}'.", event.getDbName(), event.getTableName());
      // Logging added as per ticket
      return true;
    }
    return false;
  }
}
