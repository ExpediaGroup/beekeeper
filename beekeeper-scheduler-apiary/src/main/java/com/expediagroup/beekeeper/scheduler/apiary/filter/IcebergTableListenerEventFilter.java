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

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
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
// check if the table is an iceberg table and log if it is
  @Override
  public boolean isFiltered(ListenerEvent event, LifecycleEventType type) {
    Table table = createTableFromListenerEvent(event);
    if (isIcebergTablePredicate.test(table)) {
      log.info("Ignoring Iceberg table '{}.{}'.", event.getDbName(), event.getTableName());
      // Logging when iceberg table is ignored, as per ticket
      return true;
    }
    return false;
  }
  // create table from listener event to be used in predicate
  private Table createTableFromListenerEvent(ListenerEvent event) {
    // create table from listener event
    Table table = new Table();
    table.setDbName(event.getDbName());
    table.setTableName(event.getTableName());
    table.setParameters(event.getTableParameters());
    StorageDescriptor sd = new StorageDescriptor();

    Map<String, String> tableParameters = event.getTableParameters(); // retrieve table params and assigns to map
    String outputFormat = ""; // initialize output format
    if (tableParameters != null) {// if table parameters are not null
      outputFormat = tableParameters.getOrDefault("output_format", "");// get output format from table parameters
    }
    sd.setOutputFormat(outputFormat); // set output format in storafe descriptor
    table.setSd(sd); // attach storage descriptor to table

    return table;
  }
}
