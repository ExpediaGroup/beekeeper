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
package com.expediagroup.beekeeper.scheduler.service.predicate;

import static org.apache.commons.lang3.StringUtils.containsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;

import java.util.Map;
import java.util.function.Predicate;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.springframework.stereotype.Component;

import lombok.NonNull;

// class to determine if a table is an iceberg table based on `table_type` parameter
// based off IsIcbergTablePredicate class used in Icekeeper
@Component
public class IsIcebergTablePredicate implements Predicate<Table> {

  private static final String TABLE_TYPE_KEY = "table_type";
  private static final String TABLE_TYPE_ICEBERG_VALUE = "ICEBERG";

  @Override
  public boolean test(@NonNull Table table) {
    return (hasSdProperty(table) || hasTableParameter(table));
  }
// check if the table has the output format property set to iceberg
  private boolean hasSdProperty(Table table) {
    StorageDescriptor sd = table.getSd();
    if (sd != null) {
      String tableOutputFormat = sd.getOutputFormat();
      return containsIgnoreCase(tableOutputFormat, "iceberg");
    }
    return false;
  }
//retrieve the table parameters and check if the table type is ICEBERG
  private boolean hasTableParameter(Table table) {
    Map<String, String> parameters = table.getParameters();
    if (parameters != null) {
      String tableType = table.getParameters().get(TABLE_TYPE_KEY);
      return equalsIgnoreCase(TABLE_TYPE_ICEBERG_VALUE, tableType);
    }
    return false;
  }
}
