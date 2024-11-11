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

import java.util.Map;
import java.util.function.Predicate;

import org.springframework.stereotype.Component;

// class to determine if a table is an iceberg table based on `table_type` parameter
@Component
public class IsIcebergTablePredicate implements Predicate<Map<String, String>> {

  private static final String TABLE_TYPE_KEY = "table_type";
  private static final String TABLE_TYPE_ICEBERG_VALUE = "ICEBERG";

  @Override
  public boolean test(Map<String, String> tableParameters) {
    if (tableParameters != null) {
      String tableType = tableParameters.get(TABLE_TYPE_KEY);
      return TABLE_TYPE_ICEBERG_VALUE.equalsIgnoreCase(tableType);
    }
    return false;
  }
}
