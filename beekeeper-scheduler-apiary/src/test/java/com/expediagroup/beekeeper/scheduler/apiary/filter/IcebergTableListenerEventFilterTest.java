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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.HashMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;

import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.scheduler.service.predicate.IsIcebergTablePredicate;

// class to test the IcebergTableListenerEventFilter
public class IcebergTableListenerEventFilterTest {

  private IcebergTableListenerEventFilter filter;
  private IsIcebergTablePredicate predicate;

  @BeforeEach
  public void setUp() {
    predicate = new IsIcebergTablePredicate();
    filter = new IcebergTableListenerEventFilter(predicate);
  }

  @Test
  void testIsFilteredIcebergTable() {
    Map<String, String> tableParameters = new HashMap<>();
    tableParameters.put("table_type", "ICEBERG");

    ListenerEvent listenerEvent = createListenerEvent("database", "iceberg_table", tableParameters);
    LifecycleEventType lifecycleEventType = LifecycleEventType.EXPIRED;

    boolean result = filter.isFiltered(listenerEvent, lifecycleEventType);

    assertTrue(result, "Iceberg tables should be filtered out.");
  }

  @Test
  void testIsFilteredNonIcebergTable() {
    Map<String, String> tableParameters = new HashMap<>();
    tableParameters.put("table_type", "MANAGED");

    ListenerEvent listenerEvent = createListenerEvent("database", "hive_table", tableParameters);
    LifecycleEventType lifecycleEventType = LifecycleEventType.EXPIRED;

    boolean result = filter.isFiltered(listenerEvent, lifecycleEventType);

    assertFalse(result, "Non-Iceberg tables should not be filtered out.");
  }

  @Test
  void testIsFilteredNullTableType() {
    Map<String, String> tableParameters = new HashMap<>();
    // we don't add table_type param

    ListenerEvent listenerEvent = createListenerEvent("database", "table_without_type", tableParameters);
    LifecycleEventType lifecycleEventType = LifecycleEventType.EXPIRED;

    boolean result = filter.isFiltered(listenerEvent, lifecycleEventType);

    assertFalse(result, "Tables without 'table_type' should not be filtered out.");
  }

  @Test
  void testIsFilteredNullTableParameters() {
    ListenerEvent listenerEvent = createListenerEvent("database", "table_without_parameters", null);
    LifecycleEventType lifecycleEventType = LifecycleEventType.EXPIRED;

    boolean result = filter.isFiltered(listenerEvent, lifecycleEventType);

    assertFalse(result, "Tables with null parameters should not be filtered out.");
  }

  // helper method to create a ListenerEvent
  private ListenerEvent createListenerEvent(String dbName, String tableName, Map<String, String> tableParameters) {
    return new ListenerEvent() {
      @Override
      public String getDbName() {
        return dbName;
      }

      @Override
      public String getTableName() {
        return tableName;
      }

      @Override
      public Map<String, String> getTableParameters() {
        return tableParameters;
      }
    };
  }
}
