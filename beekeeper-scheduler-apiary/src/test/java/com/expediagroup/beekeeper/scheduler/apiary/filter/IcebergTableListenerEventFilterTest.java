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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;

import com.expediagroup.beekeeper.core.model.LifecycleEventType;

public class IcebergTableListenerEventFilterTest {

  private IcebergTableListenerEventFilter filter;

  @BeforeEach
  public void setUp() {
    filter = new IcebergTableListenerEventFilter();
  }

  @Test
  void testIsFilteredIcebergTable() {
    Map<String, String> tableParameters = new HashMap<>();
    tableParameters.put("table_type", "ICEBERG");

    ListenerEvent listenerEvent = createListenerEvent("database", "iceberg_table", tableParameters);
    LifecycleEventType lifecycleEventType = LifecycleEventType.EXPIRED;

    boolean result = filter.isFiltered(listenerEvent, lifecycleEventType);

    assertThat(result).isTrue();
  }

  @Test
  void testIsFilteredNonIcebergTable() {
    Map<String, String> tableParameters = new HashMap<>();
    tableParameters.put("table_type", "NICEBERG");

    ListenerEvent listenerEvent = createListenerEvent("database", "niceberg_table", tableParameters);
    LifecycleEventType lifecycleEventType = LifecycleEventType.EXPIRED;

    boolean result = filter.isFiltered(listenerEvent, lifecycleEventType);

    assertThat(result).isFalse();
  }

  @Test
  void testIsFilteredNullTableType() {
    Map<String, String> tableParameters = new HashMap<>();

    ListenerEvent listenerEvent = createListenerEvent("database", "table_without_type", tableParameters);
    LifecycleEventType lifecycleEventType = LifecycleEventType.EXPIRED;

    boolean result = filter.isFiltered(listenerEvent, lifecycleEventType);

    assertThat(result).isFalse();
  }

  @Test
  void testIsFilteredNullTableParameters() {
    ListenerEvent listenerEvent = createListenerEvent("database", "table_without_parameters", null);
    LifecycleEventType lifecycleEventType = LifecycleEventType.EXPIRED;

    boolean result = filter.isFiltered(listenerEvent, lifecycleEventType);

    assertThat(result).isFalse();
  }

  @Test
  void testIsFilteredIcebergTableWithBothParameters() {
    Map<String, String> tableParameters = new HashMap<>();
    tableParameters.put("table_type", "ICEBERG");
    tableParameters.put("output_format", "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat");

    ListenerEvent listenerEvent = createListenerEvent("database", "iceberg_table_both", tableParameters);
    LifecycleEventType lifecycleEventType = LifecycleEventType.EXPIRED;

    boolean result = filter.isFiltered(listenerEvent, lifecycleEventType);

    assertThat(result).isTrue();
  }

  @Test
  void testIsFilteredNonIcebergTableWithDifferentOutputFormat() {
    Map<String, String> tableParameters = new HashMap<>();
    tableParameters.put("output_format", "org.apache.not.an.ice.berg.table");

    ListenerEvent listenerEvent = createListenerEvent("database", "non_iceberg_table_output_format", tableParameters);
    LifecycleEventType lifecycleEventType = LifecycleEventType.EXPIRED;

    boolean result = filter.isFiltered(listenerEvent, lifecycleEventType);

    assertThat(result).isFalse();
  }

  @Test
  void testIsFilteredNonIcebergTableWithDifferentTableType() {
    Map<String, String> tableParameters = new HashMap<>();
    tableParameters.put("table_type", "NICEBERG");

    ListenerEvent listenerEvent = createListenerEvent("database", "non_iceberg_table_type", tableParameters);
    LifecycleEventType lifecycleEventType = LifecycleEventType.EXPIRED;

    boolean result = filter.isFiltered(listenerEvent, lifecycleEventType);

    assertThat(result).isFalse();
  }

  @Test
  void testIsFilteredWithIcebergOutputFormatAndDifferentTableType() {
    Map<String, String> tableParameters = new HashMap<>();
    tableParameters.put("table_type", "NICEBERG");
    tableParameters.put("output_format", "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat");

    ListenerEvent listenerEvent = createListenerEvent("database", "iceberg_output_non_iceberg_type", tableParameters);
    LifecycleEventType lifecycleEventType = LifecycleEventType.EXPIRED;

    boolean result = filter.isFiltered(listenerEvent, lifecycleEventType);

    assertThat(result).isTrue();
  }

  @Test
  void testIsFilteredTableWithIcebergTableTypeAndDifferentOutputFormat() {
    Map<String, String> tableParameters = new HashMap<>();
    tableParameters.put("table_type", "ICEBERG");
    tableParameters.put("output_format", "org.apache.not.an.ice.berg.table");

    ListenerEvent listenerEvent = createListenerEvent("database", "iceberg_type_non_iceberg_output", tableParameters);
    LifecycleEventType lifecycleEventType = LifecycleEventType.EXPIRED;

    boolean result = filter.isFiltered(listenerEvent, lifecycleEventType);

    assertThat(result).isTrue();
  }

  @Test
  void testIsFilteredTableWithNullOutputFormat() {
    Map<String, String> tableParameters = new HashMap<>();
    tableParameters.put("output_format", null); // Explicitly setting output_format to null

    ListenerEvent listenerEvent = createListenerEvent("database", "table_null_output_format", tableParameters);
    LifecycleEventType lifecycleEventType = LifecycleEventType.EXPIRED;

    boolean result = filter.isFiltered(listenerEvent, lifecycleEventType);

    assertThat(result).isFalse();
  }

  @Test
  void testIsFilteredNonIcebergTableWithOutputFormatContainingIceberg() {
    Map<String, String> tableParameters = new HashMap<>();
    tableParameters.put("output_format", "org.apache.iceberg.mr.hive.NonIcebergOutputFormat"); // Contains "iceberg" but not Iceberg-specific

    ListenerEvent listenerEvent = createListenerEvent("database", "non_iceberg_with_iceberg_in_output_format", tableParameters);
    LifecycleEventType lifecycleEventType = LifecycleEventType.EXPIRED;

    boolean result = filter.isFiltered(listenerEvent, lifecycleEventType);

    assertThat(result).isTrue();
  }

  @Test
  void testIsFilteredTableWithNoRelevantParameters() {
    Map<String, String> tableParameters = new HashMap<>();
    tableParameters.put("some_other_param", "some_value");

    ListenerEvent listenerEvent = createListenerEvent("database", "table_no_relevant_params", tableParameters);
    LifecycleEventType lifecycleEventType = LifecycleEventType.EXPIRED;

    boolean result = filter.isFiltered(listenerEvent, lifecycleEventType);

    assertThat(result).isFalse();
  }

  @Test
  void testIsFilteredTableWithNullParametersAndStorageDescriptor() {
    ListenerEvent listenerEvent = createListenerEvent("database", "table_null_parameters", null);
    LifecycleEventType lifecycleEventType = LifecycleEventType.EXPIRED;

    boolean result = filter.isFiltered(listenerEvent, lifecycleEventType);

    assertThat(result).isFalse();
  }

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
