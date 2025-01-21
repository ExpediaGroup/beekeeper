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

import org.junit.jupiter.api.Test;

import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;

public class IcebergTableListenerEventFilterTest {

  private final IcebergTableListenerEventFilter filter = new IcebergTableListenerEventFilter();

  @Test
  public void shouldFilterWhenTableTypeIsIceberg() {
    ListenerEvent event = createListenerEventWithTableType("ICEBERG");
    boolean isFiltered = filter.isFiltered(event, LifecycleEventType.EXPIRED);
    assertThat(isFiltered).isTrue();
  }

  @Test
  public void shouldNotFilterWhenTableTypeIsNotIceberg() {
    ListenerEvent event = createListenerEventWithTableType("HIVE");
    boolean isFiltered = filter.isFiltered(event, LifecycleEventType.EXPIRED);
    assertThat(isFiltered).isFalse();
  }

  @Test
  public void shouldFilterWhenTableTypeIsIcebergIgnoreCase() {
    ListenerEvent event = createListenerEventWithTableType("iceberg");
    boolean isFiltered = filter.isFiltered(event, LifecycleEventType.EXPIRED);
    assertThat(isFiltered).isTrue();
  }

  @Test
  public void shouldFilterWhenMetadataLocationIsPresent() {
    ListenerEvent event = createListenerEventWithMetadataLocation("s3://example/path/to/metadata");
    boolean isFiltered = filter.isFiltered(event, LifecycleEventType.EXPIRED);
    assertThat(isFiltered).isTrue();
  }

  @Test
  public void shouldNotFilterWhenMetadataLocationIsEmpty() {
    ListenerEvent event = createListenerEventWithMetadataLocation("");
    boolean isFiltered = filter.isFiltered(event, LifecycleEventType.EXPIRED);
    assertThat(isFiltered).isFalse();
  }

  @Test
  public void shouldHandleNullTableParameters() {
    ListenerEvent event = createListenerEventWithTableParameters(null);
    boolean isFiltered = filter.isFiltered(event, LifecycleEventType.EXPIRED);
    assertThat(isFiltered).isFalse();
  }

  private ListenerEvent createListenerEventWithTableType(String tableType) {
    Map<String, String> tableParameters = new HashMap<>();
    tableParameters.put("table_type", tableType);
    return createListenerEventWithTableParameters(tableParameters);
  }

  private ListenerEvent createListenerEventWithMetadataLocation(String metadataLocation) {
    Map<String, String> tableParameters = new HashMap<>();
    tableParameters.put("metadata_location", metadataLocation);
    return createListenerEventWithTableParameters(tableParameters);
  }

  private ListenerEvent createListenerEventWithTableParameters(Map<String, String> tableParameters) {
    return new ListenerEvent() {
      @Override
      public String getDbName() {
        return "test_db";
      }

      @Override
      public String getTableName() {
        return "test_table";
      }

      @Override
      public Map<String, String> getTableParameters() {
        return tableParameters;
      }
    };
  }
}
