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
package com.expediagroup.beekeeper.scheduler.filter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.event.EventType;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;

import com.expediagroup.beekeeper.scheduler.apiary.filter.WhitelistedListenerEventFilter;

@ExtendWith(MockitoExtension.class)
public class WhitelistedListenerEventFilterTest {

  private static final String BEEKEEPER_HIVE_EVENT_WHITELIST = "beekeeper.hive.event.whitelist";
  private final WhitelistedListenerEventFilter listenerEventFilter = new WhitelistedListenerEventFilter();
  @Mock private ListenerEvent listenerEvent;

  @ParameterizedTest
  @EnumSource(value = EventType.class, names = { "ALTER_PARTITION",
                                                 "ALTER_TABLE" })
  public void filterDefaultEvents(EventType eventType) {
    when(this.listenerEvent.getEventType()).thenReturn(eventType);
    boolean filter = this.listenerEventFilter.isFilteredOut(this.listenerEvent, UNREFERENCED);
    assertThat(filter).isFalse();
  }

  @ParameterizedTest
  @EnumSource(value = EventType.class, names = { "DROP_PARTITION",
                                                 "DROP_TABLE", })
  public void filterNonDefaultEvents(EventType eventType) {
    when(this.listenerEvent.getEventType()).thenReturn(eventType);
    boolean filter = this.listenerEventFilter.isFilteredOut(this.listenerEvent, UNREFERENCED);
    assertThat(filter).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = { "drop_table",
                           "alter_table,drop_table",
                           "alter_table, drop_table",
                           "alter_table, drop_table , create_table",
                           "Drop_Table",
                           "DROP_TABLE",
                           "Alter_Table , Create_Table, Drop_table" })
  public void filterWhitelistedEvent(String whitelist) {
    when(this.listenerEvent.getEventType()).thenReturn(EventType.DROP_TABLE);
    when(this.listenerEvent.getTableParameters())
        .thenReturn(Map.of(WhitelistedListenerEventFilterTest.BEEKEEPER_HIVE_EVENT_WHITELIST, whitelist));
    boolean filter = this.listenerEventFilter.isFilteredOut(this.listenerEvent, UNREFERENCED);
    assertThat(filter).isFalse();
  }

  @ParameterizedTest
  @ValueSource(strings = { "",
                           "  ",
                           "create_table,alter_table",
                           "drop_table alter_table",
                           "drop table,create_table",
                           "drop-table" })
  public void filterNotWhitelistedEvent(String whitelist) {
    when(this.listenerEvent.getEventType()).thenReturn(EventType.DROP_TABLE);
    when(this.listenerEvent.getTableParameters())
        .thenReturn(Map.of(WhitelistedListenerEventFilterTest.BEEKEEPER_HIVE_EVENT_WHITELIST, whitelist));
    boolean filter = this.listenerEventFilter.isFilteredOut(this.listenerEvent, UNREFERENCED);
    assertThat(filter).isTrue();
  }

  @Test
  public void filterNullTableParametersDefaultEvent() {
    when(this.listenerEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);
    when(this.listenerEvent.getTableParameters())
        .thenReturn(null);
    boolean filter = this.listenerEventFilter.isFilteredOut(this.listenerEvent, UNREFERENCED);
    assertThat(filter).isFalse();
  }

  @Test
  public void filterEmptyTableParametersDefaultEvent() {
    when(this.listenerEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);
    when(this.listenerEvent.getTableParameters())
        .thenReturn(Collections.emptyMap());
    boolean filter = this.listenerEventFilter.isFilteredOut(this.listenerEvent, UNREFERENCED);
    assertThat(filter).isFalse();
  }

  @Test
  public void filterNullTableParametersNonDefaultEvent() {
    when(this.listenerEvent.getEventType()).thenReturn(EventType.DROP_TABLE);
    when(this.listenerEvent.getTableParameters())
        .thenReturn(null);
    boolean filter = this.listenerEventFilter.isFilteredOut(this.listenerEvent, UNREFERENCED);
    assertThat(filter).isTrue();
  }

  @Test
  public void filterEmptyTableParametersNonDefaultEvent() {
    when(this.listenerEvent.getEventType()).thenReturn(EventType.DROP_TABLE);
    when(this.listenerEvent.getTableParameters())
        .thenReturn(Collections.emptyMap());
    boolean filter = this.listenerEventFilter.isFilteredOut(this.listenerEvent, UNREFERENCED);
    assertThat(filter).isTrue();
  }
}
