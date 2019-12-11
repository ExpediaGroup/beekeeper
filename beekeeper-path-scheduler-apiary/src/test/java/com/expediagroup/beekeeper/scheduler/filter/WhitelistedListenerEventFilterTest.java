/**
 * Copyright (C) 2019 Expedia, Inc.
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
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.event.AddPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.EventType;

import com.expediagroup.beekeeper.scheduler.apiary.filter.WhitelistedListenerEventFilter;

@ExtendWith(MockitoExtension.class)
public class WhitelistedListenerEventFilterTest {

  private static final String BEEKEEPER_HIVE_EVENT_WHITELIST = "beekeeper.hive.event.whitelist";

  @Mock
  private AddPartitionEvent alterPartitionEvent;
  @Mock
  private AlterTableEvent alterTableEvent;
  @Mock
  private DropTableEvent dropTableEvent;

  private WhitelistedListenerEventFilter listenerEventFilter = new WhitelistedListenerEventFilter();

  @Test
  public void filterAlterPartitionEvent() {
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    boolean filter = listenerEventFilter.filter(alterPartitionEvent);
    assertThat(filter).isFalse();
    verifyNoMoreInteractions(alterPartitionEvent);
  }

  @Test
  public void filterAlterTableEvent() {
    when(alterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);
    boolean filter = listenerEventFilter.filter(alterTableEvent);
    assertThat(filter).isFalse();
    verifyNoMoreInteractions(alterTableEvent);
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
    when(dropTableEvent.getEventType()).thenReturn(EventType.DROP_TABLE);
    when(dropTableEvent.getTableParameters())
      .thenReturn(Map.of(BEEKEEPER_HIVE_EVENT_WHITELIST, whitelist));
    boolean filter = listenerEventFilter.filter(dropTableEvent);
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
    when(dropTableEvent.getEventType()).thenReturn(EventType.DROP_TABLE);
    when(dropTableEvent.getTableParameters())
      .thenReturn(Map.of(BEEKEEPER_HIVE_EVENT_WHITELIST, whitelist));
    boolean filter = listenerEventFilter.filter(dropTableEvent);
    assertThat(filter).isTrue();
  }

  @Test
  public void filterNullTableParameters() {
    when(dropTableEvent.getEventType()).thenReturn(EventType.DROP_TABLE);
    when(dropTableEvent.getTableParameters())
      .thenReturn(null);
    boolean filter = listenerEventFilter.filter(dropTableEvent);
    assertThat(filter).isTrue();
  }

  @Test
  public void filterNullEvent() {
    boolean filter = listenerEventFilter.filter(null);
    assertThat(filter).isTrue();
  }
}
