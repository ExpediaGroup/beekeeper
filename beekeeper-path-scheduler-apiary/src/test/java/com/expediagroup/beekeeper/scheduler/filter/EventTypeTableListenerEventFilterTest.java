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

import com.expedia.apiary.extensions.receiver.common.event.*;
import com.expediagroup.beekeeper.core.model.CleanupType;
import com.expediagroup.beekeeper.scheduler.apiary.filter.EventTypeTableListenerEventFilter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class EventTypeTableListenerEventFilterTest {

  @Mock
  private CreateTableEvent createTableEvent;
  @Mock
  private AddPartitionEvent addPartitionEvent;
  @Mock
  private AddPartitionEvent alterPartitionEvent;
  @Mock
  private AlterTableEvent alterTableEvent;
  @Mock
  private DropPartitionEvent dropPartitionEvent;
  @Mock
  private DropTableEvent dropTableEvent;

  private EventTypeTableListenerEventFilter listenerEventFilter = new EventTypeTableListenerEventFilter();

  private Map<String, String> getTableParameters(String isUnreferenced, String isExpired) {
    return Map.of(
            CleanupType.UNREFERENCED.tableParameterName(),isUnreferenced,
            CleanupType.EXPIRED.tableParameterName(),isExpired);
  }

  // create table permutations

  @Test
  public void filterWithUnreferencedOnlyCreateTableEvent() {
    assertFilterState(createTableEvent,
            EventType.CREATE_TABLE,
            "true",
            "false",
            true);
  }

  @Test
  public void filterWithExpiredOnlyCreateTableEvent() {
    assertFilterState(createTableEvent,
            EventType.CREATE_TABLE,
            "false",
            "true",
            false);
  }

  @Test
  public void filterWithUnreferencedAndExpiredCreateTableEvent() {
    assertFilterState(createTableEvent,
            EventType.CREATE_TABLE,
            "true",
            "true",
            false);
  }

  @Test
  public void filterWithNoTableParamsCreateTableEvent() {
    assertFilterState(createTableEvent,
            EventType.CREATE_TABLE,
            "false",
            "false",
            true);
  }

  // add partition permutations

  @Test
  public void filterWithUnreferencedOnlyAddPartitionEvent() {
    assertFilterState(addPartitionEvent,
            EventType.ADD_PARTITION,
            "true",
            "false",
            true);
  }

  @Test
  public void filterWithExpiredOnlyAddPartitionEvent() {
    assertFilterState(addPartitionEvent,
            EventType.ADD_PARTITION,
            "false",
            "true",
            false);
  }

  @Test
  public void filterWithUnreferencedAndExpiredAddPartitionEvent() {
    assertFilterState(addPartitionEvent,
            EventType.ADD_PARTITION,
            "true",
            "true",
            false);
  }

  @Test
  public void filterWithNoTableParamsAddPartitionEvent() {
    assertFilterState(addPartitionEvent,
            EventType.ADD_PARTITION,
            "false",
            "false",
            true);
  }

  // alter partition permutations

  @Test
  public void filterWithUnreferencedOnlyAlterPartitionEvent() {
    assertFilterState(alterPartitionEvent,
            EventType.ALTER_PARTITION,
            "true",
            "false",
            false);
  }

  @Test
  public void filterWithExpiredOnlyAlterPartitionEvent() {
    assertFilterState(alterPartitionEvent,
            EventType.ALTER_PARTITION,
            "false",
            "true",
            false);
  }

  @Test
  public void filterWithUnreferencedAndExpiredAlterPartitionEvent() {
    assertFilterState(alterPartitionEvent,
            EventType.ALTER_PARTITION,
            "true",
            "true",
            false);
  }

  @Test
  public void filterWithNoTableParamsAlterPartitionEvent() {
    assertFilterState(alterPartitionEvent,
            EventType.ALTER_PARTITION,
            "false",
            "false",
            true);
  }

  // alter table permutations

  @Test
  public void filterWithUnreferencedOnlyAlterTablevent() {
    assertFilterState(alterTableEvent,
            EventType.ALTER_TABLE,
            "true",
            "false",
            false);
  }

  @Test
  public void filterWithExpiredOnlyAlterTableEvent() {
    assertFilterState(alterTableEvent,
            EventType.ALTER_TABLE,
            "false",
            "true",
            false);
  }

  @Test
  public void filterWithUnreferencedAndExpiredAlterTableEvent() {
    assertFilterState(alterTableEvent,
            EventType.ALTER_TABLE,
            "true",
            "true",
            false);
  }

  @Test
  public void filterWithNoTableParamsAlterTableEvent() {
    assertFilterState(alterTableEvent,
            EventType.ALTER_TABLE,
            "false",
            "false",
            true);
  }

  // drop partition permutations

  @Test
  public void filterWithExpiredOnlyDropPartitionEvent() {
    assertFilterState(dropPartitionEvent,
            EventType.DROP_PARTITION,
            "false",
            "true",
            true);
  }

  @Test
  public void filterWithUnreferencedOnlyDropPartitionEvent() {
    assertFilterState(dropPartitionEvent,
            EventType.DROP_PARTITION,
            "true",
            "false",
            false);
  }

  @Test
  public void filterWithUnreferencedAndExpiredDropPartitionEvent() {
    assertFilterState(dropPartitionEvent,
            EventType.DROP_PARTITION,
            "true",
            "true",
            false);
  }

  @Test
  public void filterWithNoTableParamsDropPartitionEvent() {
    assertFilterState(dropPartitionEvent,
            EventType.DROP_PARTITION,
            "false",
            "false",
            true);
  }

  // drop table permutations

  @Test
  public void filterWithExpiredOnlyDropTableEvent() {
    assertFilterState(dropTableEvent,
            EventType.DROP_TABLE,
            "false",
            "true",
            true);
  }

  @Test
  public void filterWithUnreferencedOnlyDropTableEvent() {
    assertFilterState(dropTableEvent,
            EventType.DROP_TABLE,
            "true",
            "false",
            false);
  }

  @Test
  public void filterWithUnreferencedAndExpiredDropTableEvent() {
    assertFilterState(dropTableEvent,
            EventType.DROP_TABLE,
            "true",
            "true",
            false);
  }

  @Test
  public void filterWithNoTableParamsDropTableEvent() {
    assertFilterState(dropPartitionEvent,
            EventType.DROP_TABLE,
            "false",
            "false",
            true);
  }

  @Test
  public void typicalFilterNullEvent() {
    boolean filter = listenerEventFilter.filter(null);
    assertThat(filter).isTrue();
  }

  void assertFilterState(ListenerEvent listenerEvent,
      EventType eventType,
      String isUnreferenced,
      String isExpired,
      Boolean isFilteredOut) {
    when(listenerEvent.getEventType()).thenReturn(eventType);
    Map<String,String> tableParams = getTableParameters(isUnreferenced,isExpired);
    when(listenerEvent.getTableParameters()).thenReturn(tableParams);
    boolean filter = listenerEventFilter.filter(listenerEvent);
    if ( isFilteredOut ) {
      assertThat(filter).isTrue();
    }
    else {
      assertThat(filter).isFalse();
    }
  }
}
