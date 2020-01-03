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
package com.expediagroup.beekeeper.scheduler.apiary.filter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.event.AddPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.CreateTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.EventType;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;

@ExtendWith(MockitoExtension.class)
public class EventTypeTableListenerEventFilterTest {

  private static final String TRUE_STR = "true";
  private static final String FALSE_STR = "false";
  private final EventTypeTableListenerEventFilter listenerEventFilter = new EventTypeTableListenerEventFilter();
  @Mock private CreateTableEvent createTableEvent;
  @Mock private AddPartitionEvent addPartitionEvent;
  @Mock private AlterPartitionEvent alterPartitionEvent;
  @Mock private AlterTableEvent alterTableEvent;
  @Mock private DropPartitionEvent dropPartitionEvent;
  @Mock private DropTableEvent dropTableEvent;

  private Map<String, String> getTableParameters(String isUnreferenced, String isExpired) {
    return Map.of(
        UNREFERENCED.getTableParameterName(), isUnreferenced,
        EXPIRED.getTableParameterName(), isExpired);
  }

  // create table permutations

  @Test
  public void filterWithUnreferencedOnlyCreateTableEvent() {
    assertFilterState(createTableEvent, EventType.CREATE_TABLE, TRUE_STR, FALSE_STR, true);
  }

  @Test
  public void filterWithExpiredOnlyCreateTableEvent() {
    assertFilterState(createTableEvent, EventType.CREATE_TABLE, FALSE_STR, TRUE_STR, false);
  }

  @Test
  public void filterWithUnreferencedAndExpiredCreateTableEvent() {
    assertFilterState(createTableEvent, EventType.CREATE_TABLE, TRUE_STR, TRUE_STR, false);
  }

  @Test
  public void filterWithNoTableParamsCreateTableEvent() {
    assertFilterState(createTableEvent, EventType.CREATE_TABLE, FALSE_STR, FALSE_STR, true);
  }

  // add partition permutations

  @Test
  public void filterWithUnreferencedOnlyAddPartitionEvent() {
    assertFilterState(addPartitionEvent, EventType.ADD_PARTITION, TRUE_STR, FALSE_STR, true);
  }

  @Test
  public void filterWithExpiredOnlyAddPartitionEvent() {
    assertFilterState(addPartitionEvent, EventType.ADD_PARTITION, FALSE_STR, TRUE_STR, false);
  }

  @Test
  public void filterWithUnreferencedAndExpiredAddPartitionEvent() {
    assertFilterState(addPartitionEvent, EventType.ADD_PARTITION, TRUE_STR, TRUE_STR, false);
  }

  @Test
  public void filterWithNoTableParamsAddPartitionEvent() {
    assertFilterState(addPartitionEvent, EventType.ADD_PARTITION, FALSE_STR, FALSE_STR, true);
  }

  // alter partition permutations

  @Test
  public void filterWithUnreferencedOnlyAlterPartitionEvent() {
    assertFilterState(alterPartitionEvent, EventType.ALTER_PARTITION, TRUE_STR, FALSE_STR, false);
  }

  @Test
  public void filterWithExpiredOnlyAlterPartitionEvent() {
    assertFilterState(alterPartitionEvent, EventType.ALTER_PARTITION, FALSE_STR, TRUE_STR, false);
  }

  @Test
  public void filterWithUnreferencedAndExpiredAlterPartitionEvent() {
    assertFilterState(alterPartitionEvent, EventType.ALTER_PARTITION, TRUE_STR, TRUE_STR, false);
  }

  @Test
  public void filterWithNoTableParamsAlterPartitionEvent() {
    assertFilterState(alterPartitionEvent, EventType.ALTER_PARTITION, FALSE_STR, FALSE_STR, true);
  }

  // alter table permutations

  @Test
  public void filterWithUnreferencedOnlyAlterTableEvent() {
    assertFilterState(alterTableEvent, EventType.ALTER_TABLE, TRUE_STR, FALSE_STR, false);
  }

  @Test
  public void filterWithExpiredOnlyAlterTableEvent() {
    assertFilterState(alterTableEvent, EventType.ALTER_TABLE, FALSE_STR, TRUE_STR, false);
  }

  @Test
  public void filterWithUnreferencedAndExpiredAlterTableEvent() {
    assertFilterState(alterTableEvent, EventType.ALTER_TABLE, TRUE_STR, TRUE_STR, false);
  }

  @Test
  public void filterWithNoTableParamsAlterTableEvent() {
    assertFilterState(alterTableEvent, EventType.ALTER_TABLE, FALSE_STR, FALSE_STR, true);
  }

  // drop partition permutations

  @Test
  public void filterWithExpiredOnlyDropPartitionEvent() {
    assertFilterState(dropPartitionEvent, EventType.DROP_PARTITION, FALSE_STR, TRUE_STR, true);
  }

  @Test
  public void filterWithUnreferencedOnlyDropPartitionEvent() {
    assertFilterState(dropPartitionEvent, EventType.DROP_PARTITION, TRUE_STR, FALSE_STR, false);
  }

  @Test
  public void filterWithUnreferencedAndExpiredDropPartitionEvent() {
    assertFilterState(dropPartitionEvent, EventType.DROP_PARTITION, TRUE_STR, TRUE_STR, false);
  }

  @Test
  public void filterWithNoTableParamsDropPartitionEvent() {
    assertFilterState(dropPartitionEvent, EventType.DROP_PARTITION, FALSE_STR, FALSE_STR, true);
  }

  // drop table permutations

  @Test
  public void filterWithExpiredOnlyDropTableEvent() {
    assertFilterState(dropTableEvent, EventType.DROP_TABLE, FALSE_STR, TRUE_STR, true);
  }

  @Test
  public void filterWithUnreferencedOnlyDropTableEvent() {
    assertFilterState(dropTableEvent, EventType.DROP_TABLE, TRUE_STR, FALSE_STR, false);
  }

  @Test
  public void filterWithUnreferencedAndExpiredDropTableEvent() {
    assertFilterState(dropTableEvent, EventType.DROP_TABLE, TRUE_STR, TRUE_STR, false);
  }

  @Test
  public void filterWithNoTableParamsDropTableEvent() {
    assertFilterState(dropPartitionEvent, EventType.DROP_TABLE, FALSE_STR, FALSE_STR, true);
  }

  private void assertFilterState(
      ListenerEvent listenerEvent,
      EventType eventType,
      String isUnreferenced,
      String isExpired,
      Boolean isFilteredOut
  ) {
    when(listenerEvent.getEventType()).thenReturn(eventType);
    Map<String, String> tableParams = getTableParameters(isUnreferenced, isExpired);
    when(listenerEvent.getTableParameters()).thenReturn(tableParams);
    boolean filter = listenerEventFilter.filter(listenerEvent);
    if (isFilteredOut) {
      assertThat(filter).isTrue();
    } else {
      assertThat(filter).isFalse();
    }
  }
}
