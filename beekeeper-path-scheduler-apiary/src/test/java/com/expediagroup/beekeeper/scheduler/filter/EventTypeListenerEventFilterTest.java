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
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.event.AddPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.EventType;

import com.expediagroup.beekeeper.scheduler.apiary.filter.EventTypeListenerEventFilter;

@ExtendWith(MockitoExtension.class)
public class EventTypeListenerEventFilterTest {
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

  private EventTypeListenerEventFilter listenerEventFilter = new EventTypeListenerEventFilter();

  @Test
  public void typicalFilterAlterPartitionEvent() {
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    boolean filter = listenerEventFilter.filter(alterPartitionEvent);
    assertThat(filter).isFalse();
  }

  @Test
  public void typicalFilterAlterTableEvent() {
    when(alterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);
    boolean filter = listenerEventFilter.filter(alterTableEvent);
    assertThat(filter).isFalse();
  }

  @Test
  public void typicalFilterDropPartitionEvent() {
    when(dropPartitionEvent.getEventType()).thenReturn(EventType.DROP_PARTITION);
    boolean filter = listenerEventFilter.filter(dropPartitionEvent);
    assertThat(filter).isFalse();
  }

  @Test
  public void typicalFilterDropTableEvent() {
    when(dropTableEvent.getEventType()).thenReturn(EventType.DROP_TABLE);
    boolean filter = listenerEventFilter.filter(dropTableEvent);
    assertThat(filter).isFalse();
  }

  @Test
  public void typicalFilterOtherEvent() {
    when(addPartitionEvent.getEventType()).thenReturn(EventType.ADD_PARTITION);
    boolean filter = listenerEventFilter.filter(addPartitionEvent);
    assertThat(filter).isTrue();
  }

  @Test
  public void typicalFilterNullEvent() {
    boolean filter = listenerEventFilter.filter(null);
    assertThat(filter).isTrue();
  }
}
