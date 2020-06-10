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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.event.AddPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.EventType;

import com.expediagroup.beekeeper.scheduler.apiary.filter.EventTypeListenerEventFilter;

@ExtendWith(MockitoExtension.class)
public class EventTypeListenerEventFilterTest {

  private final EventTypeListenerEventFilter listenerEventFilter = new EventTypeListenerEventFilter();
  @Mock private AddPartitionEvent addPartitionEvent;
  @Mock private AlterPartitionEvent alterPartitionEvent;
  @Mock private AlterTableEvent alterTableEvent;
  @Mock private DropPartitionEvent dropPartitionEvent;
  @Mock private DropTableEvent dropTableEvent;

  @Test
  public void typicalFilterAlterPartitionEvent() {
    when(this.alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    boolean filter = this.listenerEventFilter.isFilteredOut(this.alterPartitionEvent, UNREFERENCED);
    assertThat(filter).isFalse();
  }

  @Test
  public void typicalFilterAlterTableEvent() {
    when(this.alterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);
    boolean filter = this.listenerEventFilter.isFilteredOut(this.alterTableEvent, UNREFERENCED);
    assertThat(filter).isFalse();
  }

  @Test
  public void typicalFilterDropPartitionEvent() {
    when(this.dropPartitionEvent.getEventType()).thenReturn(EventType.DROP_PARTITION);
    boolean filter = this.listenerEventFilter.isFilteredOut(this.dropPartitionEvent, UNREFERENCED);
    assertThat(filter).isFalse();
  }

  @Test
  public void typicalFilterDropTableEvent() {
    when(this.dropTableEvent.getEventType()).thenReturn(EventType.DROP_TABLE);
    boolean filter = this.listenerEventFilter.isFilteredOut(this.dropTableEvent, UNREFERENCED);
    assertThat(filter).isFalse();
  }

  @Test
  public void typicalFilterOtherEvent() {
    when(this.addPartitionEvent.getEventType()).thenReturn(EventType.ADD_PARTITION);
    boolean filter = this.listenerEventFilter.isFilteredOut(this.addPartitionEvent, UNREFERENCED);
    assertThat(filter).isTrue();
  }
}
