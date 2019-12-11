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

import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.event.AddPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.EventType;

import com.expediagroup.beekeeper.scheduler.apiary.filter.DropTableListenerEventFilter;

@ExtendWith(MockitoExtension.class)
public class DropTableListenerEventFilterTest {

  private static final String BEEKEEPER_WHITELIST_DROP_TABLE = "beekeeper.permit.drop.table";
  private static final String WHITELISTED = "true";
  private static final String NOT_WHITELISTED = "false";

  @Mock
  private AddPartitionEvent addPartitionEvent;
  @Mock
  private DropTableEvent dropTableEvent;

  private DropTableListenerEventFilter listenerEventFilter = new DropTableListenerEventFilter();

  @Test
  public void typicalDoNotFilterWhitelistedDropTableEvent() {
    when(dropTableEvent.getEventType()).thenReturn(EventType.DROP_TABLE);
    when(dropTableEvent.getTableParameters())
      .thenReturn(Map.of(BEEKEEPER_WHITELIST_DROP_TABLE, WHITELISTED));
    boolean filter = listenerEventFilter.filter(dropTableEvent);
    assertThat(filter).isFalse();
  }

  @Test
  public void filterNonWhitelistedDropTableEvent() {
    when(dropTableEvent.getEventType()).thenReturn(EventType.DROP_TABLE);
    when(dropTableEvent.getTableParameters())
      .thenReturn(Map.of(BEEKEEPER_WHITELIST_DROP_TABLE, NOT_WHITELISTED));
    boolean filter = listenerEventFilter.filter(dropTableEvent);
    assertThat(filter).isTrue();
  }

  @Test
  public void filterNonWhitelistedDropTableEvent2() {
    when(dropTableEvent.getEventType()).thenReturn(EventType.DROP_TABLE);
    boolean filter = listenerEventFilter.filter(dropTableEvent);
    assertThat(filter).isTrue();
  }

  @Test
  public void filterNullTableParametersDropTableEvent() {
    when(dropTableEvent.getEventType()).thenReturn(EventType.DROP_TABLE);
    when(dropTableEvent.getTableParameters())
      .thenReturn(null);
    boolean filter = listenerEventFilter.filter(dropTableEvent);
    assertThat(filter).isTrue();
  }

  @Test
  public void filterEmptyTableParametersDropTableEvent() {
    when(dropTableEvent.getEventType()).thenReturn(EventType.DROP_TABLE);
    when(dropTableEvent.getTableParameters())
      .thenReturn(Collections.emptyMap());
    boolean filter = listenerEventFilter.filter(dropTableEvent);
    assertThat(filter).isTrue();
  }

  @Test
  public void doNotFilterNonDropTableEvent() {
    when(addPartitionEvent.getEventType()).thenReturn(EventType.ADD_PARTITION);
    boolean filter = listenerEventFilter.filter(addPartitionEvent);
    assertThat(filter).isFalse();
  }

  @Test
  public void filterNullEvent() {
    boolean filter = listenerEventFilter.filter(null);
    assertThat(filter).isTrue();
  }
}
