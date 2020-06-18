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
package com.expediagroup.beekeeper.scheduler.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import static com.expedia.apiary.extensions.receiver.common.event.EventType.ALTER_PARTITION;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;

import com.expediagroup.beekeeper.core.model.Housekeeping;
import com.expediagroup.beekeeper.scheduler.apiary.filter.TableParameterListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.WhitelistedListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.handler.UnreferencedMessageHandler;

@ExtendWith(MockitoExtension.class)
public class MessageEventHandlerTest {

  private static final String UNREF_HIVE_KEY = "beekeeper.unreferenced.data.retention.period";
  private static final String UNREF_DEFAULT = "P3D";
  private static final Map<String, String> defaultProperties = Map.of(
      UNREFERENCED.getTableParameterName(), "true",
      UNREF_HIVE_KEY, UNREF_DEFAULT
  );
  @Mock private MessageEvent messageEvent;
  @Mock private AlterPartitionEvent listenerEvent;
  @Mock private WhitelistedListenerEventFilter whiteListFilter;
  @Mock private TableParameterListenerEventFilter tableFilter;

  @Test
  public void typicalHandleMessage() {
    UnreferencedMessageHandler handler = new UnreferencedMessageHandler(UNREF_DEFAULT,
        List.of(whiteListFilter));
    setupListenerEvent();
    when(whiteListFilter.isFilteredOut(listenerEvent, UNREFERENCED)).thenReturn(false);
    List<Housekeeping> paths = handler.handleMessage(messageEvent);
    assertThat(paths.isEmpty()).isFalse();
  }

  @Test
  public void typicalFilterMessage() {
    UnreferencedMessageHandler handler = new UnreferencedMessageHandler(UNREF_DEFAULT,
        List.of(whiteListFilter));
    when(messageEvent.getEvent()).thenReturn(listenerEvent);
    when(whiteListFilter.isFilteredOut(listenerEvent, UNREFERENCED)).thenReturn(true);
    List<Housekeeping> paths = handler.handleMessage(messageEvent);
    assertThat(paths.isEmpty()).isTrue();
  }

  @Test
  public void ignoreUnconfiguredTables() {
    UnreferencedMessageHandler handler = new UnreferencedMessageHandler(UNREF_DEFAULT,
        List.of(tableFilter));
    when(messageEvent.getEvent()).thenReturn(listenerEvent);
    when(tableFilter.isFilteredOut(listenerEvent, UNREFERENCED)).thenReturn(true);
    List<Housekeeping> paths = handler.handleMessage(messageEvent);
    assertThat(paths.isEmpty()).isTrue();
  }

  private void setupListenerEvent() {
    when(messageEvent.getEvent()).thenReturn(listenerEvent);
    when(listenerEvent.getTableParameters()).thenReturn(defaultProperties);
    when(listenerEvent.getEventType()).thenReturn(ALTER_PARTITION);
  }
}
