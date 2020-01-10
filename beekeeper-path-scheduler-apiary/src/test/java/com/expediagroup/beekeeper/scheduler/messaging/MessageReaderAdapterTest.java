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
package com.expediagroup.beekeeper.scheduler.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageReader;

import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.scheduler.apiary.handler.MessageEventHandler;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.MessageReaderAdapter;
import com.expediagroup.beekeeper.scheduler.apiary.model.BeekeeperEvent;

@ExtendWith(MockitoExtension.class)
public class MessageReaderAdapterTest {

  @Mock private MessageReader delegate;
  @Mock private MessageEvent messageEvent;
  @Mock private EntityHousekeepingPath path;
  @Mock private MessageEventHandler handler;
  private MessageReaderAdapter messageReaderAdapter;
  private List<HousekeepingPath> pathsList;

  @BeforeEach
  public void init() {
    pathsList = List.of(path);
    messageReaderAdapter = new MessageReaderAdapter(delegate, List.of(handler));
  }

  @Test
  public void typicalRead() {
    when(delegate.read()).thenReturn(Optional.of(messageEvent));
    when(handler.handleMessage(messageEvent)).thenReturn(pathsList);

    Optional<BeekeeperEvent> read = messageReaderAdapter.read();
    assertThat(read).isPresent();
    assertThat(read.get().getMessageEvent()).isEqualTo(messageEvent);
    assertThat(read.get().getHousekeepingPaths()).isEqualTo(pathsList);
  }

  @Test
  public void typicalReadWithEmptyMappedEvent() {
    when(delegate.read()).thenReturn(Optional.of(messageEvent));
    when(handler.handleMessage(messageEvent)).thenReturn(Collections.emptyList());
    Optional<BeekeeperEvent> read = messageReaderAdapter.read();
    assertThat(read).isEmpty();
  }

  @Test
  public void typicalEmptyRead() {
    when(delegate.read()).thenReturn(Optional.empty());
    Optional<BeekeeperEvent> read = messageReaderAdapter.read();
    assertThat(read).isEmpty();
  }

  @Test
  public void typicalDelete() {
    BeekeeperEvent beekeeperEvent = new BeekeeperEvent(pathsList, messageEvent);
    messageReaderAdapter.delete(beekeeperEvent);
    verify(delegate).delete(beekeeperEvent.getMessageEvent());
  }

  @Test
  public void typicalClose() throws IOException {
    messageReaderAdapter.close();
    verify(delegate).close();
  }
}
