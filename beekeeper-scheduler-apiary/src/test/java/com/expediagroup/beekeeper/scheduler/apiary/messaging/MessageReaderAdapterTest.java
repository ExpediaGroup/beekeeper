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
package com.expediagroup.beekeeper.scheduler.apiary.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
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

import com.amazonaws.AmazonClientException;

import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageReader;

import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.scheduler.apiary.handler.MessageEventHandler;
import com.expediagroup.beekeeper.scheduler.apiary.model.BeekeeperEvent;

@ExtendWith(MockitoExtension.class)
public class MessageReaderAdapterTest {

  @Mock private MessageReader delegate;
  @Mock private MessageEvent messageEvent;
  @Mock private HousekeepingPath path;
  @Mock private HousekeepingMetadata table;
  @Mock private MessageEventHandler unreferencedEventHandler;
  @Mock private MessageEventHandler expiredEventHandler;
  private MessageReaderAdapter messageReaderAdapter;
  private List<HousekeepingEntity> housekeepingEntities;

  @BeforeEach
  public void beforeEach() {
    housekeepingEntities = List.of(path, table);
    messageReaderAdapter = new MessageReaderAdapter(delegate, List.of(unreferencedEventHandler, expiredEventHandler));
  }

  @Test
  public void typicalRead() {
    when(delegate.read()).thenReturn(Optional.of(messageEvent));
    when(unreferencedEventHandler.handleMessage(messageEvent)).thenReturn(List.of(path));
    when(expiredEventHandler.handleMessage(messageEvent)).thenReturn(List.of(table));

    Optional<BeekeeperEvent> read = messageReaderAdapter.read();
    assertThat(read).isPresent();

    assertThat(read.get().getMessageEvent()).isEqualTo(messageEvent);
    assertThat(read.get().getHousekeepingEntities()).isEqualTo(housekeepingEntities);
  }

  @Test
  public void typicalReadWithEmptyMappedEvent() {
    when(delegate.read()).thenReturn(Optional.of(messageEvent));
    when(unreferencedEventHandler.handleMessage(messageEvent)).thenReturn(Collections.emptyList());
    when(expiredEventHandler.handleMessage(messageEvent)).thenReturn(Collections.emptyList());

    Optional<BeekeeperEvent> read = messageReaderAdapter.read();
    verify(delegate).delete(messageEvent);
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
    BeekeeperEvent beekeeperEvent = new BeekeeperEvent(housekeepingEntities, messageEvent);
    messageReaderAdapter.delete(beekeeperEvent);
    verify(delegate).delete(beekeeperEvent.getMessageEvent());
  }

  @Test
  public void deletionFailure() {
    BeekeeperEvent beekeeperEvent = new BeekeeperEvent(housekeepingEntities, messageEvent);
    doThrow(AmazonClientException.class).when(delegate).delete(beekeeperEvent.getMessageEvent());
    messageReaderAdapter.delete(beekeeperEvent);
    verify(delegate).delete(beekeeperEvent.getMessageEvent());
  }

  @Test
  public void typicalClose() throws IOException {
    messageReaderAdapter.close();
    verify(delegate).close();
  }
}
