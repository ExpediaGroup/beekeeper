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
package com.expediagroup.beekeeper.scheduler.apiary.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.expediagroup.beekeeper.scheduler.apiary.filter.ListenerEventFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.event.AddPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageReader;

@ExtendWith(MockitoExtension.class)
public class FilteringMessageReaderTest {
  @Mock
  private AddPartitionEvent alterPartitionEvent;
  @Mock
  private MessageEvent messageEvent;
  @Mock
  private ListenerEventFilter filter;
  @Mock
  private MessageReader delegate;

  private FilteringMessageReader filteringMessageReader;

  @BeforeEach
  public void init() {
    filteringMessageReader = new FilteringMessageReader(delegate, List.of(filter));
  }

  @Test
  public void typicalRead() {
    when(messageEvent.getEvent()).thenReturn(alterPartitionEvent);
    when(delegate.read()).thenReturn(Optional.of(messageEvent));
    when(filter.filter(alterPartitionEvent)).thenReturn(false);
    Optional<MessageEvent> result = filteringMessageReader.read();
    assertThat(result).isEqualTo(Optional.of(messageEvent));
    verify(delegate, times(0)).delete(any());
  }

  @Test
  public void readEmptyMessageEvent() {
    when(delegate.read()).thenReturn(Optional.empty());
    Optional<MessageEvent> result = filteringMessageReader.read();
    assertThat(result).isEmpty();
    verify(delegate, times(0)).delete(any());
  }

  @Test
  public void readNullListenerEvent() {
    when(messageEvent.getEvent()).thenReturn(null);
    when(delegate.read()).thenReturn(Optional.of(messageEvent));
    Optional<MessageEvent> result = filteringMessageReader.read();
    assertThat(result).isEmpty();
    verify(delegate).delete(messageEvent);
  }

  @Test
  public void readListenerEventFilter() {
    when(messageEvent.getEvent()).thenReturn(alterPartitionEvent);
    when(delegate.read()).thenReturn(Optional.of(messageEvent));
    when(filter.filter(alterPartitionEvent)).thenReturn(true);
    Optional<MessageEvent> result = filteringMessageReader.read();
    assertThat(result).isEmpty();
    verify(delegate).delete(messageEvent);
  }

  @Test
  public void typicalDelete() {
    filteringMessageReader.delete(messageEvent);
    verify(delegate).delete(messageEvent);
  }

  @Test
  public void typicalClose() throws IOException {
    filteringMessageReader.close();
    verify(delegate).close();
  }
}
