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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
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

import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.scheduler.apiary.model.BeekeeperEvent;
import com.expediagroup.beekeeper.scheduler.apiary.mapper.ExpiredPathMapper;
import com.expediagroup.beekeeper.scheduler.apiary.mapper.OrphanedPathMapper;

@ExtendWith(MockitoExtension.class)
public class MessageReaderAdapterTest {
  @Mock private MessageReader delegate;
  @Mock private MessageEvent messageEvent;

  @Mock private OrphanedPathMapper orphanMapper;
  @Mock private HousekeepingPath orphanedPath;

  @Mock private ExpiredPathMapper expireMapper;
  @Mock private HousekeepingPath expiredPath;

  private MessageReaderAdapter messageReaderAdapter;

  private static final List<HousekeepingPath> EMPTY_LIST = Collections.emptyList();

  @BeforeEach
  public void init() {
    messageReaderAdapter = new MessageReaderAdapter(delegate, List.of(orphanMapper, expireMapper));
  }

  @Test
  public void typicalRead() {
    when(delegate.read()).thenReturn(Optional.of(messageEvent));
    List<HousekeepingPath> orphanedResultSet = Arrays.asList(orphanedPath);
    when(orphanMapper.generateHouseKeepingPaths(messageEvent.getEvent())).thenReturn(orphanedResultSet);

    List<HousekeepingPath> expiredResultSet = Arrays.asList(expiredPath);
    when(expireMapper.generateHouseKeepingPaths(messageEvent.getEvent())).thenReturn(expiredResultSet);

    Optional<BeekeeperEvent> read = messageReaderAdapter.read();
    assertThat(read).isPresent();

    BeekeeperEvent event = read.get();
    List<HousekeepingPath> paths = event.getHousekeepingPaths();
    assertThat(event.getMessageEvent()).isEqualTo(messageEvent);
    assertThat(paths.size()).isEqualTo(2);
    assertThat(paths.get(0)).isEqualTo(orphanedPath);
    assertThat(paths.get(1)).isEqualTo(expiredPath);
  }

  @Test
  public void typicalReadWithEmptyMappedEvent() {
    when(delegate.read()).thenReturn(Optional.of(messageEvent));
    when(orphanMapper.generateHouseKeepingPaths(messageEvent.getEvent())).thenReturn(EMPTY_LIST);
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
    BeekeeperEvent pathEvent = new BeekeeperEvent(List.of(orphanedPath), messageEvent);
    messageReaderAdapter.delete(pathEvent);
    verify(delegate).delete(pathEvent.getMessageEvent());
  }

  @Test
  public void typicalClose() throws IOException {
    messageReaderAdapter.close();
    verify(delegate).close();
  }

}
