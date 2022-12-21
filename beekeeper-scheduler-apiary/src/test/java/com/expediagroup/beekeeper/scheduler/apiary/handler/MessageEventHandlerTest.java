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
package com.expediagroup.beekeeper.scheduler.apiary.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;

import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.scheduler.apiary.filter.TableParameterListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.WhitelistedListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.generator.UnreferencedHousekeepingPathGenerator;

@ExtendWith(MockitoExtension.class)
public class MessageEventHandlerTest {

  private static final LifecycleEventType LIFECYCLE_EVENT_TYPE = UNREFERENCED;
  private static final String CLIENT_ID = "apiary-metastore-event";

  @Mock
  private MessageEvent messageEvent;
  @Mock
  private AlterPartitionEvent listenerEvent;
  @Mock
  private UnreferencedHousekeepingPathGenerator generator;
  @Mock
  private WhitelistedListenerEventFilter whiteListFilter;
  @Mock
  private TableParameterListenerEventFilter tableFilter;
  private MessageEventHandler handler;

  @BeforeEach
  public void setup() {
    when(generator.getLifecycleEventType()).thenReturn(LIFECYCLE_EVENT_TYPE);
    when(messageEvent.getEvent()).thenReturn(listenerEvent);

    handler = new MessageEventHandler(generator, List.of(whiteListFilter, tableFilter));
  }

  @Test
  public void typicalHandleMessage() {
    setupFilterMocks(false, false);
    when(generator.generate(listenerEvent, CLIENT_ID)).thenReturn(List.of(mock(HousekeepingEntity.class)));

    List<HousekeepingEntity> housekeepingEntities = handler.handleMessage(messageEvent);
    assertThat(housekeepingEntities.size()).isEqualTo(1);
  }

  @Test
  public void typicalFilterMessage() {
    setupFilterMocks(false, true);

    List<HousekeepingEntity> housekeepingEntities = handler.handleMessage(messageEvent);
    assertTrue(housekeepingEntities.isEmpty());
  }

  private void setupFilterMocks(boolean whitelistValue, boolean tableParameterValue) {
    when(whiteListFilter.isFiltered(listenerEvent, LIFECYCLE_EVENT_TYPE)).thenReturn(whitelistValue);
    when(tableFilter.isFiltered(listenerEvent, LIFECYCLE_EVENT_TYPE)).thenReturn(tableParameterValue);
  }
}
