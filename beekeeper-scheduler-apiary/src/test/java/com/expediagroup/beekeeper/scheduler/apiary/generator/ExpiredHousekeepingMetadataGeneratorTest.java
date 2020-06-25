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
package com.expediagroup.beekeeper.scheduler.apiary.generator;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static com.expedia.apiary.extensions.receiver.common.event.EventType.ALTER_TABLE;
import static com.expedia.apiary.extensions.receiver.common.event.EventType.DROP_TABLE;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropTableEvent;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;

@ExtendWith(MockitoExtension.class)
public class ExpiredHousekeepingMetadataGeneratorTest extends HousekeepingEntityGeneratorTestBase {

  @Mock private AlterTableEvent alterTableEvent;
  private ExpiredHousekeepingMetadataGenerator generator;

  @BeforeEach
  public void setup() {
    generator = new ExpiredHousekeepingMetadataGenerator(cleanupDelayExtractor, clock);
  }

  @Test
  public void typicalHandleAlterTableEvent() {
    setupClockAndExtractor(alterTableEvent);
    setupListenerEvent(alterTableEvent, ALTER_TABLE);

    List<HousekeepingEntity> tables = generator.generate(alterTableEvent, CLIENT_ID);
    assertExpiredHousekeepingMetadataEntity(tables);
  }

  @Test
  public void exceptionThrownOnUnhandledEvent() {
    DropTableEvent dropTableEvent = mock(DropTableEvent.class);
    when(dropTableEvent.getEventType()).thenReturn(DROP_TABLE);

    try {
      generator.generate(dropTableEvent, CLIENT_ID);
      fail("Should have thrown exception");
    } catch (BeekeeperException e) {
      assertThat(e.getMessage()).isEqualTo(format("No handler case for %s event type", dropTableEvent.getEventType()));
    }
  }

  private void assertExpiredHousekeepingMetadataEntity(List<HousekeepingEntity> tables) {
    assertThat(tables.size()).isEqualTo(1);
    HousekeepingMetadata table = (HousekeepingMetadata) tables.get(0);
    assertHousekeepingEntity(table, EXPIRED);
  }
}
