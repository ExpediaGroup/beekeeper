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

import static com.expedia.apiary.extensions.receiver.common.event.EventType.ALTER_PARTITION;
import static com.expedia.apiary.extensions.receiver.common.event.EventType.ALTER_TABLE;
import static com.expedia.apiary.extensions.receiver.common.event.EventType.CREATE_TABLE;
import static com.expedia.apiary.extensions.receiver.common.event.EventType.DROP_PARTITION;
import static com.expedia.apiary.extensions.receiver.common.event.EventType.DROP_TABLE;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.CreateTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropTableEvent;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;

@ExtendWith(MockitoExtension.class)
public class UnreferencedHousekeepingPathGeneratorTest extends HousekeepingEntityGeneratorTestBase {

  private static final String OLD_PATH = "old_path";

  @Mock private AlterPartitionEvent alterPartitionEvent;
  @Mock private AlterTableEvent alterTableEvent;
  @Mock private DropTableEvent dropTableEvent;
  @Mock private DropPartitionEvent dropPartitionEvent;
  private UnreferencedHousekeepingPathGenerator generator;

  @BeforeEach
  public void setup() {
    generator = new UnreferencedHousekeepingPathGenerator(cleanupDelayExtractor, clock);
  }

  @Test
  public void typicalHandleAlterPartitionEvent() {
    setupClockAndExtractor(alterPartitionEvent);
    setupListenerEvent(alterPartitionEvent, ALTER_PARTITION);
    when(alterPartitionEvent.getOldPartitionLocation()).thenReturn(OLD_PATH);

    List<HousekeepingEntity> housekeepingEntities = generator.generate(alterPartitionEvent, CLIENT_ID);
    assertUnreferencedHousekeepingPathEntity(housekeepingEntities);
  }

  @Test
  public void typicalHandleAlterTableEvent() {
    setupClockAndExtractor(alterTableEvent);
    setupListenerEvent(alterTableEvent, ALTER_TABLE);
    when(alterTableEvent.getOldTableLocation()).thenReturn(OLD_PATH);

    List<HousekeepingEntity> housekeepingEntities = generator.generate(alterTableEvent, CLIENT_ID);
    assertUnreferencedHousekeepingPathEntity(housekeepingEntities);
  }

  @Test
  public void typicalHandleDropPartitionEvent() {
    setupClockAndExtractor(dropPartitionEvent);
    setupListenerEvent(dropPartitionEvent, DROP_PARTITION);
    when(dropPartitionEvent.getPartitionLocation()).thenReturn(OLD_PATH);

    List<HousekeepingEntity> housekeepingEntities = generator.generate(dropPartitionEvent, CLIENT_ID);
    assertUnreferencedHousekeepingPathEntity(housekeepingEntities);
  }

  @Test
  public void typicalHandleDropTableEvent() {
    setupClockAndExtractor(dropTableEvent);
    setupListenerEvent(dropTableEvent, DROP_TABLE);
    when(dropTableEvent.getTableLocation()).thenReturn(OLD_PATH);

    List<HousekeepingEntity> housekeepingEntities = generator.generate(dropTableEvent, CLIENT_ID);
    assertUnreferencedHousekeepingPathEntity(housekeepingEntities);
  }

  @Test
  public void exceptionThrownOnUnhandledEvent() {
    CreateTableEvent createTableEvent = mock(CreateTableEvent.class);
    when(createTableEvent.getEventType()).thenReturn(CREATE_TABLE);

    try {
      generator.generate(createTableEvent, CLIENT_ID);
      fail("Should have thrown exception");
    } catch (BeekeeperException e) {
      assertThat(e.getMessage()).isEqualTo(
          format("No handler case for %s event type", createTableEvent.getEventType()));
    }
  }

  private void assertUnreferencedHousekeepingPathEntity(List<HousekeepingEntity> paths) {
    assertThat(paths.size()).isEqualTo(1);
    HousekeepingPath path = (HousekeepingPath) paths.get(0);
    assertHousekeepingEntity(path, UNREFERENCED);
    assertThat(path.getPath()).isEqualTo(OLD_PATH);
  }
}
