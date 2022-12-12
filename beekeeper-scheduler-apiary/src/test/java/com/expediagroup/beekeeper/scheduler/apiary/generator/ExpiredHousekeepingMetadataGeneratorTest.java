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

import static com.expedia.apiary.extensions.receiver.common.event.EventType.ADD_PARTITION;
import static com.expedia.apiary.extensions.receiver.common.event.EventType.ALTER_PARTITION;
import static com.expedia.apiary.extensions.receiver.common.event.EventType.ALTER_TABLE;
import static com.expedia.apiary.extensions.receiver.common.event.EventType.CREATE_TABLE;
import static com.expedia.apiary.extensions.receiver.common.event.EventType.DROP_TABLE;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.ImmutableSortedMap;

import com.expedia.apiary.extensions.receiver.common.event.AddPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.CreateTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropTableEvent;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;

@ExtendWith(MockitoExtension.class)
public class ExpiredHousekeepingMetadataGeneratorTest extends HousekeepingEntityGeneratorTestBase {

  private static final Map<String, String> PARTITION_KEYS = ImmutableSortedMap.of(
      "event_date", "date",
      "event_hour", "smallint",
      "event_type", "string");
  private static final List<String> PARTITION_VALUES = List.of("2020-01-01", "0", "A");
  private static final String PARTITION_NAME = "event_date=2020-01-01/event_hour=0/event_type=A";

  @Mock private CreateTableEvent createTableEvent;
  @Mock private AlterTableEvent alterTableEvent;
  @Mock private AddPartitionEvent addPartitionEvent;
  @Mock private AlterPartitionEvent alterPartitionEvent;
  private ExpiredHousekeepingMetadataGenerator generator;

  @BeforeEach
  public void setup() {
    generator = new ExpiredHousekeepingMetadataGenerator(cleanupDelayExtractor, clock);
  }

  @Test
  public void typicalHandleCreateTableEvent() {
    setupClockAndExtractor(createTableEvent);
    setupListenerEvent(createTableEvent, CREATE_TABLE);
    when(createTableEvent.getTableLocation()).thenReturn(TABLE_PATH);

    List<HousekeepingEntity> housekeepingEntities = generator.generate(createTableEvent, CLIENT_ID);
    assertThat(housekeepingEntities.size()).isEqualTo(1);
    assertExpiredHousekeepingMetadataEntity(housekeepingEntities.get(0), null);
  }

  @Test
  public void typicalHandleAlterTableEvent() {
    setupClockAndExtractor(alterTableEvent);
    setupListenerEvent(alterTableEvent, ALTER_TABLE);
    when(alterTableEvent.getTableLocation()).thenReturn(TABLE_PATH);

    List<HousekeepingEntity> housekeepingEntities = generator.generate(alterTableEvent, CLIENT_ID);
    assertThat(housekeepingEntities.size()).isEqualTo(1);
    assertExpiredHousekeepingMetadataEntity(housekeepingEntities.get(0), null);
  }

  @Test
  public void typicalHandleAddPartitionEvent() {
    setupClockAndExtractor(addPartitionEvent);
    setupListenerEvent(addPartitionEvent, ADD_PARTITION);
    when(addPartitionEvent.getPartitionLocation()).thenReturn(TABLE_PATH);
    when(addPartitionEvent.getPartitionKeys()).thenReturn(PARTITION_KEYS);
    when(addPartitionEvent.getPartitionValues()).thenReturn(PARTITION_VALUES);

    List<HousekeepingEntity> housekeepingEntities = generator.generate(addPartitionEvent, CLIENT_ID);
    assertThat(housekeepingEntities.size()).isEqualTo(1);
    assertExpiredHousekeepingMetadataEntity(housekeepingEntities.get(0), PARTITION_NAME);
  }

  @Test
  public void typicalHandleAlterPartitionEvent() {
    setupClockAndExtractor(alterPartitionEvent);
    setupListenerEvent(alterPartitionEvent, ALTER_PARTITION);
    when(alterPartitionEvent.getPartitionLocation()).thenReturn(TABLE_PATH);
    when(alterPartitionEvent.getPartitionKeys()).thenReturn(PARTITION_KEYS);
    when(alterPartitionEvent.getPartitionValues()).thenReturn(PARTITION_VALUES);

    List<HousekeepingEntity> housekeepingEntities = generator.generate(alterPartitionEvent, CLIENT_ID);
    assertThat(housekeepingEntities.size()).isEqualTo(1);
    assertExpiredHousekeepingMetadataEntity(housekeepingEntities.get(0), PARTITION_NAME);
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

  private void assertExpiredHousekeepingMetadataEntity(HousekeepingEntity housekeepingEntity, String partitionName) {
    HousekeepingMetadata housekeepingMetadata = (HousekeepingMetadata) housekeepingEntity;
    assertHousekeepingEntity(housekeepingMetadata, EXPIRED);
    assertThat(housekeepingMetadata.getPartitionName()).isEqualTo(partitionName);
  }
}
