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
package com.expediagroup.beekeeper.scheduler.apiary.mapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.event.AddPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.CreateTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.EventType;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;

import com.expediagroup.beekeeper.core.model.HousekeepingPath;

@ExtendWith(MockitoExtension.class)
public class ExpiredPathMapperTest {

  private static final String DATABASE = "database";
  private static final String TABLE = "table";
  private static final String PATH = "some_path";
  private static final String OLD_PATH = "old_path";
  private static final Integer CLEANUP_ATTEMPTS = 0;
  private static final String CLEANUP_EXPIRED_DELAY = "P14D";
  private static final String CLEANUP_EXPIRED_DELAY_PROPERTY = "beekeeper.expired.data.retention.period";
  private static final String DEFAULT_EXPIRED_CLEANUP_DELAY = "P356D";
  private static final LocalDateTime CREATION_TIMESTAMP = LocalDateTime.now();

  private @Mock ListenerEvent listenerEvent;
  private @Mock AlterTableEvent alterTableEvent;
  private @Mock AlterPartitionEvent alterPartitionEvent;
  private @Mock CreateTableEvent createTableEvent;
  private @Mock AddPartitionEvent addPartitionEvent;

  @AfterEach
  public void afterEach() {
    reset(listenerEvent);
  }

  @Test
  public void shouldIgnoreAllEventsIfTableNotConfigured() {
    ExpiredPathMapper mapper = new ExpiredPathMapper(DEFAULT_EXPIRED_CLEANUP_DELAY, CLEANUP_EXPIRED_DELAY_PROPERTY);

    for (EventType e : EventType.values()) {
      setupTableParams(listenerEvent, false, false);
      when(listenerEvent.getEventType()).thenReturn(e);
      List<HousekeepingPath> houseKeepingPaths = mapper.generateHouseKeepingPaths(listenerEvent);
      assertThat(houseKeepingPaths.size()).isEqualTo(0);
      reset(listenerEvent);
    }
  }

  @Test
  public void shouldIgnoreUnwatchedEvents() {
    ExpiredPathMapper mapper = new ExpiredPathMapper(DEFAULT_EXPIRED_CLEANUP_DELAY, CLEANUP_EXPIRED_DELAY_PROPERTY);
    EventType[] ignoredEvents = { EventType.DROP_TABLE, EventType.DROP_PARTITION, EventType.INSERT };

    for (EventType e : ignoredEvents) {
      setupTableParams(listenerEvent, true, true);
      when(listenerEvent.getEventType()).thenReturn(e);
      List<HousekeepingPath> houseKeepingPaths = mapper.generateHouseKeepingPaths(listenerEvent);
      assertThat(houseKeepingPaths.size()).isEqualTo(0);
      reset(listenerEvent);
    }
  }

  @Test
  public void shouldProperlyDefaultIfEventExpirationInvalid() {
    ExpiredPathMapper mapper = new ExpiredPathMapper(DEFAULT_EXPIRED_CLEANUP_DELAY, CLEANUP_EXPIRED_DELAY_PROPERTY);
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    when(alterPartitionEvent.getOldPartitionLocation()).thenReturn(OLD_PATH);
    when(alterPartitionEvent.getTableName()).thenReturn(TABLE);
    when(alterPartitionEvent.getDbName()).thenReturn(DATABASE);
    when(alterPartitionEvent.getTableParameters()).thenReturn(Map.of(
        EXPIRED.getTableParameterName(), "true",
        CLEANUP_EXPIRED_DELAY, "INVALID_TIMESTAMP"
    ));

    List<HousekeepingPath> houseKeepingPaths = mapper.generateHouseKeepingPaths(alterPartitionEvent);
    assertThat(houseKeepingPaths.size()).isEqualTo(1);
    HousekeepingPath path = houseKeepingPaths.get(0);

    MapperTestUtils.assertPath(path, DEFAULT_EXPIRED_CLEANUP_DELAY, TABLE, DATABASE, CLEANUP_ATTEMPTS,
        CREATION_TIMESTAMP, OLD_PATH, EXPIRED);
  }

  @Test
  public void typicalMapAlterPartitionEvent() {
    ExpiredPathMapper mapper = new ExpiredPathMapper(DEFAULT_EXPIRED_CLEANUP_DELAY, CLEANUP_EXPIRED_DELAY_PROPERTY);

    setupTableParams(alterPartitionEvent, true, true);
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    when(alterPartitionEvent.getOldPartitionLocation()).thenReturn(OLD_PATH);
    when(alterPartitionEvent.getTableName()).thenReturn(TABLE);
    when(alterPartitionEvent.getDbName()).thenReturn(DATABASE);

    List<HousekeepingPath> houseKeepingPaths = mapper.generateHouseKeepingPaths(alterPartitionEvent);
    assertThat(houseKeepingPaths.size()).isEqualTo(1);
    HousekeepingPath path = houseKeepingPaths.get(0);

    MapperTestUtils.assertPath(path, CLEANUP_EXPIRED_DELAY, TABLE, DATABASE, CLEANUP_ATTEMPTS, CREATION_TIMESTAMP,
        OLD_PATH, EXPIRED);
  }

  @Test
  public void typicalMapAlterTableEvent() {
    ExpiredPathMapper mapper = new ExpiredPathMapper(DEFAULT_EXPIRED_CLEANUP_DELAY, CLEANUP_EXPIRED_DELAY_PROPERTY);
    setupTableParams(alterTableEvent, true, true);
    when(alterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);
    when(alterTableEvent.getDbName()).thenReturn(DATABASE);
    when(alterTableEvent.getTableName()).thenReturn(TABLE);
    when(alterTableEvent.getOldTableLocation()).thenReturn(OLD_PATH);

    List<HousekeepingPath> houseKeepingPaths = mapper.generateHouseKeepingPaths(alterTableEvent);
    assertThat(houseKeepingPaths.size()).isEqualTo(1);
    HousekeepingPath path = houseKeepingPaths.get(0);

    MapperTestUtils.assertPath(path, CLEANUP_EXPIRED_DELAY, TABLE, DATABASE, CLEANUP_ATTEMPTS, CREATION_TIMESTAMP,
        OLD_PATH, EXPIRED);
  }

  @Test
  public void typicalMapCreateTableEvent() {
    ExpiredPathMapper mapper = new ExpiredPathMapper(DEFAULT_EXPIRED_CLEANUP_DELAY, CLEANUP_EXPIRED_DELAY_PROPERTY);
    setupTableParams(createTableEvent, true, true);
    when(createTableEvent.getEventType()).thenReturn(EventType.CREATE_TABLE);
    when(createTableEvent.getDbName()).thenReturn(DATABASE);
    when(createTableEvent.getTableName()).thenReturn(TABLE);
    when(createTableEvent.getTableLocation()).thenReturn(PATH);

    List<HousekeepingPath> houseKeepingPaths = mapper.generateHouseKeepingPaths(createTableEvent);
    assertThat(houseKeepingPaths.size()).isEqualTo(1);
    HousekeepingPath path = houseKeepingPaths.get(0);

    MapperTestUtils.assertPath(path, CLEANUP_EXPIRED_DELAY, TABLE, DATABASE, CLEANUP_ATTEMPTS, CREATION_TIMESTAMP, PATH,
        EXPIRED);
  }

  @Test
  public void typicalAddPartitionEvent() {
    ExpiredPathMapper mapper = new ExpiredPathMapper(DEFAULT_EXPIRED_CLEANUP_DELAY, CLEANUP_EXPIRED_DELAY_PROPERTY);

    setupTableParams(addPartitionEvent, true, true);
    when(addPartitionEvent.getEventType()).thenReturn(EventType.ADD_PARTITION);
    when(addPartitionEvent.getTableName()).thenReturn(TABLE);
    when(addPartitionEvent.getDbName()).thenReturn(DATABASE);
    when(addPartitionEvent.getPartitionLocation()).thenReturn(PATH);

    List<HousekeepingPath> houseKeepingPaths = mapper.generateHouseKeepingPaths(addPartitionEvent);
    assertThat(houseKeepingPaths.size()).isEqualTo(1);
    HousekeepingPath path = houseKeepingPaths.get(0);

    MapperTestUtils.assertPath(path, CLEANUP_EXPIRED_DELAY, TABLE, DATABASE, CLEANUP_ATTEMPTS, CREATION_TIMESTAMP, PATH,
        EXPIRED);
  }

  private void setupTableParams(ListenerEvent event, Boolean isExpired, Boolean delaysDefined) {
    Map<String, String> tableParams = new HashMap<>();
    tableParams.put(EXPIRED.getTableParameterName(), isExpired.toString().toLowerCase());
    if (delaysDefined) {
      tableParams.put(CLEANUP_EXPIRED_DELAY_PROPERTY, CLEANUP_EXPIRED_DELAY);
    }
    when(event.getTableParameters()).thenReturn(tableParams);
  }
}


