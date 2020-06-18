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

import static com.expedia.apiary.extensions.receiver.common.event.EventType.ALTER_TABLE;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;

import com.expediagroup.beekeeper.core.model.EntityHousekeepingTable;
import com.expediagroup.beekeeper.core.model.Housekeeping;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.scheduler.apiary.filter.EventTypeListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.TableParameterListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.WhitelistedListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.handler.ExpiredMessageHandler;

@ExtendWith(MockitoExtension.class)
public class ExpiredMessageHandlerTest {

  private static final String EXPIRED_HIVE_KEY = "beekeeper.expired.data.retention.period";
  private static final String EXPIRED_DEFAULT = "P3D";
  private static final String DATABASE = "database";
  private static final String TABLE = "table";
  private static final Integer CLEANUP_ATTEMPTS = 0;
  private static final String CLEANUP_DELAY = "P7D";
  private static final LocalDateTime CREATION_TIMESTAMP = LocalDateTime.now();

  private static final Map<String, String> defaultProperties = Map.of(
      EXPIRED.getTableParameterName(), "true",
      EXPIRED_HIVE_KEY, CLEANUP_DELAY
  );

  @Mock private MessageEvent messageEvent;
  @Mock private TableParameterListenerEventFilter tableParameterListenerEventFilter;
  @Mock private EventTypeListenerEventFilter eventTypeListenerEventFilter;
  @Mock private WhitelistedListenerEventFilter whitelistedListenerEventFilter;
  @Mock private AlterTableEvent alterTableEvent;
  private ExpiredMessageHandler msgHandler;

  @BeforeEach
  public void setup() {
    this.msgHandler = new ExpiredMessageHandler(EXPIRED_DEFAULT,
        List.of(tableParameterListenerEventFilter, eventTypeListenerEventFilter, whitelistedListenerEventFilter));
  }

  @Test
  public void typicalHandleAlterTableEvent() {
    setupListenerEvent(alterTableEvent);
    setupFilterMocks(alterTableEvent);
    when(alterTableEvent.getTableParameters()).thenReturn(defaultProperties);
    when(alterTableEvent.getEventType()).thenReturn(ALTER_TABLE);
    List<Housekeeping> tables = msgHandler.handleMessage(messageEvent);
    assertTable(tables, CLEANUP_DELAY, SCHEDULED);
  }

  @Test
  public void typicalHandleDefaultDelay() {
    setupListenerEvent(alterTableEvent);
    setupFilterMocks(alterTableEvent);
    when(alterTableEvent.getEventType()).thenReturn(ALTER_TABLE);
    when(alterTableEvent.getTableParameters()).thenReturn(
        Map.of(EXPIRED.getTableParameterName(), "true")
    );
    List<Housekeeping> tables = msgHandler.handleMessage(messageEvent);
    assertTable(tables, EXPIRED_DEFAULT, SCHEDULED);
  }

  @Test
  public void handleDefaultDelayOnDurationException() {
    setupListenerEvent(alterTableEvent);
    setupFilterMocks(alterTableEvent);
    when(alterTableEvent.getEventType()).thenReturn(ALTER_TABLE);
    when(alterTableEvent.getTableParameters()).thenReturn(
        Map.of(EXPIRED.getTableParameterName(), "true",
            EXPIRED_HIVE_KEY, "1")
    );
    List<Housekeeping> paths = msgHandler.handleMessage(messageEvent);
    assertTable(paths, EXPIRED_DEFAULT, SCHEDULED);
  }

  private void setupFilterMocks(ListenerEvent listenerEvent) {
    when(eventTypeListenerEventFilter.isFilteredOut(listenerEvent, EXPIRED)).thenReturn(false);
    when(tableParameterListenerEventFilter.isFilteredOut(listenerEvent, EXPIRED)).thenReturn(false);
    when(whitelistedListenerEventFilter.isFilteredOut(listenerEvent, EXPIRED)).thenReturn(false);
  }

  private void setupListenerEvent(ListenerEvent listenerEvent) {
    when(this.messageEvent.getEvent()).thenReturn(listenerEvent);
    when(listenerEvent.getDbName()).thenReturn(DATABASE);
    when(listenerEvent.getTableName()).thenReturn(TABLE);
  }

  private void assertTable(List<Housekeeping> tables, String cleanupDelay, HousekeepingStatus housekeepingStatus) {
    assertThat(tables.size()).isEqualTo(1);
    EntityHousekeepingTable table = (EntityHousekeepingTable) tables.get(0);
    LocalDateTime now = LocalDateTime.now();
    assertThat(LifecycleEventType.valueOf(table.getLifecycleType())).isEqualTo(EXPIRED);
    assertThat(table.getHousekeepingStatus()).isEqualTo(housekeepingStatus);
    assertThat(table.getTableName()).isEqualTo(TABLE);
    assertThat(table.getDatabaseName()).isEqualTo(DATABASE);
    assertThat(table.getCleanupAttempts()).isEqualTo(CLEANUP_ATTEMPTS);
    assertThat(table.getCleanupDelay()).isEqualTo(Duration.parse(cleanupDelay));
    assertThat(table.getModifiedTimestamp()).isNull();
    assertThat(table.getCreationTimestamp()).isBetween(CREATION_TIMESTAMP, now);
    assertThat(table.getCleanupTimestamp()).isEqualTo(table.getCreationTimestamp().plus(Duration.parse(cleanupDelay)));
  }
}
