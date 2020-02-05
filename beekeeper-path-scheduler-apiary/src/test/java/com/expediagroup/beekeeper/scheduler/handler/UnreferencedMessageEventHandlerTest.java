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

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.EventType;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;

import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.scheduler.apiary.filter.EventTypeListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.MetadataOnlyListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.TableParameterListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.WhitelistedListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.handler.UnreferencedMessageHandler;

@ExtendWith(MockitoExtension.class)
public class UnreferencedMessageEventHandlerTest {

  private static final String UNREF_HIVE_KEY = "beekeeper.unreferenced.data.retention.period";
  private static final String UNREF_DEFAULT = "P3D";
  private static final String PATH = "path";
  private static final String OLD_PATH = "old_path";
  private static final String DATABASE = "database";
  private static final String TABLE = "table";
  private static final String OLD_TABLE = "old_table";
  private static final Integer CLEANUP_ATTEMPTS = 0;
  private static final String CLEANUP_DELAY = "P7D";
  private static final LocalDateTime CREATION_TIMESTAMP = LocalDateTime.now();

  private static final Map<String, String> defaultProperties = Map.of(
      UNREFERENCED.getTableParameterName(), "true",
      UNREF_HIVE_KEY, CLEANUP_DELAY
  );

  @Mock private MessageEvent messageEvent;
  @Mock private TableParameterListenerEventFilter tableParameterListenerEventFilter;
  @Mock private MetadataOnlyListenerEventFilter metadataOnlyListenerEventFilter;
  @Mock private EventTypeListenerEventFilter eventTypeListenerEventFilter;
  @Mock private WhitelistedListenerEventFilter whiteListedFilter;
  @Mock private AlterPartitionEvent alterPartitionEvent;
  @Mock private AlterTableEvent alterTableEvent;
  @Mock private DropTableEvent dropTableEvent;
  @Mock private DropPartitionEvent dropPartitionEvent;
  private UnreferencedMessageHandler msgHandler;

  @BeforeEach
  public void setup() throws Exception {
    msgHandler = new UnreferencedMessageHandler(
        UNREF_HIVE_KEY,
        UNREF_DEFAULT,
        List.of(
            tableParameterListenerEventFilter, metadataOnlyListenerEventFilter,
            eventTypeListenerEventFilter, whiteListedFilter
        )
    );
  }

  @Test
  public void typicalHandleAlterPartitionEvent() {
    setupListenerEvent(alterPartitionEvent);
    setupFilterMocks(alterPartitionEvent, true, false, true, false, true, false, true, false);
    when(alterPartitionEvent.getTableParameters()).thenReturn(defaultProperties);
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    when(alterPartitionEvent.getOldPartitionLocation()).thenReturn(OLD_PATH);
    List<HousekeepingPath> paths = msgHandler.handleMessage(messageEvent);
    assertPath(paths, CLEANUP_DELAY);
  }

  @Test
  public void typicalHandleAlterTableEvent() {
    setupListenerEvent(alterTableEvent);
    setupFilterMocks(alterTableEvent, true, false, true, false, true, false, true, false);
    when(alterTableEvent.getTableParameters()).thenReturn(defaultProperties);
    when(alterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);
    when(alterTableEvent.getOldTableLocation()).thenReturn(OLD_PATH);
    List<HousekeepingPath> paths = msgHandler.handleMessage(messageEvent);
    assertPath(paths, CLEANUP_DELAY);
  }

  @Test
  public void typicalHandleDropPartitionEvent() {
    setupListenerEvent(dropPartitionEvent);
    setupFilterMocks(dropPartitionEvent, true, false, true, false, true, false, true, false);
    when(dropPartitionEvent.getTableParameters()).thenReturn(defaultProperties);
    when(dropPartitionEvent.getEventType()).thenReturn(EventType.DROP_PARTITION);
    when(dropPartitionEvent.getPartitionLocation()).thenReturn(OLD_PATH);
    List<HousekeepingPath> paths = msgHandler.handleMessage(messageEvent);
    assertPath(paths, CLEANUP_DELAY);
  }

  @Test
  public void typicalHandleDropTableEvent() {
    setupListenerEvent(dropTableEvent);
    setupFilterMocks(dropTableEvent, true, false, true, false, true, false, true, false);
    when(dropTableEvent.getTableParameters()).thenReturn(defaultProperties);
    when(dropTableEvent.getEventType()).thenReturn(EventType.DROP_TABLE);
    when(dropTableEvent.getTableLocation()).thenReturn(OLD_PATH);
    List<HousekeepingPath> paths = msgHandler.handleMessage(messageEvent);
    assertPath(paths, CLEANUP_DELAY);
  }

  @Test
  public void typicalHandleDefaultDelay() {
    setupListenerEvent(alterPartitionEvent);
    setupFilterMocks(alterPartitionEvent, true, false, true, false, true, false, true, false);
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    when(alterPartitionEvent.getOldPartitionLocation()).thenReturn(OLD_PATH);
    when(alterPartitionEvent.getTableParameters()).thenReturn(
        Map.of(UNREFERENCED.getTableParameterName(), "true")
    );
    List<HousekeepingPath> paths = msgHandler.handleMessage(messageEvent);
    assertPath(paths, UNREF_DEFAULT);
  }

  @Test
  public void handleDefaultDelayOnDurationException() {
    setupListenerEvent(alterPartitionEvent);
    setupFilterMocks(alterPartitionEvent, true, false, true, false, true, false, true, false);
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    when(alterPartitionEvent.getOldPartitionLocation()).thenReturn(OLD_PATH);
    when(alterPartitionEvent.getTableParameters()).thenReturn(
        Map.of(UNREFERENCED.getTableParameterName(), "true", UNREF_HIVE_KEY, "1")
    );
    List<HousekeepingPath> paths = msgHandler.handleMessage(messageEvent);
    assertPath(paths, UNREF_DEFAULT);
  }

  private void setupFilterMocks(ListenerEvent listenerEvent,
      boolean whiteListEnabled, boolean whitelistValue,
      boolean eventTypeEnabled, boolean eventTypeValue,
      boolean metadataEnabled, boolean metadataValue,
      boolean tableParameterEnabled, boolean tableParameterValue
  ) {

    if (whiteListEnabled) {
      when(whiteListedFilter.filter(listenerEvent, UNREFERENCED)).thenReturn(whitelistValue);
    }

    if (eventTypeEnabled) {
      when(eventTypeListenerEventFilter.filter(listenerEvent, UNREFERENCED)).thenReturn(eventTypeValue);
    }

    if (metadataEnabled) {
      when(metadataOnlyListenerEventFilter.filter(listenerEvent, UNREFERENCED)).thenReturn(metadataValue);
    }

    if (tableParameterEnabled) {
      when(tableParameterListenerEventFilter.filter(listenerEvent, UNREFERENCED)).thenReturn(tableParameterValue);
    }
  }

  private void setupListenerEvent(ListenerEvent listenerEvent) {
    when(messageEvent.getEvent()).thenReturn(listenerEvent);
    when(listenerEvent.getDbName()).thenReturn(DATABASE);
    when(listenerEvent.getTableName()).thenReturn(TABLE);
  }

  private void assertPath(List<HousekeepingPath> paths, String cleanupDelay) {
    assertThat(paths.size()).isEqualTo(1);
    HousekeepingPath path = paths.get(0);
    LocalDateTime now = LocalDateTime.now();
    assertThat(path.getPath()).isEqualTo(OLD_PATH);
    assertThat(path.getTableName()).isEqualTo(TABLE);
    assertThat(path.getDatabaseName()).isEqualTo(DATABASE);
    assertThat(path.getCleanupAttempts()).isEqualTo(CLEANUP_ATTEMPTS);
    assertThat(path.getCleanupDelay()).isEqualTo(Duration.parse(cleanupDelay));
    assertThat(path.getModifiedTimestamp()).isNull();
    assertThat(path.getCreationTimestamp()).isBetween(CREATION_TIMESTAMP, now);
    assertThat(path.getCleanupTimestamp()).isEqualTo(path.getCreationTimestamp().plus(Duration.parse(cleanupDelay)));
  }
}
