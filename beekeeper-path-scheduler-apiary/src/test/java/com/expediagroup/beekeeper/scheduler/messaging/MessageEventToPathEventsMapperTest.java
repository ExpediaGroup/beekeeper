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
package com.expediagroup.beekeeper.scheduler.messaging;

import static com.expediagroup.beekeeper.core.model.CleanupType.EXPIRED;
import static com.expediagroup.beekeeper.core.model.CleanupType.UNREFERENCED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;

import com.expediagroup.beekeeper.core.model.CleanupType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.EventType;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageProperty;
import com.expedia.apiary.extensions.receiver.sqs.messaging.SqsMessageProperty;

import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.MessageEventToPathEventMapper;
import com.expediagroup.beekeeper.scheduler.apiary.model.PathEvents;

@ExtendWith(MockitoExtension.class)
@ExtendWith(SpringExtension.class)
@TestPropertySource(properties = {
        "properties.beekeeper.default-cleanup-delay=P3D",
        "properties.apiary.cleanup-delay-property-key=beekeeper.unreferenced.data.retention.period",
        "properties.beekeeper.default-expired-cleanup-delay=P356D",
        "properties.apiary.expired-cleanup-delay-property-key=beekeeper.expired.data.retention.period"
})
@ContextConfiguration(classes = { MessageEventToPathEventMapper.class },
    loader = AnnotationConfigContextLoader.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class MessageEventToPathEventsMapperTest {

  private static final String RECEIPT_HANDLE = "receiptHandle";
  private static final String DATABASE = "database";
  private static final String TABLE = "table";
  private static final String PATH = "some_path";
  private static final String OLD_PATH = "old_path";
  private static final Integer CLEANUP_ATTEMPTS = 0;
  private static final String CLEANUP_UNREFERENCED_DELAY = "P7D";
  private static final String CLEANUP_UNREFERENCED_DELAY_PROPERTY = "beekeeper.unreferenced.data.retention.period";
  private static final String CLEANUP_EXPIRED_DELAY = "P14D";
  private static final String CLEANUP_EXPIRED_DELAY_PROPERTY = "beekeeper.expired.data.retention.period";
  private static final String DEFAULT_UNREFERENCED_CLEANUP_DELAY = "P3D";
  private static final String DEFAULT_EXPIRED_CLEANUP_DELAY = "P356D";
  private static final LocalDateTime CREATION_TIMESTAMP = LocalDateTime.now();

  private @Mock AlterPartitionEvent alterPartitionEvent;
  private @Mock AlterTableEvent alterTableEvent;
  private @Mock DropPartitionEvent dropPartitionEvent;
  private @Mock DropTableEvent dropTableEvent;

  @Autowired
  private MessageEventToPathEventMapper mapper;

  @Test
  public void typicalMapAlterPartitionEvent() {
    setupMocks(alterPartitionEvent, true, true, true);
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    when(alterPartitionEvent.getTableLocation()).thenReturn(PATH);
    when(alterPartitionEvent.getPartitionLocation()).thenReturn(PATH);
    when(alterPartitionEvent.getOldPartitionLocation()).thenReturn(OLD_PATH);
    MessageEvent messageEvent = newMessageEvent(alterPartitionEvent);
    Optional<PathEvents> pathEvent = mapper.map(messageEvent);
    assertPath(messageEvent, pathEvent, CLEANUP_UNREFERENCED_DELAY, UNREFERENCED, OLD_PATH, 0);
    assertPath(messageEvent, pathEvent, CLEANUP_EXPIRED_DELAY, EXPIRED, PATH,1);
  }

  @Test
  public void typicalMapAlterTableEvent() {
    setupMocks(alterTableEvent, true, true, true);
    when(alterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);
    when(alterTableEvent.getTableLocation()).thenReturn(OLD_PATH);
    when(alterTableEvent.getOldTableLocation()).thenReturn(PATH);
    MessageEvent messageEvent = newMessageEvent(alterTableEvent);
    Optional<PathEvents> pathEvent = mapper.map(messageEvent);
    assertPath(messageEvent, pathEvent, CLEANUP_UNREFERENCED_DELAY, UNREFERENCED, PATH, 0);
    assertPath(messageEvent, pathEvent, CLEANUP_EXPIRED_DELAY, EXPIRED, OLD_PATH, 1);
  }

  @Test
  public void typicalMapDropPartitionEvent() {
    setupMocks(dropPartitionEvent, true, true, true);
    when(dropPartitionEvent.getEventType()).thenReturn(EventType.DROP_PARTITION);
    when(dropPartitionEvent.getPartitionLocation()).thenReturn(OLD_PATH);
    MessageEvent messageEvent = newMessageEvent(dropPartitionEvent);
    Optional<PathEvents> pathEvent = mapper.map(messageEvent);
    assertPath(messageEvent, pathEvent, CLEANUP_UNREFERENCED_DELAY, UNREFERENCED, OLD_PATH, 0);
  }

  @Test
  public void typicalMapDropTableEvent() {
    setupMocks(dropTableEvent, true,true, true);
    when(dropTableEvent.getEventType()).thenReturn(EventType.DROP_TABLE);
    when(dropTableEvent.getTableLocation()).thenReturn(OLD_PATH);
    MessageEvent messageEvent = newMessageEvent(dropTableEvent);
    Optional<PathEvents> pathEvent = mapper.map(messageEvent);
    assertPath(messageEvent, pathEvent, CLEANUP_UNREFERENCED_DELAY, UNREFERENCED, OLD_PATH, 0);
  }

  @Test
  public void typicalMapDefaultDelay() {
    setupMocks(alterPartitionEvent, true, true, false);
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    when(alterPartitionEvent.getPartitionLocation()).thenReturn(PATH);
    when(alterPartitionEvent.getOldPartitionLocation()).thenReturn(OLD_PATH);
    when(alterPartitionEvent.getTableLocation()).thenReturn(PATH);
    MessageEvent messageEvent = newMessageEvent(alterPartitionEvent);
    Optional<PathEvents> pathEvent = mapper.map(messageEvent);
    assertPath(messageEvent, pathEvent, DEFAULT_UNREFERENCED_CLEANUP_DELAY, UNREFERENCED, OLD_PATH,0);
    assertPath(messageEvent, pathEvent, DEFAULT_EXPIRED_CLEANUP_DELAY, EXPIRED, PATH, 1);
  }

  // TODO: More discovery over what this is suppose to be doing.
  private void assertPath(MessageEvent messageEvent,
                          Optional<PathEvents> pathEventsOptional,
                          String cleanupDelay,
                          CleanupType cleanupType,
                          String pathToCleanup,
                          int index) {
    PathEvents pathEvents = pathEventsOptional.get();
    assertThat(pathEvents.getMessageEvent()).isEqualTo(messageEvent);
    List<HousekeepingPath> paths = pathEvents.getHousekeepingPaths();
    HousekeepingPath path = pathEvents.getHousekeepingPaths().get(index);
  }

  private void assertPath(MessageEvent messageEvent, Optional<PathEvent> pathEventOptional, String cleanupDelay) {
    PathEvent pathEvent = pathEventOptional.get();
    assertThat(pathEvent.getMessageEvent()).isEqualTo(messageEvent);
    HousekeepingPath path = pathEvent.getHousekeepingPath();
    LocalDateTime now = LocalDateTime.now();
    assertThat(path.getPath()).isEqualTo(pathToCleanup);
    assertThat(path.getTableName()).isEqualTo(TABLE);
    assertThat(path.getDatabaseName()).isEqualTo(DATABASE);
    assertThat(path.getCleanupAttempts()).isEqualTo(CLEANUP_ATTEMPTS);
    assertThat(path.getCleanupDelay()).isEqualTo(Duration.parse(cleanupDelay));
    assertThat(path.getCleanupType()).isEqualToIgnoringCase(cleanupType.toString());
    assertThat(path.getModifiedTimestamp()).isNull();
    assertThat(path.getCreationTimestamp()).isBetween(CREATION_TIMESTAMP, now);
    assertThat(path.getCleanupTimestamp())
        .isEqualTo(path.getCreationTimestamp().plus(Duration.parse(cleanupDelay)));
    assertThat(pathEvents.getMessageEvent().getMessageProperties()).isEqualTo(newMessageProperties());
  }

  @Test
  public void mapDefaultDelayOnDurationException() {
    setupMocks(alterPartitionEvent);
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    when(alterPartitionEvent.getOldPartitionLocation()).thenReturn(OLD_PATH);
    when(alterPartitionEvent.getTableParameters())
      .thenReturn(Collections.singletonMap(CLEANUP_DELAY_PROPERTY, "1"));
    MessageEvent messageEvent = newMessageEvent(alterPartitionEvent);
    Optional<PathEvent> pathEvent = mapper.map(messageEvent);
    assertPath(messageEvent, pathEvent, DEFAULT_CLEANUP_DELAY);
  }

  private MessageEvent newMessageEvent(ListenerEvent event) {
    return new MessageEvent(event, newMessageProperties());
  }

  private Map<MessageProperty, String> newMessageProperties() {
    return Collections.singletonMap(SqsMessageProperty.SQS_MESSAGE_RECEIPT_HANDLE, RECEIPT_HANDLE);
  }

  private void setupMocks(ListenerEvent event, Boolean isUnreferenced, Boolean isExpired, Boolean delaysDefined) {
    Map<String,String> tableParams = new HashMap<>();

    tableParams.put(UNREFERENCED.tableParameterName(),isUnreferenced.toString().toLowerCase());
    tableParams.put(EXPIRED.tableParameterName(),isExpired.toString().toLowerCase());

    if ( delaysDefined ) {
      tableParams.put(CLEANUP_UNREFERENCED_DELAY_PROPERTY, CLEANUP_UNREFERENCED_DELAY);
      tableParams.put(CLEANUP_EXPIRED_DELAY_PROPERTY, CLEANUP_EXPIRED_DELAY);
    }

    when(event.getTableParameters()).thenReturn(tableParams);
    when(event.getDbName()).thenReturn(DATABASE);
    when(event.getTableName()).thenReturn(TABLE);
  }
}
