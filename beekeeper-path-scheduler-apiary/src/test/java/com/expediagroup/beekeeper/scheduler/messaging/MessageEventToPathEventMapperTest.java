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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

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
import com.expediagroup.beekeeper.scheduler.apiary.model.PathEvent;

@ExtendWith(MockitoExtension.class)
@ExtendWith(SpringExtension.class)
@TestPropertySource(properties = { "properties.beekeeper.default-cleanup-delay=P3D",
                                   "properties.apiary.cleanup-delay-property-key=beekeeper.unreferenced.data.retention.period" })
@ContextConfiguration(classes = { MessageEventToPathEventMapper.class },
    loader = AnnotationConfigContextLoader.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class MessageEventToPathEventMapperTest {

  private static final String RECEIPT_HANDLE = "receiptHandle";
  private static final String DATABASE = "database";
  private static final String TABLE = "table";
  private static final String OLD_PATH = "old_path";
  private static final Integer CLEANUP_ATTEMPTS = 0;
  private static final String CLEANUP_DELAY = "P7D";
  private static final String CLEANUP_DELAY_PROPERTY = "beekeeper.unreferenced.data.retention.period";
  private static final String DEFAULT_CLEANUP_DELAY = "P3D";
  private static final LocalDateTime CREATION_TIMESTAMP = LocalDateTime.now();

  private @Mock AlterPartitionEvent alterPartitionEvent;
  private @Mock AlterTableEvent alterTableEvent;
  private @Mock DropPartitionEvent dropPartitionEvent;
  private @Mock DropTableEvent dropTableEvent;

  @Autowired
  private MessageEventToPathEventMapper mapper;

  @Test
  public void typicalMapAlterPartitionEvent() {
    setupMocks(alterPartitionEvent);
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    when(alterPartitionEvent.getOldPartitionLocation()).thenReturn(OLD_PATH);
    MessageEvent messageEvent = newMessageEvent(alterPartitionEvent);
    Optional<PathEvent> pathEvent = mapper.map(messageEvent);
    assertPath(messageEvent, pathEvent, CLEANUP_DELAY);
  }

  @Test
  public void typicalMapAlterTableEvent() {
    setupMocks(alterTableEvent);
    when(alterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);
    when(alterTableEvent.getOldTableLocation()).thenReturn(OLD_PATH);
    MessageEvent messageEvent = newMessageEvent(alterTableEvent);
    Optional<PathEvent> pathEvent = mapper.map(messageEvent);
    assertPath(messageEvent, pathEvent, CLEANUP_DELAY);
  }

  @Test
  public void typicalMapDropPartitionEvent() {
    setupMocks(dropPartitionEvent);
    when(dropPartitionEvent.getEventType()).thenReturn(EventType.DROP_PARTITION);
    when(dropPartitionEvent.getPartitionLocation()).thenReturn(OLD_PATH);
    MessageEvent messageEvent = newMessageEvent(dropPartitionEvent);
    Optional<PathEvent> pathEvent = mapper.map(messageEvent);
    assertPath(messageEvent, pathEvent, CLEANUP_DELAY);
  }

  @Test
  public void typicalMapDropTableEvent() {
    setupMocks(dropTableEvent);
    when(dropTableEvent.getEventType()).thenReturn(EventType.DROP_TABLE);
    when(dropTableEvent.getTableLocation()).thenReturn(OLD_PATH);
    MessageEvent messageEvent = newMessageEvent(dropTableEvent);
    Optional<PathEvent> pathEvent = mapper.map(messageEvent);
    assertPath(messageEvent, pathEvent, CLEANUP_DELAY);
  }

  @Test
  public void typicalMapDefaultDelay() {
    setupMocks(alterPartitionEvent);
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    when(alterPartitionEvent.getOldPartitionLocation()).thenReturn(OLD_PATH);
    when(alterPartitionEvent.getTableParameters()).thenReturn(Collections.emptyMap());
    MessageEvent messageEvent = newMessageEvent(alterPartitionEvent);
    Optional<PathEvent> pathEvent = mapper.map(messageEvent);
    assertPath(messageEvent, pathEvent, DEFAULT_CLEANUP_DELAY);
  }

  private void assertPath(MessageEvent messageEvent, Optional<PathEvent> pathEventOptional, String cleanupDelay) {
    PathEvent pathEvent = pathEventOptional.get();
    assertThat(pathEvent.getMessageEvent()).isEqualTo(messageEvent);
    HousekeepingPath path = pathEvent.getHousekeepingPath();
    LocalDateTime now = LocalDateTime.now();
    assertThat(path.getPath()).isEqualTo(OLD_PATH);
    assertThat(path.getTableName()).isEqualTo(TABLE);
    assertThat(path.getDatabaseName()).isEqualTo(DATABASE);
    assertThat(path.getCleanupAttempts()).isEqualTo(CLEANUP_ATTEMPTS);
    assertThat(path.getCleanupDelay()).isEqualTo(Duration.parse(cleanupDelay));
    assertThat(path.getModifiedTimestamp()).isNull();
    assertThat(path.getCreationTimestamp()).isBetween(CREATION_TIMESTAMP, now);
    assertThat(path.getCleanupTimestamp())
        .isEqualTo(path.getCreationTimestamp().plus(Duration.parse(cleanupDelay)));
    assertThat(pathEvent.getMessageEvent().getMessageProperties()).isEqualTo(newMessageProperties());
  }

  private MessageEvent newMessageEvent(ListenerEvent event) {
    return new MessageEvent(event, newMessageProperties());
  }

  private Map<MessageProperty, String> newMessageProperties() {
    return Collections.singletonMap(SqsMessageProperty.SQS_MESSAGE_RECEIPT_HANDLE, RECEIPT_HANDLE);
  }

  private void setupMocks(ListenerEvent event) {
    when(event.getTableParameters())
        .thenReturn(Collections.singletonMap(CLEANUP_DELAY_PROPERTY, CLEANUP_DELAY));
    when(event.getDbName()).thenReturn(DATABASE);
    when(event.getTableName()).thenReturn(TABLE);
  }
}
