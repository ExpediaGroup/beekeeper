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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.mockito.Mock;

import com.expedia.apiary.extensions.receiver.common.event.EventType;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;

import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.scheduler.apiary.generator.utils.CleanupDelayExtractor;

public class HousekeepingEntityGeneratorTestBase {

  protected static final String CLIENT_ID = "client-id";
  protected static final String DATABASE = "database";
  protected static final String TABLE = "table";
  protected static final Integer CLEANUP_ATTEMPTS = 0;
  protected static final Duration CLEANUP_DELAY = Duration.ofDays(30L);
  protected static final Clock FIXED_CLOCK = Clock.fixed(Instant.parse("2020-01-01T00:00:00.00Z"), ZoneId.of("UTC"));

  @Mock protected CleanupDelayExtractor cleanupDelayExtractor;
  @Mock protected Clock clock;

  protected void setupListenerEvent(ListenerEvent listenerEvent, EventType eventType) {
    when(listenerEvent.getDbName()).thenReturn(DATABASE);
    when(listenerEvent.getTableName()).thenReturn(TABLE);
    when(listenerEvent.getEventType()).thenReturn(eventType);
  }

  protected void setupClockAndExtractor(ListenerEvent listenerEvent) {
    when(clock.instant()).thenReturn(FIXED_CLOCK.instant());
    when(clock.getZone()).thenReturn(FIXED_CLOCK.getZone());
    when(cleanupDelayExtractor.extractCleanupDelay(listenerEvent)).thenReturn(CLEANUP_DELAY);
  }

  protected void assertHousekeepingEntity(HousekeepingEntity housekeepingEntity,
      LifecycleEventType lifecycleEventType) {
    LocalDateTime creationTimestamp = LocalDateTime.now(FIXED_CLOCK);
    assertThat(LifecycleEventType.valueOf(housekeepingEntity.getLifecycleType())).isEqualTo(lifecycleEventType);
    assertThat(housekeepingEntity.getHousekeepingStatus()).isEqualTo(SCHEDULED);
    assertThat(housekeepingEntity.getTableName()).isEqualTo(TABLE);
    assertThat(housekeepingEntity.getDatabaseName()).isEqualTo(DATABASE);
    assertThat(housekeepingEntity.getCleanupAttempts()).isEqualTo(CLEANUP_ATTEMPTS);
    assertThat(housekeepingEntity.getCleanupDelay()).isEqualTo(CLEANUP_DELAY);
    assertThat(housekeepingEntity.getCleanupTimestamp()).isEqualTo(creationTimestamp.plus(CLEANUP_DELAY));
    assertThat(housekeepingEntity.getCreationTimestamp()).isEqualTo(creationTimestamp);
    assertThat(housekeepingEntity.getModifiedTimestamp()).isNull();
  }
}
