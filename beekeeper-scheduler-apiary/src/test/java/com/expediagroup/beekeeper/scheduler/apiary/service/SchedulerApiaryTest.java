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
package com.expediagroup.beekeeper.scheduler.apiary.service;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.util.EnumMap;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.BeekeeperEventReader;
import com.expediagroup.beekeeper.scheduler.apiary.model.BeekeeperEvent;
import com.expediagroup.beekeeper.scheduler.service.ExpiredHousekeepingMetadataSchedulerService;
import com.expediagroup.beekeeper.scheduler.service.SchedulerService;
import com.expediagroup.beekeeper.scheduler.service.UnreferencedHousekeepingPathSchedulerService;

@ExtendWith(MockitoExtension.class)
public class SchedulerApiaryTest {

  @Mock private UnreferencedHousekeepingPathSchedulerService pathSchedulerService;
  @Mock private ExpiredHousekeepingMetadataSchedulerService tableSchedulerService;
  @Mock private BeekeeperEventReader beekeeperEventReader;
  @Mock private HousekeepingPath path;
  @Mock private HousekeepingMetadata table;

  private SchedulerApiary scheduler;

  @BeforeEach
  public void init() {
    EnumMap<LifecycleEventType, SchedulerService> schedulerMap = new EnumMap<>(LifecycleEventType.class);
    schedulerMap.put(UNREFERENCED, pathSchedulerService);
    schedulerMap.put(EXPIRED, tableSchedulerService);
    scheduler = new SchedulerApiary(beekeeperEventReader, schedulerMap);
  }

  @Test
  public void typicalPathSchedule() {
    Optional<BeekeeperEvent> event = Optional.of(newHousekeepingEvent(path, UNREFERENCED));
    when(beekeeperEventReader.read()).thenReturn(event);
    scheduler.scheduleBeekeeperEvent();
    verify(pathSchedulerService).scheduleForHousekeeping(path);
    verifyNoInteractions(tableSchedulerService);
    verify(beekeeperEventReader).delete(event.get());
  }

  @Test
  public void typicalTableSchedule() {
    Optional<BeekeeperEvent> event = Optional.of(newHousekeepingEvent(table, EXPIRED));
    when(beekeeperEventReader.read()).thenReturn(event);
    scheduler.scheduleBeekeeperEvent();
    verify(tableSchedulerService).scheduleForHousekeeping(table);
    verifyNoInteractions(pathSchedulerService);
    verify(beekeeperEventReader).delete(event.get());
  }

  @Test
  public void typicalNoSchedule() {
    when(beekeeperEventReader.read()).thenReturn(Optional.empty());
    scheduler.scheduleBeekeeperEvent();
    verifyNoInteractions(pathSchedulerService);
    verifyNoInteractions(tableSchedulerService);
    verify(beekeeperEventReader, times(0)).delete(any());
  }

  @Test
  public void housekeepingPathRepositoryThrowsException() {
    Optional<BeekeeperEvent> event = Optional.of(newHousekeepingEvent(path, UNREFERENCED));
    when(beekeeperEventReader.read()).thenReturn(event);
    doThrow(new BeekeeperException("exception")).when(pathSchedulerService).scheduleForHousekeeping(path);

    try {
      scheduler.scheduleBeekeeperEvent();
      fail("Should have thrown exception");
    } catch (Exception e) {
      verify(pathSchedulerService).scheduleForHousekeeping(path);
      verify(beekeeperEventReader, times(0)).delete(any());
      verifyNoInteractions(tableSchedulerService);
      assertThat(e).isInstanceOf(BeekeeperException.class);
      assertThat(e.getMessage()).isEqualTo(
          format("Unable to schedule %s deletion for entity, this message will go back on the queue",
              path.getLifecycleType()));
    }
  }

  @Test
  public void housekeepingTableRepositoryThrowsException() {
    Optional<BeekeeperEvent> event = Optional.of(newHousekeepingEvent(table, EXPIRED));
    when(beekeeperEventReader.read()).thenReturn(event);
    doThrow(new BeekeeperException("exception")).when(tableSchedulerService).scheduleForHousekeeping(table);

    try {
      scheduler.scheduleBeekeeperEvent();
      fail("Should have thrown exception");
    } catch (Exception e) {
      verify(tableSchedulerService).scheduleForHousekeeping(table);
      verify(beekeeperEventReader, times(0)).delete(any());
      verifyNoInteractions(pathSchedulerService);
      assertThat(e).isInstanceOf(BeekeeperException.class);
      assertThat(e.getMessage()).isEqualTo(
          format("Unable to schedule %s deletion for entity, this message will go back on the queue",
              table.getLifecycleType()));
    }
  }

  @Test
  public void typicalClose() throws Exception {
    scheduler.close();
    verify(beekeeperEventReader, times(1)).close();
  }

  private BeekeeperEvent newHousekeepingEvent(HousekeepingEntity housekeepingEntity,
      LifecycleEventType lifecycleEventType) {
    when(housekeepingEntity.getLifecycleType()).thenReturn(lifecycleEventType.name());
    return new BeekeeperEvent(List.of(housekeepingEntity), Mockito.mock(MessageEvent.class));
  }
}
