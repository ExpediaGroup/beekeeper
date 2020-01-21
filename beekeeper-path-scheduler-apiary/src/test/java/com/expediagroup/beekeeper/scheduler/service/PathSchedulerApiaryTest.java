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
package com.expediagroup.beekeeper.scheduler.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

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
import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.BeekeeperEventReader;
import com.expediagroup.beekeeper.scheduler.apiary.model.BeekeeperEvent;
import com.expediagroup.beekeeper.scheduler.apiary.service.PathSchedulerApiary;

@ExtendWith(MockitoExtension.class)
public class PathSchedulerApiaryTest {

  private static final String PATH = "path";

  @Mock private SchedulerService pathSchedulerService;
  @Mock private BeekeeperEventReader beekeeperEventReader;
  @Mock private EntityHousekeepingPath path;

  private PathSchedulerApiary scheduler;

  @BeforeEach
  public void init() {
    EnumMap schedulerMap = new EnumMap(LifecycleEventType.class);
    schedulerMap.put(UNREFERENCED, pathSchedulerService);
    scheduler = new PathSchedulerApiary(beekeeperEventReader, schedulerMap);
  }

  @Test
  public void typicalSchedule() {
    Optional<BeekeeperEvent> event = Optional.of(newPathEvent(path, UNREFERENCED));
    when(beekeeperEventReader.read()).thenReturn(event);
    scheduler.scheduleBeekeeperEvent();
    verify(pathSchedulerService).scheduleForHousekeeping(path);
    verify(beekeeperEventReader).delete(event.get());
  }

  @Test
  public void typicalNoSchedule() {
    when(beekeeperEventReader.read()).thenReturn(Optional.empty());
    scheduler.scheduleBeekeeperEvent();
    verifyZeroInteractions(pathSchedulerService);
    verify(beekeeperEventReader, times(0)).delete(any());
  }

  @Test
  public void repositoryThrowsException() {
    when(path.getPath()).thenReturn(PATH);
    Optional<BeekeeperEvent> event = Optional.of(newPathEvent(path, UNREFERENCED));
    when(beekeeperEventReader.read()).thenReturn(event);
    doThrow(new BeekeeperException("exception")).when(pathSchedulerService).scheduleForHousekeeping(path);

    try {
      scheduler.scheduleBeekeeperEvent();
      fail("Should have thrown exception");
    } catch (Exception e) {
      verify(pathSchedulerService).scheduleForHousekeeping(path);
      verify(beekeeperEventReader, times(0)).delete(any());
      assertThat(e).isInstanceOf(BeekeeperException.class);
      assertThat(e.getMessage()).isEqualTo(
          "Unable to schedule path 'path' for deletion, this message will go back on the queue");
    }
  }

  @Test
  public void typicalClose() throws Exception {
    scheduler.close();
    verify(beekeeperEventReader, times(1)).close();
  }

  private BeekeeperEvent newPathEvent(EntityHousekeepingPath path, LifecycleEventType lifecycleEventType) {
    when(path.getLifecycleType()).thenReturn(lifecycleEventType.name());
    return new BeekeeperEvent(List.of(path), Mockito.mock(MessageEvent.class));
  }
}
