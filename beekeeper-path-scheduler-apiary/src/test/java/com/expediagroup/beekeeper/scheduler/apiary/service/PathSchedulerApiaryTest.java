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
package com.expediagroup.beekeeper.scheduler.apiary.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.util.ArrayList;
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
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.scheduler.apiary.messaging.BeekeeperEventReader;
import com.expediagroup.beekeeper.scheduler.apiary.model.BeekeeperEvent;

@ExtendWith(MockitoExtension.class)
public class PathSchedulerApiaryTest {

  private static final String PATH = "path";

  @Mock private SchedulerService pathSchedulerService;
  @Mock private BeekeeperEventReader beekeeperEventReader;
  @Mock private EntityHousekeepingPath path;

  private PathSchedulerApiary scheduler;

  @BeforeEach
  public void init() {
    scheduler = new PathSchedulerApiary(beekeeperEventReader, pathSchedulerService);
  }

  @Test
  public void typicalSchedule() {
    when(path.getLifecycleType()).thenReturn(UNREFERENCED.toString());
    Optional<BeekeeperEvent> event = Optional.of(newPathEvent(path));
    when(beekeeperEventReader.read()).thenReturn(event);
    scheduler.schedulePath();
    verify(pathSchedulerService).scheduleForHousekeeping(path);
    verify(beekeeperEventReader).delete(event.get());
  }

  @Test
  public void typicalNoSchedule() {
    when(beekeeperEventReader.read()).thenReturn(Optional.empty());
    scheduler.schedulePath();
    verifyZeroInteractions(pathSchedulerService);
    verify(beekeeperEventReader, times(0)).delete(any());
  }


  @Test
  public void repositorySchedulingThrowsException() {
    when(path.getPath()).thenReturn(PATH);
    when(path.getLifecycleType()).thenReturn(UNREFERENCED.toString());
    Optional<BeekeeperEvent> event = Optional.of(newPathEvent(path));
    when(beekeeperEventReader.read()).thenReturn(event);
    doThrow(new BeekeeperException("exception")).when(pathSchedulerService).scheduleForHousekeeping(path);

    try {
      scheduler.schedulePath();
      fail("Should have thrown exception");
    } catch (Exception e) {
      verify(pathSchedulerService).scheduleForHousekeeping(path);
      verify(beekeeperEventReader, times(0)).delete(any());
      assertThat(e).isInstanceOf(BeekeeperException.class);
      assertThat(e.getMessage()).isEqualTo(
              "Unable to schedule/update path 'path' for deletion, this message will go back on the queue");
    }
  }

  @Test
  public void repositoryCleanupExpiredThrowsException() {
    when(path.getPath()).thenReturn(PATH);
    when(path.getLifecycleType()).thenReturn(EXPIRED.toString());
    Optional<BeekeeperEvent> event = Optional.of(newPathEvent(path));
    when(beekeeperEventReader.read()).thenReturn(event);
    doThrow(new BeekeeperException("exception")).when(pathSchedulerService).scheduleExpiration(any(HousekeepingPath.class));

    try {
      scheduler.schedulePath();
      fail("Should have thrown exception");
    } catch (Exception e) {
      verify(pathSchedulerService).scheduleExpiration(any(HousekeepingPath.class));
      verify(beekeeperEventReader, times(0)).delete(any());
      assertThat(e).isInstanceOf(BeekeeperException.class);
      assertThat(e.getMessage()).isEqualTo(
              "Unable to schedule/update path 'path' for deletion, this message will go back on the queue");
    }
  }

  @Test
  public void typicalClose() throws Exception {
    scheduler.close();
    verify(beekeeperEventReader, times(1)).close();
  }

  private BeekeeperEvent newPathEvent(EntityHousekeepingPath path) {
    List<HousekeepingPath> paths = new ArrayList<>();
    paths.add(path);
    return new BeekeeperEvent(paths, Mockito.mock(MessageEvent.class));
  }
}
