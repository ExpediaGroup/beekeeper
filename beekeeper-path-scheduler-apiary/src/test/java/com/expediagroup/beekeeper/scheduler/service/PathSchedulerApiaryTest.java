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
package com.expediagroup.beekeeper.scheduler.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

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
import com.expediagroup.beekeeper.scheduler.apiary.messaging.PathEventReader;
import com.expediagroup.beekeeper.scheduler.apiary.model.PathEvent;
import com.expediagroup.beekeeper.scheduler.apiary.service.PathSchedulerApiary;

@ExtendWith(MockitoExtension.class)
public class PathSchedulerApiaryTest {

  private static final String PATH = "path";

  @Mock
  private SchedulerService pathSchedulerService;

  @Mock
  private PathEventReader pathEventReader;

  @Mock
  private EntityHousekeepingPath path;

  private PathSchedulerApiary scheduler;

  @BeforeEach
  public void init() {
    scheduler = new PathSchedulerApiary(pathEventReader, pathSchedulerService);
  }

  @Test
  public void typicalSchedule() {
    Optional<PathEvent> event = Optional.of(newPathEvent(path));
    when(pathEventReader.read()).thenReturn(event);
    scheduler.schedulePath();
    verify(pathSchedulerService).scheduleForHousekeeping(path);
    verify(pathEventReader).delete(event.get());
  }

  @Test
  public void typicalNoSchedule() {
    when(pathEventReader.read()).thenReturn(Optional.empty());
    scheduler.schedulePath();
    verifyZeroInteractions(pathSchedulerService);
    verify(pathEventReader, times(0)).delete(any());
  }

  @Test
  public void repositoryThrowsException() {
    when(path.getPath()).thenReturn(PATH);
    Optional<PathEvent> event = Optional.of(newPathEvent(path));
    when(pathEventReader.read()).thenReturn(event);
    doThrow(new BeekeeperException("exception")).when(pathSchedulerService).scheduleForHousekeeping(path);

    try {
      scheduler.schedulePath();
      fail("Should have thrown exception");
    } catch (Exception e) {
      verify(pathSchedulerService).scheduleForHousekeeping(path);
      verify(pathEventReader, times(0)).delete(any());
      assertThat(e).isInstanceOf(BeekeeperException.class);
      assertThat(e.getMessage()).isEqualTo(
          "Unable to schedule path 'path' for deletion, this message will go back on the queue");
    }
  }

  @Test
  public void typicalClose() throws Exception {
    scheduler.close();
    verify(pathEventReader, times(1)).close();
  }

  private PathEvent newPathEvent(EntityHousekeepingPath path) {
    return new PathEvent(path, Mockito.mock(MessageEvent.class));
  }
}
