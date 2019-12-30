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

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;

@ExtendWith(MockitoExtension.class)
public class PathSchedulerServiceTest {

  @Mock
  private HousekeepingPathRepository housekeepingPathRepository;

  @InjectMocks
  private PathSchedulerService pathSchedulerService;

  @Test
  public void typicalScheduleForHousekeeping() {
    EntityHousekeepingPath path = new EntityHousekeepingPath.Builder()
        .creationTimestamp(LocalDateTime.now())
        .cleanupDelay(Duration.parse("P3D"))
        .build();
    pathSchedulerService.scheduleForHousekeeping(path);
    verify(housekeepingPathRepository).save(path);
  }

  @Test
  public void scheduleFails() {
    EntityHousekeepingPath path = new EntityHousekeepingPath.Builder()
        .path("path_to_schedule")
        .creationTimestamp(LocalDateTime.now())
        .cleanupDelay(Duration.parse("P3D"))
        .build();

    when(housekeepingPathRepository.save(path)).thenThrow(new RuntimeException());

    assertThatExceptionOfType(BeekeeperException.class)
        .isThrownBy(() -> pathSchedulerService.scheduleForHousekeeping(path))
        .withMessage("Unable to schedule path 'path_to_schedule' for deletion");
    verify(housekeepingPathRepository).save(path);
  }
}
