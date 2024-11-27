/**
 * Copyright (C) 2019-2023 Expedia, Inc.
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

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.FAILED_TO_SCHEDULE;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.PeriodDuration;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;
import com.expediagroup.beekeeper.core.service.BeekeeperHistoryService;

@ExtendWith(MockitoExtension.class)
public class UnreferencedHousekeepingPathSchedulerServiceTest {

  @Mock
  private HousekeepingPathRepository housekeepingPathRepository;

  @Mock
  private BeekeeperHistoryService beekeeperHistoryService;

  @InjectMocks
  private UnreferencedHousekeepingPathSchedulerService unreferencedHousekeepingPathSchedulerService;

  @Test
  public void typicalScheduleForHousekeeping() {
    HousekeepingPath path = HousekeepingPath
        .builder()
        .creationTimestamp(LocalDateTime.now())
        .cleanupDelay(PeriodDuration.parse("P3D"))
        .build();
    unreferencedHousekeepingPathSchedulerService.scheduleForHousekeeping(path);

    verify(housekeepingPathRepository).save(path);

    verify(beekeeperHistoryService).saveHistory(path, SCHEDULED);
  }

  @Test
  public void verifyLifecycleType() {
    assertThat(unreferencedHousekeepingPathSchedulerService.getLifecycleEventType()).isEqualTo(UNREFERENCED);
  }

  @Test
  public void scheduleFails() {
    HousekeepingPath path = HousekeepingPath
        .builder()
        .path("path_to_schedule")
        .creationTimestamp(LocalDateTime.now())
        .cleanupDelay(PeriodDuration.parse("P3D"))
        .build();

    when(housekeepingPathRepository.save(path)).thenThrow(new RuntimeException());

    assertThatExceptionOfType(BeekeeperException.class)
        .isThrownBy(() -> unreferencedHousekeepingPathSchedulerService.scheduleForHousekeeping(path))
        .withMessage(format("Unable to schedule %s", path));
    verify(housekeepingPathRepository).save(path);
    verify(beekeeperHistoryService).saveHistory(any(), eq(FAILED_TO_SCHEDULE));
  }
}
