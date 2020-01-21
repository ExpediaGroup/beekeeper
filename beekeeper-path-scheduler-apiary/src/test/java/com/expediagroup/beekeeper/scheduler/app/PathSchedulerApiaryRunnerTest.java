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
package com.expediagroup.beekeeper.scheduler.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.awaitility.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.ApplicationArguments;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.scheduler.apiary.app.PathSchedulerApiaryRunner;
import com.expediagroup.beekeeper.scheduler.apiary.service.PathSchedulerApiary;

@ExtendWith(MockitoExtension.class)
public class PathSchedulerApiaryRunnerTest {

  private final ExecutorService executor = Executors.newFixedThreadPool(1);
  @Mock private ApplicationArguments args;
  @Mock private PathSchedulerApiary pathSchedulerApiary;
  private PathSchedulerApiaryRunner pathSchedulerApiaryRunner;

  @BeforeEach
  public void init() {
    pathSchedulerApiaryRunner = new PathSchedulerApiaryRunner(pathSchedulerApiary);
  }

  @Test
  public void typicalRun() throws Exception {
    runRunner();
    await().atMost(Duration.FIVE_SECONDS)
        .untilAsserted(() -> verify(pathSchedulerApiary, atLeast(1)).scheduleBeekeeperEvent());
    destroy();
    verify(pathSchedulerApiary).close();
  }

  @Test
  public void typicalRunWithException() throws Exception {
    doThrow(new RuntimeException())
        .doNothing()
        .when(pathSchedulerApiary)
        .scheduleBeekeeperEvent();
    runRunner();
    await().atMost(Duration.FIVE_SECONDS)
        .untilAsserted(() -> verify(pathSchedulerApiary, atLeast(2)).scheduleBeekeeperEvent());
    destroy();
    verify(pathSchedulerApiary).close();
  }

  @Test
  public void typicalRunSchedulerTimesoutOnDestroy() throws Exception {
    doAnswer(answer -> {
      Thread.sleep(15000L);
      return null;
    }).when(pathSchedulerApiary)
        .scheduleBeekeeperEvent();

    try {
      runRunner();
      await().atMost(Duration.FIVE_SECONDS)
          .untilAsserted(() -> verify(pathSchedulerApiary, atLeast(1)).scheduleBeekeeperEvent());
      destroy();
      fail("Runner should have thrown exception");
    } catch (Exception e) {
      assertThat(e).isInstanceOf(BeekeeperException.class);
      assertThat(e.getMessage()).isEqualTo("Runner taking too long to shut down");
      verify(pathSchedulerApiary).close();
    }
  }

  private void runRunner() {
    executor.execute(() -> {
      try {
        pathSchedulerApiaryRunner.run(args);
      } catch (Exception e) {
        fail("Exception thrown on run");
      }
    });
  }

  private void destroy() throws InterruptedException {
    pathSchedulerApiaryRunner.destroy();
    executor.awaitTermination(1, TimeUnit.SECONDS);
  }
}
