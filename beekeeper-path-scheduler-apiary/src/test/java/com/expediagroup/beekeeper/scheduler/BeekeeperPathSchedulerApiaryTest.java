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
package com.expediagroup.beekeeper.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ConfigurableApplicationContext;

import io.micrometer.core.instrument.MeterRegistry;

import com.expediagroup.beekeeper.scheduler.apiary.BeekeeperPathSchedulerApiary;
import com.expediagroup.beekeeper.scheduler.apiary.app.PathSchedulerApiaryRunner;

@ExtendWith(MockitoExtension.class)
public class BeekeeperPathSchedulerApiaryTest {

  private @Mock MeterRegistry meterRegistryMock;
  private @Mock ConfigurableApplicationContext contextMock;
  private @Mock PathSchedulerApiaryRunner runnerMock;

  @BeforeEach()
  public void resetContext() {
    BeekeeperPathSchedulerApiary.resetStaticContext();
  }

  @Test
  public void verifyApplicationContext() {
    setupContextMock(true, true, true);
  }

  @Test
  public void verifyIsRunning() {
    setupContextMock(true, true, true);
    when(contextMock.isRunning()).thenReturn(true);
    boolean isRunning = BeekeeperPathSchedulerApiary.isRunning();
    assertThat(isRunning).isTrue();
  }

  @Test
  public void verifyMeterRegistry() {
    setupContextMock(true, true, true);
    MeterRegistry meterRegistry = BeekeeperPathSchedulerApiary.meterRegistry();
    assertThat(meterRegistry).isInstanceOf(MeterRegistry.class);
  }

  @Test
  public void typicalStop() {
    setupContextMock(true, true, true);
    BeekeeperPathSchedulerApiary.stop();
    verify(runnerMock).destroy();
    verify(contextMock).close();
  }

  @Test
  public void stopExceptionThrownWhenRunnerIsNull() {
    setupContextMock(true, true, false);
    Exception exception = assertThrows(RuntimeException.class, () -> BeekeeperPathSchedulerApiary.stop());
    assertThat(exception.getMessage()).isEqualTo("Application runner has not been started.");
  }

  @Test
  public void stopExceptionThrownWhenContextIsNull() {
    Exception exception = assertThrows(RuntimeException.class, () -> BeekeeperPathSchedulerApiary.stop());
    assertThat(exception.getMessage()).isEqualTo("Application context has not been started.");
  }

  private void setupContextMock(boolean defineContext, boolean defineMeterRegistry, boolean defineRunner) {
    BeekeeperPathSchedulerApiary beekeeperPathSchedulerApiary = new BeekeeperPathSchedulerApiary();

    when(contextMock.getBean("compositeMeterRegistry"))
        .thenReturn(defineMeterRegistry ? meterRegistryMock : null);

    when(contextMock.getBean("pathSchedulerApiaryRunner"))
        .thenReturn(defineRunner ? runnerMock : null);

    if (defineContext) {
      beekeeperPathSchedulerApiary.setApplicationContext(contextMock);
    }
  }
}
