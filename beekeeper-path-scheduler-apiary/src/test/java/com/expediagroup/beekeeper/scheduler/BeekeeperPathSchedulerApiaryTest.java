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
    return;
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

    when(contextMock.getBean("meterRegistry"))
        .thenReturn(defineMeterRegistry ? meterRegistryMock : null);

    when(contextMock.getBean("pathSchedulerApiaryRunner"))
        .thenReturn(defineRunner ? runnerMock : null);

    if (defineContext) {
      beekeeperPathSchedulerApiary.setApplicationContext(contextMock);
    }
  }
}
