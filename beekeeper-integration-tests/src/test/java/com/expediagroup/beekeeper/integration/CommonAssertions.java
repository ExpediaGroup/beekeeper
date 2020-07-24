package com.expediagroup.beekeeper.integration;

import static org.assertj.core.api.Assertions.assertThat;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.CLEANUP_ATTEMPTS_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.CLEANUP_DELAY_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.CLIENT_ID_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.CREATION_TIMESTAMP_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.DATABASE_NAME_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.TABLE_NAME_VALUE;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.scheduler.apiary.BeekeeperSchedulerApiary;

public class CommonAssertions {

  public static void assertHousekeepingEntity(HousekeepingEntity actual, String expectedPath, LifecycleEventType expectedLifecycleEventType) {
    assertThat(actual.getPath()).isEqualTo(expectedPath);
    assertThat(actual.getDatabaseName()).isEqualTo(DATABASE_NAME_VALUE);
    assertThat(actual.getTableName()).isEqualTo(TABLE_NAME_VALUE);
    assertThat(actual.getHousekeepingStatus()).isEqualTo(SCHEDULED);
    assertThat(actual.getCreationTimestamp()).isAfterOrEqualTo(CREATION_TIMESTAMP_VALUE);
    assertThat(actual.getModifiedTimestamp()).isAfterOrEqualTo(CREATION_TIMESTAMP_VALUE);
    assertThat(actual.getCleanupTimestamp()).isEqualTo(actual.getCreationTimestamp().plus(actual.getCleanupDelay()));
    assertThat(actual.getCleanupDelay()).isEqualTo(Duration.parse(CLEANUP_DELAY_VALUE));
    assertThat(actual.getCleanupAttempts()).isEqualTo(CLEANUP_DELAY_VALUE);
    assertThat(actual.getClientId()).isEqualTo(CLIENT_ID_VALUE);
    assertThat(actual.getLifecycleType()).isEqualTo(expectedLifecycleEventType.toString());
  }

  public static void assertMetrics(String... metrics) {
    Set<MeterRegistry> meterRegistry = ((CompositeMeterRegistry) BeekeeperSchedulerApiary.meterRegistry())
        .getRegistries();
    assertThat(meterRegistry).hasSize(2);
    meterRegistry.forEach(registry -> {
      List<Meter> meters = registry.getMeters();
      assertThat(meters).extracting("id", Meter.Id.class).extracting("name").contains(metrics);
    });
  }
}
