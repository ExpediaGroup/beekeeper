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
package com.expediagroup.beekeeper.scheduler.apiary.generator.utils;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;

import com.expediagroup.beekeeper.core.error.BeekeeperException;

@ExtendWith(MockitoExtension.class)
public class CleanupDelayExtractorTest {

  private static final String EXTRACTOR_KEY = "beekeeper.expired.data.retention.period";
  private static final String EXTRACTOR_DEFAULT_VALUE = "P7D";

  @Mock
  private AlterTableEvent listenerEvent;
  private final CleanupDelayExtractor cleanupDelayExtractor = new CleanupDelayExtractor(EXTRACTOR_KEY,
      EXTRACTOR_DEFAULT_VALUE);

  @Test
  public void typicalExtractCleanUpDelay() {
    String cleanupDelayInput = "P30D";
    when(listenerEvent.getTableParameters()).thenReturn(Map.of(EXTRACTOR_KEY, cleanupDelayInput));
    Duration cleanupDelay = cleanupDelayExtractor.extractCleanupDelay(listenerEvent);
    assertThat(cleanupDelay).isEqualTo(Duration.parse(cleanupDelayInput));
  }

  @Test
  public void noKeyExtractCleanUpDelay() {
    when(listenerEvent.getTableParameters()).thenReturn(Map.of());
    Duration cleanupDelay = cleanupDelayExtractor.extractCleanupDelay(listenerEvent);
    assertThat(cleanupDelay).isEqualTo(Duration.parse(EXTRACTOR_DEFAULT_VALUE));
  }

  @Test
  public void parseErrorExtractCleanUpDelay() {
    when(listenerEvent.getTableParameters()).thenReturn(Map.of(EXTRACTOR_KEY, "1"));
    assertThrows(BeekeeperException.class, () -> {
      cleanupDelayExtractor.extractCleanupDelay(listenerEvent);
    });
  }

  @Test
  public void invalidDefaultCleanupDelay() {
    String defaultDelay = "1";
    assertThatExceptionOfType(BeekeeperException.class)
        .isThrownBy(() -> new CleanupDelayExtractor(EXTRACTOR_KEY, defaultDelay))
        .withMessage(format("Default delay value '%s' for key '%s' cannot be parsed to a Duration", defaultDelay,
            EXTRACTOR_KEY));
  }
}
