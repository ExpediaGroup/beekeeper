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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;

@ExtendWith(MockitoExtension.class)
public class CleanupDelayExtractorTest {

  private static final String EXTRACTOR_KEY = "beekeeper.expired.data.retention.period";
  private static final String EXTRACTOR_DEFAULT_VALUE = "P7D";

  @Mock private AlterTableEvent listenerEvent;
  private CleanupDelayExtractor cleanupDelayExtractor;

  @BeforeEach
  public void setup() {
    cleanupDelayExtractor = new CleanupDelayExtractor(EXTRACTOR_KEY, EXTRACTOR_DEFAULT_VALUE);
  }

  @Test
  public void typicalExtractCleanUpDelay() {
    String cleanupDelayInput = "P30D";
    when(listenerEvent.getTableParameters()).thenReturn(Map.of(EXTRACTOR_KEY, cleanupDelayInput));
    Duration cleanupDelay = cleanupDelayExtractor.extractCleanupDelay(listenerEvent);
    assertThat(cleanupDelay).isEqualTo(Duration.parse(cleanupDelayInput));
  }

  @Test
  public void NoKeyExtractCleanUpDelay() {
    when(listenerEvent.getTableParameters()).thenReturn(Map.of());
    Duration cleanupDelay = cleanupDelayExtractor.extractCleanupDelay(listenerEvent);
    assertThat(cleanupDelay).isEqualTo(Duration.parse(EXTRACTOR_DEFAULT_VALUE));
  }

  @Test
  public void parseErrorExtractCleanUpDelay() {
    when(listenerEvent.getTableParameters()).thenReturn(Map.of(EXTRACTOR_KEY, "1"));
    Duration cleanupDelay = cleanupDelayExtractor.extractCleanupDelay(listenerEvent);
    assertThat(cleanupDelay).isEqualTo(Duration.parse(EXTRACTOR_DEFAULT_VALUE));
  }
}
