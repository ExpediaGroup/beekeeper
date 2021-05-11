/**
 * Copyright (C) 2019-2021 Expedia, Inc.
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
package com.expediagroup.beekeeper.core.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.LocalDateTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.core.instrument.search.RequiredSearch;

import com.expediagroup.beekeeper.core.TestApplication;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;

@ExtendWith(SpringExtension.class)
@ExtendWith(MockitoExtension.class)
@ContextConfiguration(classes = { TestApplication.class, MonitoredClass.class },
  loader = AnnotationConfigContextLoader.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class TimedTaggableAspectTest {

  public static final String TIMER_NAME = "metric-timer";
  private static final String DATABASE = "database";
  private static final String TABLE = "table";
  private static final String TABLE_2 = "table2";

  private HousekeepingPath housekeepingPath;
  
  @Autowired
  private MeterRegistry meterRegistry;

  @Autowired
  private MonitoredClass monitoredClass;

  @BeforeEach
  public void init() {
    housekeepingPath = HousekeepingPath.builder()
      .databaseName(DATABASE)
      .tableName(TABLE)
      .creationTimestamp(LocalDateTime.now())
      .cleanupDelay(Duration.ofDays(1))
      .build();
  }
  
  @Test
  public void typicalTime() {
    monitoredClass.doSomething(housekeepingPath);
    housekeepingPath.setTableName(TABLE_2);
    monitoredClass.doSomething(housekeepingPath);
    Timer timer1 = RequiredSearch.in(meterRegistry)
      .name(TIMER_NAME)
      .tags("table", String.join(".", DATABASE, TABLE))
      .timer();
    Timer timer2 = RequiredSearch.in(meterRegistry)
      .name(TIMER_NAME)
      .tags("table", String.join(".", DATABASE, TABLE_2))
      .timer();
    assertThat(timer1).isNotNull();
    assertThat(timer2).isNotNull();
  }

  @Test
  public void timeSucceedsWithMultipleArguments() {
    monitoredClass.multipleArguments(housekeepingPath, 1, "test-string");
    Timer timer = RequiredSearch.in(meterRegistry)
      .name(TIMER_NAME)
      .tags("table", String.join(".", DATABASE, TABLE))
      .timer();
    assertThat(timer).isNotNull();
  }

  @Test
  public void timeFailsIfPathIsNotFirstArgument() {
    monitoredClass.pathIsNotTheFirstArg(1, "test-string", housekeepingPath);
    assertThatThrownBy(() -> RequiredSearch.in(meterRegistry)
      .name(TIMER_NAME)
      .tags("table", String.join(".", DATABASE, TABLE))
      .timer()).isInstanceOf(MeterNotFoundException.class);
  }

}
