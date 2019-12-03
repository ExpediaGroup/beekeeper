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
package com.expediagroup.beekeeper.core.monitoring;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.LocalDateTime;

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
import io.micrometer.core.instrument.search.RequiredSearch;

import com.expediagroup.beekeeper.core.TestApplication;
import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;

@ExtendWith(SpringExtension.class)
@ExtendWith(MockitoExtension.class)
@ContextConfiguration(classes = { TestApplication.class, MonitoredClass.class },
  loader = AnnotationConfigContextLoader.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class TimedHousekeepingPathAspectTest {

  public static final String TIMER_NAME = "metric-timer";
  private static final String DATABASE = "database";
  private static final String TABLE = "table";
  private static final String TABLE_2 = "table2";

  @Autowired
  private MeterRegistry meterRegistry;

  @Autowired
  private MonitoredClass monitoredClass;

  @Test
  public void typicalTime() {
    EntityHousekeepingPath path = new EntityHousekeepingPath.Builder()
      .databaseName(DATABASE)
      .tableName(TABLE)
      .creationTimestamp(LocalDateTime.now())
      .cleanupDelay(Duration.ofDays(1))
      .build();
    monitoredClass.doSomething(path);
    path.setTableName(TABLE_2);
    monitoredClass.doSomething(path);
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

}
