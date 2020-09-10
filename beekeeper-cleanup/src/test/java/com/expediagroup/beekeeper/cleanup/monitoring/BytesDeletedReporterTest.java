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
package com.expediagroup.beekeeper.cleanup.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import static com.expediagroup.beekeeper.cleanup.monitoring.BytesDeletedReporter.DRY_RUN_METRIC_NAME;
import static com.expediagroup.beekeeper.cleanup.monitoring.BytesDeletedReporter.METRIC_NAME;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Statistic;
import io.micrometer.core.instrument.search.RequiredSearch;

import com.expediagroup.beekeeper.cleanup.TestApplication;
import com.expediagroup.beekeeper.core.config.FileSystemType;
import com.expediagroup.beekeeper.core.monitoring.MetricTag;
import com.expediagroup.beekeeper.core.monitoring.Taggable;

@ExtendWith(SpringExtension.class)
@ExtendWith(MockitoExtension.class)
@ContextConfiguration(classes = { TestApplication.class },
  loader = AnnotationConfigContextLoader.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class BytesDeletedReporterTest {

  private static final String TABLE = "database.table";
  private static final long BYTES_DELETED = 10;

  private @Autowired MeterRegistry meterRegistry;
  private @Mock Taggable taggable;
  private BytesDeletedReporter bytesDeletedReporter;

  @BeforeEach
  public void init() {
    when(taggable.getMetricTag()).thenReturn(new MetricTag("table", "database.table"));
    bytesDeletedReporter = new BytesDeletedReporter(meterRegistry, false);
  }

  @Test
  public void typical() {
    bytesDeletedReporter.reportTaggable(BYTES_DELETED, taggable, FileSystemType.S3);
    bytesDeletedReporter.reportTaggable(BYTES_DELETED, taggable, FileSystemType.S3);
    Counter counter = RequiredSearch.in(meterRegistry)
        .name("s3-" + METRIC_NAME)
        .tags("table", TABLE)
        .counter();
    assertThat(counter).isNotNull();
    assertThat(counter.measure().iterator()).toIterable().extracting("statistic")
        .containsExactly(Statistic.COUNT);
    assertThat(counter.measure().iterator()).toIterable().extracting("value")
        .containsExactly((double) BYTES_DELETED * 2);
  }

  @Test
  public void typicalDryRun() {
    bytesDeletedReporter = new BytesDeletedReporter(meterRegistry, true);
    bytesDeletedReporter.reportTaggable(BYTES_DELETED, taggable, FileSystemType.S3);
    Counter counter = RequiredSearch.in(meterRegistry)
        .name("s3-" + DRY_RUN_METRIC_NAME)
        .tags("table", TABLE)
        .counter();
    assertThat(counter).isNotNull();
    assertThat(counter.measure().iterator()).toIterable().extracting("statistic")
        .containsExactly(Statistic.COUNT);
    assertThat(counter.measure().iterator()).toIterable().extracting("value")
        .containsExactly((double) BYTES_DELETED);
  }

  @Test
  public void multipleTablesCreateMultipleCounters() {
    Taggable taggable2 = Mockito.mock(Taggable.class);
    when(taggable2.getMetricTag()).thenReturn(new MetricTag("table", "database2.table2"));
    bytesDeletedReporter.reportTaggable(BYTES_DELETED, taggable, FileSystemType.S3);
    bytesDeletedReporter.reportTaggable(BYTES_DELETED * 2, taggable2, FileSystemType.S3);
    Counter counter1 = RequiredSearch.in(meterRegistry)
        .name("s3-" + METRIC_NAME)
        .tags("table", TABLE)
        .counter();
    Counter counter2 = RequiredSearch.in(meterRegistry)
        .name("s3-" + METRIC_NAME)
        .tags("table", "database2.table2")
        .counter();
    assertThat(counter1).isNotNull();
    assertThat(counter2).isNotNull();
    assertThat(counter1.measure().iterator()).toIterable().extracting("statistic")
        .containsExactly(Statistic.COUNT);
    assertThat(counter1.measure().iterator()).toIterable().extracting("value")
        .containsExactly((double) BYTES_DELETED);
    assertThat(counter2.measure().iterator()).toIterable().extracting("statistic")
        .containsExactly(Statistic.COUNT);
    assertThat(counter2.measure().iterator()).toIterable().extracting("value")
        .containsExactly((double) BYTES_DELETED * 2);
  }
}
