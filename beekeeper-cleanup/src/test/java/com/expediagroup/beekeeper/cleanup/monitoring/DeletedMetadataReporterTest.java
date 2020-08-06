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

import static com.expediagroup.beekeeper.cleanup.monitoring.DeletedMetadataReporter.DRY_RUN_METRIC_NAME;
import static com.expediagroup.beekeeper.cleanup.monitoring.DeletedMetadataReporter.METRIC_NAME;

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
import com.expediagroup.beekeeper.core.config.MetadataType;
import com.expediagroup.beekeeper.core.monitoring.MetricTag;
import com.expediagroup.beekeeper.core.monitoring.Taggable;

@ExtendWith(SpringExtension.class)
@ExtendWith(MockitoExtension.class)
@ContextConfiguration(classes = { TestApplication.class }, loader = AnnotationConfigContextLoader.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class DeletedMetadataReporterTest {

  private static final String TABLE = "database.table";

  private String tableCounterName = String.join("-", MetadataType.HIVE_TABLE.toString().toLowerCase(), METRIC_NAME);
  private String dryRunTableCounterName = String
      .join("-", MetadataType.HIVE_TABLE.toString().toLowerCase(), DRY_RUN_METRIC_NAME);

  private @Autowired MeterRegistry meterRegistry;
  private @Mock
  Taggable taggable;
  private DeletedMetadataReporter deletedMetadataReporter;

  @BeforeEach
  public void init() {
    when(taggable.getMetricTag()).thenReturn(new MetricTag("table", "database.table"));
    deletedMetadataReporter = new DeletedMetadataReporter(meterRegistry, false);
  }

  @Test
  public void typical() {
    deletedMetadataReporter.reportTaggable(taggable, MetadataType.HIVE_TABLE);

    Counter counter = RequiredSearch.in(meterRegistry).name(tableCounterName).tags("table", TABLE).counter();
    assertThat(counter).isNotNull();
    assertThat(counter.measure().iterator()).toIterable().extracting("statistic").containsExactly(Statistic.COUNT);
    assertThat(counter.measure().iterator()).toIterable().extracting("value").containsExactly((double) 1);
  }

  @Test
  public void typicalDryRun() {
    deletedMetadataReporter = new DeletedMetadataReporter(meterRegistry, true);
    deletedMetadataReporter.reportTaggable(taggable, MetadataType.HIVE_TABLE);

    Counter counter = RequiredSearch.in(meterRegistry).name(dryRunTableCounterName).tags("table", TABLE).counter();
    assertThat(counter).isNotNull();
    assertThat(counter.measure().iterator()).toIterable().extracting("statistic").containsExactly(Statistic.COUNT);
    assertThat(counter.measure().iterator()).toIterable().extracting("value").containsExactly((double) 1);
  }

  @Test
  public void multipleTablesDeleted() {
    Taggable taggable2 = Mockito.mock(Taggable.class);
    when(taggable2.getMetricTag()).thenReturn(new MetricTag("table", "database2.table2"));
    deletedMetadataReporter.reportTaggable(taggable, MetadataType.HIVE_TABLE);
    deletedMetadataReporter.reportTaggable(taggable2, MetadataType.HIVE_TABLE);

    Counter counter = RequiredSearch.in(meterRegistry).name(tableCounterName).tags("table", TABLE).counter();
    assertThat(counter).isNotNull();
    assertThat(counter.measure().iterator()).toIterable().extracting("statistic").containsExactly(Statistic.COUNT);
    assertThat(counter.measure().iterator()).toIterable().extracting("value").containsExactly((double) 1);
    Counter counter2 = RequiredSearch
        .in(meterRegistry)
        .name(tableCounterName)
        .tags("table", "database2.table2")
        .counter();
    assertThat(counter2).isNotNull();
    assertThat(counter2.measure().iterator()).toIterable().extracting("statistic").containsExactly(Statistic.COUNT);
    assertThat(counter2.measure().iterator()).toIterable().extracting("value").containsExactly((double) 1);
  }

}
