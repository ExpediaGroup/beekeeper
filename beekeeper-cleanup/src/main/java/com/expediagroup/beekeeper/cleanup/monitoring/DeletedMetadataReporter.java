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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

import com.expediagroup.beekeeper.core.config.MetadataType;
import com.expediagroup.beekeeper.core.monitoring.MetricTag;
import com.expediagroup.beekeeper.core.monitoring.Taggable;

public class DeletedMetadataReporter {

  private static final Logger log = LoggerFactory.getLogger(DeletedMetadataReporter.class);
  public static final String METRIC_NAME = "metadata-deleted";
  public static final String DRY_RUN_METRIC_NAME = "dry-run-" + METRIC_NAME;

  private MeterRegistry meterRegistry;
  private String metricName;

  public DeletedMetadataReporter(MeterRegistry meterRegistry, boolean dryRunEnabled) {
    this.meterRegistry = meterRegistry;
    this.metricName = dryRunEnabled ? DRY_RUN_METRIC_NAME : METRIC_NAME;
  }

  public void reportTaggable(Taggable taggable, MetadataType metadataType) {
    log.info("Deleted " + metadataType.getTypeName());
    MetricTag tag = taggable.getMetricTag();
    String metadataMetricName = String.join("-", metadataType.getTypeName(), metricName);
    Counter counter = Counter.builder(metadataMetricName).tags(createTag(tag)).register(meterRegistry);
    counter.increment();
  }

  private Iterable<Tag> createTag(MetricTag metricTag) {
    return Tags.of(metricTag.getKey(), metricTag.getTag());
  }
}
