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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

import com.expediagroup.beekeeper.core.config.FileSystemType;
import com.expediagroup.beekeeper.core.monitoring.MetricTag;
import com.expediagroup.beekeeper.core.monitoring.Taggable;

@Service
public class BytesDeletedReporter {

  private static final Logger log = LoggerFactory.getLogger(BytesDeletedReporter.class);
  public static final String METRIC_NAME = "bytes-deleted";
  public static final String DRY_RUN_METRIC_NAME = "dry-run-" + METRIC_NAME;

  private MeterRegistry meterRegistry;
  private String metricName;

  @Autowired
  public BytesDeletedReporter(MeterRegistry meterRegistry,
    @Value("${properties.dry-run-enabled}") boolean dryRunEnabled) {
    this.meterRegistry = meterRegistry;
    this.metricName = dryRunEnabled ? DRY_RUN_METRIC_NAME : METRIC_NAME;
  }

  public void reportTaggable(long bytesDeleted, Taggable taggable, FileSystemType fileSystemType) {
    log.info("Bytes deleted: {}", bytesDeleted);
    String fileSystemMetricName = String.join("-", fileSystemType.toString()
      .toLowerCase(), metricName);
    Counter counter = Counter
        .builder(fileSystemMetricName)
        .baseUnit("bytes")
        .tags(tags(taggable.getMetricTag()))
        .register(meterRegistry);
    counter.increment(bytesDeleted);
  }

  private Iterable<Tag> tags(MetricTag metricTag) {
    return Tags.of(metricTag.getKey(), metricTag.getTag());
  }
}
