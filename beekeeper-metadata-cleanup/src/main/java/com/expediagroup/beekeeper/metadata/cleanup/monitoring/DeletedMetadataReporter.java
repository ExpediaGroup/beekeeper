package com.expediagroup.beekeeper.metadata.cleanup.monitoring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

import com.expediagroup.beekeeper.core.monitoring.MetricTag;
import com.expediagroup.beekeeper.core.monitoring.Taggable;

public class DeletedMetadataReporter {

  private static final Logger log = LoggerFactory.getLogger(DeletedMetadataReporter.class);
  public static final String METRIC_NAME = "tables-deleted";
  public static final String DRY_RUN_METRIC_NAME = "dry-run-" + METRIC_NAME;

  private MeterRegistry meterRegistry;
  private String metricName;

  @Autowired
  public DeletedMetadataReporter(
      MeterRegistry meterRegistry,
      @Value("${properties.dry-run-enabled}") boolean dryRunEnabled) {
    this.meterRegistry = meterRegistry;
    this.metricName = dryRunEnabled ? DRY_RUN_METRIC_NAME : METRIC_NAME;
  }

  public void reportTaggable(Taggable taggable, String databaseName) {
    log.info("Metadata deleted from database: {}", databaseName);
    String databaseMetricName = String.join("-", databaseName, metricName);

    Counter counter = Counter
        .builder(databaseMetricName)
        .tags(tags(taggable.getMetricTag()))
        .register(meterRegistry);
    counter.increment();
  }

  private Iterable<Tag> tags(MetricTag metricTag) {
    return Tags.of(metricTag.getKey(), metricTag.getTag());
  }

}
