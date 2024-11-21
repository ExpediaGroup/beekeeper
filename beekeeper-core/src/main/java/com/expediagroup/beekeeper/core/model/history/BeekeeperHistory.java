package com.expediagroup.beekeeper.core.model.history;

import java.time.LocalDateTime;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import com.expediagroup.beekeeper.core.monitoring.MetricTag;
import com.expediagroup.beekeeper.core.monitoring.Taggable;

@Data
@NoArgsConstructor
@Entity
@Table(name = "beekeeper_history")
public class BeekeeperHistory implements Taggable {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @EqualsAndHashCode.Exclude
  @Column(name = "event_timestamp", nullable = false, updatable = false)
  private LocalDateTime eventTimestamp;

  @Column(name = "database_name", nullable = false)
  private String databaseName;

  @Column(name = "table_name", nullable = false)
  private String tableName;

  @Column(name = "lifecycle_type", nullable = false)
  private String lifecycleType;

  @Column(name = "housekeeping_status", nullable = false)
  private String housekeepingStatus;

  @Column(name = "event_details", columnDefinition = "TEXT")
  private String eventDetails;

  @Builder
  public BeekeeperHistory(
      Long id,
      LocalDateTime eventTimestamp,
      String databaseName,
      String tableName,
      String lifecycleType,
      String housekeepingStatus,
      String eventDetails
  ) {
    this.id = id;
    this.eventTimestamp = eventTimestamp;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.lifecycleType = lifecycleType;
    this.housekeepingStatus = housekeepingStatus;
    this.eventDetails = eventDetails;
  }

  @Override
  public MetricTag getMetricTag() {
    return new MetricTag("table", String.join(".", databaseName, tableName));
  }

}
