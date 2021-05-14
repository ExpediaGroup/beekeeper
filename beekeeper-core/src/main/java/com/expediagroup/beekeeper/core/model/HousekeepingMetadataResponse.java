package com.expediagroup.beekeeper.core.model;

import java.time.Duration;
import java.time.LocalDateTime;

import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.annotations.UpdateTimestamp;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.expediagroup.beekeeper.core.monitoring.MetricTag;


@Data
@NoArgsConstructor
@Entity
@Table(name = "housekeeping_metadata")
public class HousekeepingMetadataResponse extends HousekeepingMetadata{

  public HousekeepingMetadataResponse(HousekeepingMetadata housekeepingMetadata){
    this.id = housekeepingMetadata.getId();
    this.path = housekeepingMetadata.getPath();
    this.databaseName = housekeepingMetadata.getDatabaseName();
    this.tableName = housekeepingMetadata.getTableName();
    this.partitionName = housekeepingMetadata.getPartitionName();
    this.housekeepingStatus = housekeepingMetadata.getHousekeepingStatus();
//    this.creationTimestamp = creationTimestamp;
//    this.modifiedTimestamp = modifiedTimestamp;
//    this.cleanupDelay = cleanupDelay;
//    this.cleanupTimestamp = configureCleanupTimestamp();
//    this.cleanupAttempts = cleanupAttempts;
//    this.lifecycleType = lifecycleType;
//    this.clientId = clientId;
  }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "path", nullable = false)
    private String path;

    @Column(name = "database_name", nullable = false)
    private String databaseName;

    @Column(name = "table_name", nullable = false)
    private String tableName;

    @Column(name = "partition_name")
    private String partitionName;

    @Column(name = "housekeeping_status", nullable = false)
    @Enumerated(EnumType.STRING)
    private HousekeepingStatus housekeepingStatus;

//    @EqualsAndHashCode.Exclude
//    @Column(name = "creation_timestamp", nullable = false, updatable = false)
//    private LocalDateTime creationTimestamp;
//
//    @EqualsAndHashCode.Exclude
//    @Column(name = "modified_timestamp")
//    @UpdateTimestamp
//    private LocalDateTime modifiedTimestamp;
//
//    @EqualsAndHashCode.Exclude
//    @Column(name = "cleanup_timestamp", nullable = false)
//    private LocalDateTime cleanupTimestamp;
//
//    @Column(name = "cleanup_delay", nullable = false)
//    @Convert(converter = DurationConverter.class)
//    private Duration cleanupDelay;
//
//    @Column(name = "cleanup_attempts", nullable = false)
//    private int cleanupAttempts;
//
//    @Column(name = "client_id")
//    private String clientId;
//
//    @Column(name = "lifecycle_type", nullable = false)
//    private String lifecycleType;
//
//    public void setCleanupDelay(Duration cleanupDelay) {
//      this.cleanupDelay = cleanupDelay;
//      cleanupTimestamp = creationTimestamp.plus(cleanupDelay);
//    }

}
