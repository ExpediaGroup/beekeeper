package com.expediagroup.beekeeper.api.response;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Table;

import org.hibernate.annotations.UpdateTimestamp;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;

import com.expediagroup.beekeeper.core.model.DurationConverter;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;

@Value
@Builder
@Table(name = "housekeeping_metadata")
public class HousekeepingMetadataResponse {

  @Column(name = "path", nullable = false)
  String path;

  @Column(name = "database_name", nullable = false)
  String databaseName;

  @Column(name = "table_name", nullable = false)
  String tableName;

  @Column(name = "partition_name")
  String partitionName;

  @Column(name = "housekeeping_status", nullable = false)
  @Enumerated(EnumType.STRING)
  HousekeepingStatus housekeepingStatus;

  @EqualsAndHashCode.Exclude
  @Column(name = "creation_timestamp", nullable = false, updatable = false)
  LocalDateTime creationTimestamp;

  @EqualsAndHashCode.Exclude
  @Column(name = "modified_timestamp")
  @UpdateTimestamp
  LocalDateTime modifiedTimestamp;

  @EqualsAndHashCode.Exclude
  @Column(name = "cleanup_timestamp", nullable = false)
  LocalDateTime cleanupTimestamp;

  @Column(name = "cleanup_delay", nullable = false)
  @Convert(converter = DurationConverter.class)
  Duration cleanupDelay;

  @Column(name = "cleanup_attempts", nullable = false)
  int cleanupAttempts;

  @Column(name = "lifecycle_type", nullable = false)
  String lifecycleType;

  public static HousekeepingMetadataResponse convertToHouseKeepingMetadataResponse(
      HousekeepingMetadata housekeepingMetadata) {

    return HousekeepingMetadataResponse.builder()
        .path(housekeepingMetadata.getPath())
        .databaseName(housekeepingMetadata.getDatabaseName())
        .tableName(housekeepingMetadata.getTableName())
        .partitionName(housekeepingMetadata.getPartitionName())
        .housekeepingStatus(housekeepingMetadata.getHousekeepingStatus())
        .creationTimestamp(housekeepingMetadata.getCreationTimestamp())
        .modifiedTimestamp(housekeepingMetadata.getModifiedTimestamp())
        .cleanupTimestamp(housekeepingMetadata.getCleanupTimestamp())
        .cleanupDelay(housekeepingMetadata.getCleanupDelay())
        .cleanupAttempts(housekeepingMetadata.getCleanupAttempts())
        .lifecycleType(housekeepingMetadata.getLifecycleType())
        .build();
  }

  public static Page<HousekeepingMetadataResponse> convertToHouseKeepingMetadataResponsePage(List<HousekeepingMetadata> housekeepingMetadataList){
    List<HousekeepingMetadataResponse> housekeepingMetadataResponseList = new ArrayList<>();
    for (HousekeepingMetadata housekeepingMetadata : housekeepingMetadataList) {
      HousekeepingMetadataResponse housekeepingMetadataResponse = convertToHouseKeepingMetadataResponse(housekeepingMetadata);
      housekeepingMetadataResponseList.add(housekeepingMetadataResponse);
    }
    return new PageImpl<>(housekeepingMetadataResponseList);
  }

}
