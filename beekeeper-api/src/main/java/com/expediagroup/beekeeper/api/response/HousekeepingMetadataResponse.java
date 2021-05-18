package com.expediagroup.beekeeper.api.response;

import java.util.List;

import javax.persistence.Column;
import javax.persistence.Table;

import lombok.Builder;
import lombok.Value;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;

@Value
@Builder
@Table(name = "housekeeping_metadata")
public class HousekeepingMetadataResponse {

  @Column(name = "database_name", nullable = false)
  String databaseName;

  @Column(name = "table_name", nullable = false)
  String tableName;

  @Column(name = "path", nullable = false)
  String path;

  @Column(name = "lifecycles", nullable = false)
  List<Lifecycle> lifecycles;

  public static HousekeepingMetadataResponse convertToHouseKeepingMetadataResponse(
      HousekeepingMetadata housekeepingMetadata) {
    HousekeepingMetadataResponse.builder().databaseName()
  }
}
