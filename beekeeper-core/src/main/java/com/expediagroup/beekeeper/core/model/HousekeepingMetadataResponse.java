package com.expediagroup.beekeeper.core.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Value;

@Value
@NoArgsConstructor
@Table(name = "housekeeping_metadata")
public class HousekeepingMetadataResponse{

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(name = "database_name", nullable = false)
  private String databaseName;

  @Column(name = "table_name", nullable = false)
  private String tableName;

  @Column(name = "path", nullable = false)
  private String path;

  @Column(name = "lifecycle", nullable = false)
  private Lifecycle lifecycle;

  @Builder
  public HousekeepingMetadataResponse(Long id, String path, String databaseName, String tableName, Lifecycle lifecycle){
    this.id = id;
    this.path = path;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.lifecycle = lifecycle;
  }

//  public HousekeepingMetadataResponse(HousekeepingMetadata housekeepingMetadata) {
//    Long id = housekeepingMetadata.getId();
//    String path = housekeepingMetadata.getPath();
//    String databaseName = housekeepingMetadata.getDatabaseName();
//    String tableName = housekeepingMetadata.getTableName();
//    String partitionName = housekeepingMetadata.getPartitionName();
//    HousekeepingStatus housekeepingStatus = housekeepingMetadata.getHousekeepingStatus();
//  }
}
