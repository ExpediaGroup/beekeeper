package com.expediagroup.beekeeper.core.model;

import java.util.List;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
@Table(name = "housekeeping_metadata")
public class HousekeepingMetadataResponse{

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  Long id;

  @Column(name = "database_name", nullable = false)
  String databaseName;

  @Column(name = "table_name", nullable = false)
  String tableName;

  @Column(name = "path", nullable = false)
  String path;

  @Column(name = "lifecycles", nullable = false)
  List<Lifecycle> lifecycles;

}
