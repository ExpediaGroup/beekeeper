package com.expediagroup.beekeeper.api;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class HousekeepingMetadataId {
  private String databaseName;
  private String tableName;
}
