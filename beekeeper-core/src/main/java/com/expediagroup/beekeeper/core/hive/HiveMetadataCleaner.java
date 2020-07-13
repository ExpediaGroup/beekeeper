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
package com.expediagroup.beekeeper.core.hive;

import com.expediagroup.beekeeper.core.config.MetadataType;
import com.expediagroup.beekeeper.core.metadata.MetadataCleaner;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.monitoring.DeletedMetadataReporter;

public class HiveMetadataCleaner implements MetadataCleaner {

  private HiveClient client;
  private DeletedMetadataReporter deletedMetadataReporter;

  public HiveMetadataCleaner(HiveClient client, DeletedMetadataReporter deletedMetadataReporter, boolean dryRunEnabled) {
    this.client = client;
    this.deletedMetadataReporter = deletedMetadataReporter;
    this.deletedMetadataReporter.isDryRunEnabled(dryRunEnabled);
  }

  @Override
  public void cleanupMetadata(HousekeepingMetadata housekeepingMetadata) {
    client.deleteMetadata(housekeepingMetadata.getDatabaseName(), housekeepingMetadata.getTableName());
    deletedMetadataReporter.reportTaggable(housekeepingMetadata, MetadataType.HIVE_TABLE);
  }

  @Override
  public void cleanupPartition(HousekeepingMetadata housekeepingMetadata) {
    // TODO
    // partition value ???
    // Vedant said he might have it come out as a comma separated string
    // change from "year=2020,hour=01" to "year=2020/hour=01"
    // check if need to change - not sure how we're going to be given it
    String partitionName = "";
    // formatPartitions(housekeepingMetadata.getPartitions());

    client.dropPartition(housekeepingMetadata.getDatabaseName(), housekeepingMetadata.getTableName(), partitionName);
    deletedMetadataReporter.reportTaggable(housekeepingMetadata, MetadataType.HIVE_PARTITION);
  }

  // TODO
  private String formatPartitions(String partitionName) {
    return partitionName.replace(",", "/");
  }

}