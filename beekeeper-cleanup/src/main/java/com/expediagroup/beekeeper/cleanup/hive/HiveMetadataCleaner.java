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
package com.expediagroup.beekeeper.cleanup.hive;

import com.expediagroup.beekeeper.cleanup.metadata.MetadataCleaner;
import com.expediagroup.beekeeper.cleanup.monitoring.DeletedMetadataReporter;
import com.expediagroup.beekeeper.core.config.MetadataType;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.monitoring.TimedTaggable;

public class HiveMetadataCleaner implements MetadataCleaner {

  private HiveClientFactory clientFactory;
  private HiveClient hiveClient;
  private DeletedMetadataReporter deletedMetadataReporter;

  public HiveMetadataCleaner(HiveClientFactory clientFactory, DeletedMetadataReporter deletedMetadataReporter) {
    this.clientFactory = clientFactory;
    this.deletedMetadataReporter = deletedMetadataReporter;
  }

  /**
   * @implNote Doing this to avoid having long-running Hive MetaStore clients. So we only create a client when its
   * needed and close it after.
   */
  @Override
  public void setupCleaner() {
    hiveClient = clientFactory.newInstance();
  }

  @Override
  @TimedTaggable("hive-table-deleted")
  public void dropTable(HousekeepingMetadata housekeepingMetadata) {
    hiveClient.dropTable(housekeepingMetadata.getDatabaseName(), housekeepingMetadata.getTableName());
    deletedMetadataReporter.reportTaggable(housekeepingMetadata, MetadataType.HIVE_TABLE);
    closeClient();
  }

  @Override
  @TimedTaggable("hive-partition-deleted")
  public boolean dropPartition(HousekeepingMetadata housekeepingMetadata) {
    boolean partitionDeleted = hiveClient
        .dropPartition(housekeepingMetadata.getDatabaseName(), housekeepingMetadata.getTableName(),
            housekeepingMetadata.getPartitionName());
    if (partitionDeleted) {
      deletedMetadataReporter.reportTaggable(housekeepingMetadata, MetadataType.HIVE_PARTITION);
    }
    closeClient();
    return partitionDeleted;
  }

  @Override
  public boolean tableExists(String databaseName, String tableName) {
    return clientFactory.newInstance().tableExists(databaseName, tableName);
  }

  private void closeClient(){
    hiveClient.close();
  }
}
