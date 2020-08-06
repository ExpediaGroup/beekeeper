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

import com.expediagroup.beekeeper.core.config.MetadataType;
import com.expediagroup.beekeeper.cleanup.metadata.MetadataCleaner;
import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.cleanup.monitoring.DeletedMetadataReporter;

public class HiveMetadataCleaner implements MetadataCleaner {

  private HiveClient client;
  private DeletedMetadataReporter deletedMetadataReporter;

  public HiveMetadataCleaner(HiveClient client, DeletedMetadataReporter deletedMetadataReporter) {
    this.client = client;
    this.deletedMetadataReporter = deletedMetadataReporter;
  }

  @Override
  public boolean dropTable(HousekeepingMetadata housekeepingMetadata) {
    boolean successfulDeletion = client
        .dropTable(housekeepingMetadata.getDatabaseName(), housekeepingMetadata.getTableName());
    if (successfulDeletion) {
      deletedMetadataReporter.reportTaggable(housekeepingMetadata, MetadataType.HIVE_TABLE);
    }
    return successfulDeletion;
  }

  @Override
  public boolean dropPartition(HousekeepingMetadata housekeepingMetadata) {
    boolean successfulDeletion = client
        .dropPartition(housekeepingMetadata.getDatabaseName(), housekeepingMetadata.getTableName(),
            housekeepingMetadata.getPartitionName());
    if (successfulDeletion) {
      deletedMetadataReporter.reportTaggable(housekeepingMetadata, MetadataType.HIVE_PARTITION);
    }
    return successfulDeletion;
  }

}
