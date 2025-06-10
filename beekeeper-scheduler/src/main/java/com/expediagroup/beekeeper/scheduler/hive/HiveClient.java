/**
 * Copyright (C) 2019-2025 Expedia, Inc.
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
package com.expediagroup.beekeeper.scheduler.hive;

import java.io.Closeable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.iterator.PartitionIterator;

public class HiveClient implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(HiveClient.class);

  protected final CloseableMetaStoreClient metaStoreClient;
  protected final PartitionIteratorFactory partitionIteratorFactory;

  public HiveClient(CloseableMetaStoreClient client, PartitionIteratorFactory partitionIteratorFactory) {
    this.metaStoreClient = client;
    this.partitionIteratorFactory = partitionIteratorFactory;
  }

  public Map<String, PartitionInfo> getTablePartitionsInfo(String databaseName, String tableName) {
    try {
      Map<String, PartitionInfo> partitionInfoMap = new HashMap<>();

      Table table = metaStoreClient.getTable(databaseName, tableName);
      List<FieldSchema> partitionKeys = table.getPartitionKeys();

      PartitionIterator iterator = partitionIteratorFactory.newInstance(metaStoreClient, table);
      while (iterator.hasNext()) {
        Partition partition = iterator.next();
        List<String> values = partition.getValues();
        String path = partition.getSd().getLocation();
        String partitionName = Warehouse.makePartName(partitionKeys, values);
        
        LocalDateTime createTime = extractCreateTime(partition);
        
        partitionInfoMap.put(partitionName, new PartitionInfo(path, createTime));

        log.debug("Retrieved partition values '{}' with path '{}' for table {}.{}",
            values, path, databaseName, table);
      }
      return partitionInfoMap;
    } catch (TException e) {
      log.warn("Got error. Returning empty map. Error message: {}", e.getMessage());
      return Collections.emptyMap();
    }
  }

  public PartitionInfo getSinglePartitionInfo(String databaseName, String tableName, String partitionName) {
    try {
      Table table = metaStoreClient.getTable(databaseName, tableName);
      List<FieldSchema> partitionKeys = table.getPartitionKeys();
      List<String> partitionValues = Warehouse.getPartValuesFromPartName(partitionName);

      Partition partition = metaStoreClient.getPartition(databaseName, tableName, partitionValues);
      
      String path = partition.getSd().getLocation();
      LocalDateTime createTime = extractCreateTime(partition);
      
      log.debug("Retrieved partition '{}' with path '{}' for table {}.{}", 
          partitionName, path, databaseName, tableName);
      
      return new PartitionInfo(path, createTime);
    } catch (TException e) {
      log.warn("Failed to get partition info for {}.{}.{}: {}", 
          databaseName, tableName, partitionName, e.getMessage());
      return null;
    }
  }

  private LocalDateTime extractCreateTime(Partition partition) {
    if (partition.getCreateTime() > 0) {
        return LocalDateTime.ofInstant(
            Instant.ofEpochSecond(partition.getCreateTime()), 
            ZoneId.systemDefault());
    }
    
    log.warn("Creation time for partition is not available, using current time");
    return LocalDateTime.now();
  }

  public void close() {
    metaStoreClient.close();
  }
}
