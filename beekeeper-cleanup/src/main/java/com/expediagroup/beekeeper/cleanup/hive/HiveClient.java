/**
 * Copyright (C) 2019-2022 Expedia, Inc.
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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expediagroup.beekeeper.cleanup.metadata.CleanerClient;
import com.expediagroup.beekeeper.core.error.BeekeeperException;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

public class HiveClient implements CleanerClient {

  private static final Logger log = LoggerFactory.getLogger(HiveClient.class);

  private CloseableMetaStoreClient client;
  private final boolean dryRunEnabled;

  public HiveClient(CloseableMetaStoreClient client, boolean dryRunEnabled) {
    this.client = client;
    this.dryRunEnabled = dryRunEnabled;
  }

  /**
   * Will drop the table from the database if it exists.
   *
   * @param databaseName
   * @param tableName
   */
  @Override
  public void dropTable(String databaseName, String tableName) {
    if (dryRunEnabled) {
      log.info("Dry run - dropping table \"{}.{}\"", databaseName, tableName);
    } else {
      try {
        log.info("Dropping table \"{}.{}\"", databaseName, tableName);
        client.dropTable(databaseName, tableName);
      } catch (NoSuchObjectException e) {
        log.info("Could not drop table: table not found: \"{}.{}\"", databaseName, tableName);
      } catch (TException e) {
        throw new BeekeeperException(
            "Unexpected exception when dropping table: \"" + databaseName + "." + tableName + "\".",
            e);
      }
    }
  }

  /**
   * Will drop the partition from the table if it exists.
   *
   * @param databaseName
   * @param tableName
   * @param partitionName expected format: "event_date=2020-01-01/event_hour=0/event_type=A"
   */
  @Override
  public boolean dropPartition(String databaseName, String tableName, String partitionName) {
    boolean partitionDeleted = true;
    if (dryRunEnabled) {
      log.info("Dry run - dropping partition \"{}\" from table \"{}.{}\"", partitionName, databaseName, tableName);
    } else {
      try {
        log.info("Dropping partition \"{}\" from table \"{}.{}\"", partitionName, databaseName, tableName);
        client.dropPartition(databaseName, tableName, partitionName, false);
      } catch (NoSuchObjectException e) {
        log
            .info("Could not drop partition \"{}\" from table \"{}.{}\". Partition does not exist.", partitionName,
                databaseName, tableName);
        partitionDeleted = false;
      } catch (TException e) {
        throw new BeekeeperException("Unexpected exception when dropping partition \""
            + partitionName
            + "\" from table: \""
            + databaseName
            + "."
            + tableName
            + "\".", e);
      }
    }
    return partitionDeleted;
  }

  @Override
  public boolean tableExists(String databaseName, String tableName) {
    try {
      return client.tableExists(databaseName, tableName);
    } catch (TException e) {
      throw new BeekeeperException(
          "Unexpected exception when checking if table \"" + databaseName + "." + tableName + "\" exists.", e);
    }
  }

  @Override
  public Map<String, String> getTableProperties(String databaseName, String tableName) {
    try {
      Table table = client.getTable(databaseName, tableName);
      if (table.getParameters() == null) {
        return new HashMap<>();
      }
      return table.getParameters();
    } catch (NoSuchObjectException e) {
      log.warn("The table {}.{} does not exist", databaseName, tableName);
      return new HashMap<>();
    } catch (TException e) {
      throw new BeekeeperException(
          "Unexpected exception when getting table properties for \"" + databaseName + "." + tableName + ".", e);
    }
  }

  @Override
  public void close() {
    client.close();
  }
}
