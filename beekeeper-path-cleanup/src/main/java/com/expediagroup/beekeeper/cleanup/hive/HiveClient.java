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

import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expediagroup.beekeeper.core.error.BeekeeperException;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

public class HiveClient {

  private static final Logger log = LoggerFactory.getLogger(HiveClient.class);

  private CloseableMetaStoreClient client;
  private final boolean dryRunEnabled;

  public HiveClient(CloseableMetaStoreClient client, boolean dryRunEnabled) {
    this.client = client;
    this.dryRunEnabled = dryRunEnabled;
  }

  /**
   * Will drop the table from the database. Error is not thrown if table not found.
   * 
   * @param databaseName
   * @param tableName
   */
  public boolean dropTable(String databaseName, String tableName) {
    boolean tableDeleted = true;
    if (dryRunEnabled) {
      log.info("Dry run - dropping table \"{}.{}\"", databaseName, tableName);
    } else {
      log.info("Dropping table \"{}.{}\"", databaseName, tableName);
      try {
        client.dropTable(databaseName, tableName);
      } catch (NoSuchObjectException e) {
        log.info("Could not drop table: table not found: \"{}.{}\"", databaseName, tableName);
        tableDeleted = false;
      } catch (TException e) {
        throw new BeekeeperException(
            "Unexpected exception when dropping table: \"" + databaseName + "." + tableName + "\".",
            e);
      }
    }
    return tableDeleted;
  }

  /**
   * Will drop the partition from the table. Error is not thrown if the table or partition are not found.
   * 
   * @param databaseName
   * @param tableName
   * @param partitionName expected format: "event_date=2020-01-01/event_hour=0/event_type=A"
   */
  public boolean dropPartition(String databaseName, String tableName, String partitionName) {
    boolean partitionDeleted = true;
    if (dryRunEnabled) {
      log.info("Dry run - dropping partition \"{}\" from table \"{}.{}\"", partitionName, databaseName, tableName);
    } else {
      log.info("Dropping partition \"{}\" from table \"{}.{}\"", partitionName, databaseName, tableName);
      if (tableExists(databaseName, tableName)) {
        try {
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
      } else {
        log.info("Could not drop partition from table \"{}.{}\". Table does not exist.", databaseName, tableName);
        partitionDeleted = false;
      }
    }
    return partitionDeleted;
  }

  private boolean tableExists(String databaseName, String tableName) {
    try {
      return client.tableExists(databaseName, tableName);
    } catch (TException e) {
      throw new BeekeeperException(
          "Unexpected exception when checking if table \"" + databaseName + "." + tableName + "\" exists.", e);
    }
  }

}
