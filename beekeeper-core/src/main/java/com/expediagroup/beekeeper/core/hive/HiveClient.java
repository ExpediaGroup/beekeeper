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

import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expediagroup.beekeeper.core.error.BeekeeperException;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

public class HiveClient {

  private static final String BEEKEEPER_ALTER_PARTITION = "alter_partition";
  private static final String BEEKEEPER_ALTER_TABLE = "alter_table";
  private static final String BEEKEEPER_HIVE_EVENT_WHITELIST = "beekeeper.hive.event.whitelist";

  private static final Logger log = LoggerFactory.getLogger(HiveClient.class);
  private static final String EXTERNAL_KEY = "EXTERNAL";
  private static final String IS_EXTERNAL = "TRUE";

  private CloseableMetaStoreClient client;
  private final boolean dryRunEnabled;

  public HiveClient(CloseableMetaStoreClient client, boolean dryRunEnabled) {
    this.dryRunEnabled = dryRunEnabled;
    this.client = client;
  }

  /**
   * Will delete the metadata. Error is not thrown if table not found.
   * 
   * @param database
   * @param tableName
   */
  public void deleteMetadata(String database, String tableName) {
    if (dryRunEnabled) {
      log.info("Dry run - deleting metadata {}.{}", database, tableName);
    } else {
      log.info("Deleting metadata {}.{}", database, tableName);
      try {
        client.dropTable(database, tableName);

      } catch (NoSuchObjectException e) {
        log.info("Could not delete metadata. Table not found: {}.{}", database, tableName);
      } catch (TException e) {
        throw new BeekeeperException("Unexpected exception when deleting metadata: " + database + "." + tableName + ".",
            e);
      }
    }
  }

  /**
   * Will drop the partition from the table. Error is not thrown if table or partition are not found.
   * 
   * @param databaseName
   * @param tableName
   * @param partitionValue
   */
  public void dropPartition(String databaseName, String tableName, String partitionName) {
    if (dryRunEnabled) {
      log.info("Dry run - dropping partition \"{}\" from table \"{}.{}\"", partitionName, databaseName, tableName);
    } else {
      log.info("Dropping partition \"{}\" from table \"{}.{}\"", partitionName, databaseName, tableName);

      if (tableExists(databaseName, tableName)) {

        try {
          // client.dropPartition(databaseName, tableName, partitionValue, false);
          client.dropPartition(databaseName, tableName, partitionName, false);
        } catch (NoSuchObjectException e) {
          log
              .info("Could not drop partition \"{}\" from table \"{}.{}\". Partition does not exist.", partitionName,
                  databaseName, tableName);
        } catch (TException e) {
          throw new BeekeeperException("Unexpected exception when dropping partition values: \""
              + partitionName
              + "\" from table: "
              + databaseName
              + "."
              + tableName
              + ".", e);
        }
      } else {
        log.info("Could not drop partition from table \"{}.{}\". Table does not exist.", databaseName, tableName);
      }

    }
  }

  private boolean tableExists(String databaseName, String tableName) {
    try {
      client.getTable(databaseName, tableName);
      return true;
    } catch (TException e) {
      return false;
    }
  }

}