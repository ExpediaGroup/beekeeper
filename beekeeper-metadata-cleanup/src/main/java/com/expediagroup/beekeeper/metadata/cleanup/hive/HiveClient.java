package com.expediagroup.beekeeper.metadata.cleanup.hive;

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
    this.dryRunEnabled = dryRunEnabled;
    this.client = client;
  }

  void deleteMetadata(String database, String tableName) {
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

}
