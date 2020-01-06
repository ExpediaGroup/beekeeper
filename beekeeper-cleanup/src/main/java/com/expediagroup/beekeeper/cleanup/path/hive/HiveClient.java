package com.expediagroup.beekeeper.cleanup.path.hive;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveClient {

  private static final Logger log = LoggerFactory.getLogger(HiveClient.class);
  private final HiveMetaStoreClient metaStoreClient;
  private final boolean dryRunEnabled;

  public HiveClient(HiveMetaStoreClient metaStoreClient, boolean dryRunEnabled) {
    this.metaStoreClient = metaStoreClient;
    this.dryRunEnabled = dryRunEnabled;
  }

  boolean dropTable(String databaseName, String tableName) {
    throw new UnsupportedOperationException("dropTable is unimplemented");
  }

  public boolean dropPartition(String databaseName, String tableName, String partitionName) {
    throw new UnsupportedOperationException("dropPartition is unimplemented");
  }
}
