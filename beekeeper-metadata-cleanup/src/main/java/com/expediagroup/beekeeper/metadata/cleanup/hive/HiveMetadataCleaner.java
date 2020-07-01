package com.expediagroup.beekeeper.metadata.cleanup.hive;

import org.apache.hadoop.hive.conf.HiveConf;

import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.metadata.cleanup.monitoring.DeletedMetadataReporter;
import com.expediagroup.beekeeper.metadata.cleanup.table.MetadataCleaner;

import com.hotels.hcommon.hive.metastore.client.api.MetaStoreClientFactory;

public class HiveMetadataCleaner implements MetadataCleaner {

  private MetaStoreClientFactory metastoreClientFactory;
  private HiveConf conf;
  private String name = "";

  private HiveClient client;
  private DeletedMetadataReporter tablesDeletedReporter;

  public HiveMetadataCleaner(HiveClient client, DeletedMetadataReporter tablesDeletedReporter) {
    this.client = client;
    this.tablesDeletedReporter = tablesDeletedReporter;
  }

  // TODO
  // This currently does nothing
  // need to get the conf and name from somewhere
  // also the metastoreclientfactory object needs to come from somewhere

  @Override
  public void cleanupMetadata(HousekeepingPath housekeepingPath) {
    // TODO Auto-generated method stub

    String databaseName = housekeepingPath.getDatabaseName();
    String tableName = housekeepingPath.getTableName();

    // going to need a metastore client supplier
    // try (CloseableMetaStoreClient client = metastoreClientFactory.newInstance(conf, name)) {
    //
    // try {
    // client.dropTable(databaseName, tableName);
    // } catch (TException e) {
    // throw new BeekeeperException("Could not delete table: " + databaseName + "." + tableName + ".", e);
    // }
    // }
    client.deleteMetadata(databaseName, tableName);
    tablesDeletedReporter.reportTaggable(housekeepingPath, databaseName);

  }

}
