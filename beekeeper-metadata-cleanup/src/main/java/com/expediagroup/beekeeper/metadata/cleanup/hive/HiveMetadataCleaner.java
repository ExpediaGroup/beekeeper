package com.expediagroup.beekeeper.metadata.cleanup.hive;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.metadata.cleanup.cleaner.MetadataCleaner;
import com.expediagroup.beekeeper.metadata.cleanup.monitoring.DeletedMetadataReporter;

public class HiveMetadataCleaner implements MetadataCleaner {

  private HiveClient client;
  private DeletedMetadataReporter deletedMetadataReporter;

  public HiveMetadataCleaner(HiveClient client, DeletedMetadataReporter tablesDeletedReporter) {
    this.client = client;
    this.deletedMetadataReporter = tablesDeletedReporter;
  }

  // TODO
  // This currently does nothing
  // need to get the conf and name from somewhere
  // also the metastoreclientfactory object needs to come from somewhere

  @Override
  public void cleanupMetadata(HousekeepingMetadata housekeepingMetadata) {
    // TODO Auto-generated method stub

    String databaseName = housekeepingMetadata.getDatabaseName();
    String tableName = housekeepingMetadata.getTableName();

    // going to need a metastore client supplier
    // try (CloseableMetaStoreClient client = metastoreClientFactory.newInstance(conf, name)) {
    //
    // try {
    // client.dropTable(databaseName, tableName);
    // } catch (TException e) {
    // throw new BeekeeperException("Could not delete table: " + databaseName + "." + tableName + ".", e);
    // }
    // }

    // TODO
    // need to prevent the drop table from being listened to be Beekeeper
    // might not be the end of the world, it would mean it would try to clean it up twice, but the second time it wont
    // do anything.

    client.deleteMetadata(databaseName, tableName);
    deletedMetadataReporter.reportTaggable(housekeepingMetadata, databaseName);

  }

}