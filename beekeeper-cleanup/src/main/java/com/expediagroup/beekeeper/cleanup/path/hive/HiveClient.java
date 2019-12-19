package com.expediagroup.beekeeper.cleanup.path.hive;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

import java.util.List;

public class HiveClient {

    private static final Logger log = LoggerFactory.getLogger(HiveClient.class);
    private HiveMetaStoreClient metaStoreClient;
    private boolean dryRunEnabled;

    public HiveClient(HiveMetaStoreClient metaStoreClient, boolean dryRunEnabled) {
        this.metaStoreClient = metaStoreClient;
        this.dryRunEnabled = dryRunEnabled;
    }

    public List<String> getAllDatabases() throws MetaException {
        return this.metaStoreClient.getAllDatabases();
    }

}
