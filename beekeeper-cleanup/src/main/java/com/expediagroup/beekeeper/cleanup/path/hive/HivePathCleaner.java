package com.expediagroup.beekeeper.cleanup.path.hive;

import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HivePathCleaner implements PathCleaner {
    private static final Logger log = LoggerFactory.getLogger(HivePathCleaner.class);
    private HiveClient hiveClient;

    public HivePathCleaner(HiveClient hiveClient) {
        this.hiveClient = hiveClient;
    }

    @Override
    public void cleanupPath(HousekeepingPath housekeepingPath) {
        System.out.println(housekeepingPath.toString());
    }
}
