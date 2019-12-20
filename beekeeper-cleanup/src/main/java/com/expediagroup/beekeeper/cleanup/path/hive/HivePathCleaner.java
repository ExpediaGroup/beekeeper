package com.expediagroup.beekeeper.cleanup.path.hive;

import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public class HivePathCleaner implements PathCleaner {
    private static final Logger log = LoggerFactory.getLogger(HivePathCleaner.class);
    private HiveClient hiveClient;

    public HivePathCleaner(HiveClient hiveClient) {
        this.hiveClient = hiveClient;
    }

    @Override
    public boolean cleanupPath(HousekeepingPath housekeepingPath) {
        if (LocalDateTime.now().isBefore(housekeepingPath.getCleanupTimestamp())) {
            log.debug("Not deleting path \"{}\", expiration timestamp not met yet.", housekeepingPath.getPath());
            return false;
        }

        // TODO: Determine difference between partition / table from houseKeepingPath and drop accordingly
        boolean result = this.hiveClient.dropTable(
            housekeepingPath.getDatabaseName(),
            housekeepingPath.getTableName()
        );


        return result;
    }
}
