/**
 * Copyright (C) 2019 Expedia, Inc.
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
package com.expediagroup.beekeeper.cleanup.path.hive;

import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.monitoring.TimedTaggable;
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
    @TimedTaggable("hive-paths-deleted")
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
