package com.expediagroup.beekeeper.metadata.cleanup.table;

import com.expediagroup.beekeeper.core.model.HousekeepingPath;

public interface MetadataCleaner {

  void cleanupMetadata(HousekeepingPath housekeepingPath);

}
