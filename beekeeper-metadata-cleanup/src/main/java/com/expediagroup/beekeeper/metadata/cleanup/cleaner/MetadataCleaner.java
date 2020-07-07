package com.expediagroup.beekeeper.metadata.cleanup.cleaner;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;

public interface MetadataCleaner {

  void cleanupMetadata(HousekeepingMetadata housekeepingMetadata);

}