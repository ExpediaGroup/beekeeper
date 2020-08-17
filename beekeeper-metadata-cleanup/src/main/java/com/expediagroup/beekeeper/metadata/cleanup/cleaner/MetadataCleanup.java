package com.expediagroup.beekeeper.metadata.cleanup.cleaner;

import java.time.LocalDateTime;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;

public interface MetadataCleanup {

  Page<HousekeepingMetadata> findRecordsToClean(LocalDateTime instant, Pageable pageable);

  void cleanupContent(HousekeepingMetadata housekeepingMetadata, LocalDateTime instant, boolean dryRunEnabled);

  boolean cleanupMetadata(HousekeepingMetadata housekeepingMetadata, LocalDateTime instant, boolean dryRunEnabled);
}

