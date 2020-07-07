package com.expediagroup.beekeeper.metadata.cleanup.handler;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;

import java.time.LocalDateTime;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;
import com.expediagroup.beekeeper.metadata.cleanup.cleaner.MetadataCleaner;

@Component
public class ExpiredMetadataHandler extends GenericMetadataHandler {

  private HousekeepingMetadataRepository housekeepingMetadataRepository;
  private MetadataCleaner metadataCleaner;

  @Autowired
  public ExpiredMetadataHandler(
      HousekeepingMetadataRepository housekeepingMetadataRepository,
      @Qualifier("hiveTableCleaner") MetadataCleaner metadataCleaner) {
    this.housekeepingMetadataRepository = housekeepingMetadataRepository;
    this.metadataCleaner = metadataCleaner;
  }

  // might need to overwrite the 'process page' method
  // currently only cleans up the path - need to include table clean up

  // TODO
  // will actually need to be a housekeeping metadata repository - or something more generic
  // waiting on Vedant;s changes
  @Override
  public HousekeepingMetadataRepository getHousekeepingMetadataRepository() {
    return housekeepingMetadataRepository;
  }

  @Override
  public LifecycleEventType getLifecycleType() {
    return EXPIRED;
  }

  @Override
  public MetadataCleaner getMetadataCleaner() {
    return metadataCleaner;
  }

  @Override
  public Page<HousekeepingMetadata> findRecordsToClean(LocalDateTime instant, Pageable pageable) {
    // TODO Auto-generated method stub
    // get all the records which have tables that need to be cleaned up
    return housekeepingMetadataRepository.findRecordsForCleanupByModifiedTimestamp(instant, pageable);
  }

}