package com.expediagroup.beekeeper.metadata.cleanup.handler;

import java.time.LocalDateTime;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;
import com.expediagroup.beekeeper.metadata.cleanup.table.MetadataCleaner;

@Component
public class ExpiredMetadataHandler extends GenericMetadataHandler {

  private HousekeepingPathRepository housekeepingPathRepository;
  private MetadataCleaner metadataCleaner;

  @Autowired
  public ExpiredMetadataHandler(
      HousekeepingPathRepository housekeepingPathRepository,
      @Qualifier("hiveTableCleaner") MetadataCleaner metadataCleaner) {
    this.housekeepingPathRepository = housekeepingPathRepository;
    this.metadataCleaner = metadataCleaner;
  }

  // might need to overwrite the 'process page' method
  // currently only cleans up the path - need to include table clean up

  // TODO
  // will actually need to be a housekeeping metadata repository - or something more generic
  // waiting on Vedant;s changes
  @Override
  public HousekeepingPathRepository getHousekeepingPathRepository() {
    return housekeepingPathRepository;
  }

  @Override
  public LifecycleEventType getLifecycleType() {
    // TODO
    // update when vedants changes are merged
    // return LifecycleEventType.EXPIRED;
    return null;
  }

  @Override
  public MetadataCleaner getMetadataCleaner() {
    return metadataCleaner;
  }

  @Override
  public Page<EntityHousekeepingPath> findRecordsToClean(LocalDateTime instant, Pageable pageable) {
    // TODO Auto-generated method stub
    // return housekeepingMetadataRepository.findRecordsForCleanupByModifiedTimestamp(instant, pageable) <- get all the
    // records which have tables
    // that need to be cleaned up
    return null;
  }

}
