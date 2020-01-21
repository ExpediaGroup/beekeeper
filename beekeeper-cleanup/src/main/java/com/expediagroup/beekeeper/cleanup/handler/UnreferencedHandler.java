package com.expediagroup.beekeeper.cleanup.handler;

import java.time.LocalDateTime;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;

import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.cleanup.path.aws.S3PathCleaner;
import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;

@Component
public class UnreferencedHandler extends GenericHandler {

  private static final LifecycleEventType EVENT_TYPE = LifecycleEventType.UNREFERENCED;
  private S3PathCleaner s3PathCleaner;

  @Autowired
  public UnreferencedHandler(S3PathCleaner s3PathCleaner) {
    this.s3PathCleaner = s3PathCleaner;
  }

  @VisibleForTesting
  void setS3PathCleaner(S3PathCleaner cleaner) {
    s3PathCleaner = cleaner;
  }

  @Override
  public LifecycleEventType getLifecycleType() {
    return EVENT_TYPE;
  }

  @Override
  public PathCleaner getPathCleaner() { return s3PathCleaner; }

  @Override
  public Page<EntityHousekeepingPath> findRecordsToClean(LocalDateTime instant, Pageable pageable) {
    return housekeepingPathRepository.findRecordsForCleanupByModifiedTimestamp(instant, pageable);
  }
}
