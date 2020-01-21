package com.expediagroup.beekeeper.cleanup.handler;

import java.time.LocalDateTime;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;

@Component
public class UnreferencedHandler extends GenericHandler {

  private static final LifecycleEventType EVENT_TYPE = LifecycleEventType.UNREFERENCED;
  private PathCleaner pathCleaner;

  @Autowired
  public UnreferencedHandler(@Qualifier("s3PathCleaner") PathCleaner pathCleaner) {
    this.pathCleaner = pathCleaner;
  }

  @Override
  public LifecycleEventType getLifecycleType() {
    return EVENT_TYPE;
  }

  @Override
  public PathCleaner getPathCleaner() { return pathCleaner; }

  @Override
  protected void setPathCleaner(PathCleaner pathCleaner) { this.pathCleaner = pathCleaner; }

  @Override
  public Page<EntityHousekeepingPath> findRecordsToClean(LocalDateTime instant, Pageable pageable) {
    return housekeepingPathRepository.findRecordsForCleanupByModifiedTimestamp(instant, pageable);
  }
}
