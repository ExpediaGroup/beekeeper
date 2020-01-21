package com.expediagroup.beekeeper.cleanup.handler;

import java.time.LocalDateTime;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.model.PathStatus;
import com.expediagroup.beekeeper.core.repository.HousekeepingPathRepository;

public abstract class GenericHandler {

  private final Logger log = LoggerFactory.getLogger(GenericHandler.class);
  @Autowired HousekeepingPathRepository housekeepingPathRepository;

  public abstract LifecycleEventType getLifecycleType();

  public abstract PathCleaner getPathCleaner();

  protected abstract void setPathCleaner(PathCleaner cleaner);

  public abstract Page<EntityHousekeepingPath> findRecordsToClean(LocalDateTime instant, Pageable pageable);

  public void processPage(List<EntityHousekeepingPath> pageContent, boolean dryRunEnabled) {
    if (dryRunEnabled) {
      pageContent.forEach(this::cleanUpPath);
    } else {
      pageContent.forEach(this::cleanupContent);
    }
  }

  private void cleanUpPath(EntityHousekeepingPath housekeepingPath) {
    PathCleaner pathCleaner = getPathCleaner();
    pathCleaner.cleanupPath(housekeepingPath);
  }

  private void cleanupContent(EntityHousekeepingPath housekeepingPath) {
    try {
      log.info("Cleaning up path \"{}\"", housekeepingPath.getPath());
      cleanUpPath(housekeepingPath);
      updateAttemptsAndStatus(housekeepingPath, PathStatus.DELETED);
    } catch (Exception e) {
      updateAttemptsAndStatus(housekeepingPath, PathStatus.FAILED);
      log.warn("Unexpected exception deleting \"{}\"", housekeepingPath.getPath(), e);
    }
  }

  private void updateAttemptsAndStatus(EntityHousekeepingPath housekeepingPath, PathStatus status) {
    housekeepingPath.setCleanupAttempts(housekeepingPath.getCleanupAttempts() + 1);
    housekeepingPath.setPathStatus(status);
    housekeepingPathRepository.save(housekeepingPath);
  }
}
