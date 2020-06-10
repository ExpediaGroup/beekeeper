package com.expediagroup.beekeeper.scheduler.apiary.model;

import com.expediagroup.beekeeper.core.model.LifecycleEventType;

public class UnreferencedEventModel implements EventModel {

  private final LifecycleEventType lifecycleEventType;
  private final String cleanupPath;

  public UnreferencedEventModel(LifecycleEventType lifecycleEvent, String cleanupPath) {
    this.lifecycleEventType = lifecycleEvent;
    this.cleanupPath = cleanupPath;
  }

  public String getCleanupPath() {
    return cleanupPath;
  }

  @Override
  public LifecycleEventType getLifecycleEventType() {
    return lifecycleEventType;
  }
}
