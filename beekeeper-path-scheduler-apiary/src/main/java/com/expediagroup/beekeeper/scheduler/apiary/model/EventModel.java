package com.expediagroup.beekeeper.scheduler.apiary.model;

import com.expediagroup.beekeeper.core.model.LifecycleEventType;

public class EventModel {

  private final LifecycleEventType lifecycleEvent;
  private final String cleanupPath;

  public EventModel(LifecycleEventType lifecycleEvent, String cleanupPath) {
    this.lifecycleEvent = lifecycleEvent;
    this.cleanupPath = cleanupPath;
  }

  public String getCleanupPath() {
    return cleanupPath;
  }

  public LifecycleEventType getLifecycleEvent() {
    return lifecycleEvent;
  }
}
