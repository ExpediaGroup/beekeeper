package com.expediagroup.beekeeper.scheduler.apiary.model;

import com.expediagroup.beekeeper.core.model.LifecycleEventType;

public class EventModel {

  public final LifecycleEventType lifecycleEvent;
  public final String cleanupPath;

  public EventModel(LifecycleEventType lifecycleEvent, String cleanupPath) {
    this.lifecycleEvent = lifecycleEvent;
    this.cleanupPath = cleanupPath;
  }
}

