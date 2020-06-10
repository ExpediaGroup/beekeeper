package com.expediagroup.beekeeper.scheduler.apiary.model;

import com.expediagroup.beekeeper.core.model.LifecycleEventType;

public class ExpiredEventModel implements EventModel {

  private final LifecycleEventType lifecycleEventType;

  public ExpiredEventModel(LifecycleEventType lifecycleEvent) {
    this.lifecycleEventType = lifecycleEvent;
  }

  @Override
  public LifecycleEventType getLifecycleEventType() {
    return lifecycleEventType;
  }
}
