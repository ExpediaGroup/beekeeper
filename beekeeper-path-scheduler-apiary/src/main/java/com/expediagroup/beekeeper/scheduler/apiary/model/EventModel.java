package com.expediagroup.beekeeper.scheduler.apiary.model;

import com.expediagroup.beekeeper.core.model.LifecycleEventType;

public class EventModel {
    public LifecycleEventType lifeCycleEvent;
    public String cleanupPath;

    public EventModel(LifecycleEventType lifeCycleEvent, String cleanup) {
        this.lifeCycleEvent = lifeCycleEvent;
        this.cleanupPath = cleanup;
    }
}

