package com.expediagroup.beekeeper.scheduler.apiary.model;

import com.expediagroup.beekeeper.core.model.LifeCycleEventType;

public class EventModel {
    public LifeCycleEventType lifeCycleEvent;
    public String cleanupPath;

    public EventModel(LifeCycleEventType lifeCycleEvent, String cleanup) {
        this.lifeCycleEvent = lifeCycleEvent;
        this.cleanupPath = cleanup;
    }
}

