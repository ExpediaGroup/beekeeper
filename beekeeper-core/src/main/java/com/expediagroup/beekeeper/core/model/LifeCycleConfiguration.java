package com.expediagroup.beekeeper.core.model;

import java.util.Map;

public class LifeCycleConfiguration {
    private LifeCycleEventType type;
    private String hivePropertyKey;
    private String defaultDeletionDelay;

    public LifeCycleConfiguration(LifeCycleEventType type, String hivePropertyKey, String deletionDelay) {
        this.type = type;
        this.hivePropertyKey = hivePropertyKey;
        this.defaultDeletionDelay = deletionDelay;
    }

    public LifeCycleEventType getType() { return this.type; };
    public String getHivePropertyKey() { return this.hivePropertyKey; };
    public String getDefaultDeletionDelay() { return this.defaultDeletionDelay; };

    public Boolean getBoolean(Map<String,String> tableParameters) {
        return Boolean.valueOf(tableParameters.get(this.type.getTableParameterName()));
    }
}
