package com.expediagroup.beekeeper.core.model;

import java.util.Map;

public enum LifeCycleEventType {
    UNREFERENCED("beekeeper.remove.unreferenced.data"),
    EXPIRED("beekeeper.remove.expired.data");

    private String tableParameterName;

    public String getTableParameterName() {
        return this.tableParameterName;
    }

    LifeCycleEventType(String tableParameterName) { this.tableParameterName = tableParameterName; }

    public Boolean getBoolean(Map<String,String> tableParameters) {
        return Boolean.valueOf(tableParameters.get(getTableParameterName()));
    }
}
