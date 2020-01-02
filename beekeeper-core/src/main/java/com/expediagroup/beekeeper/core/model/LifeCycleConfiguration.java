/**
 * Copyright (C) 2019 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.expediagroup.beekeeper.core.model;

import java.util.Map;

public class LifeCycleConfiguration {
    private LifecycleEventType type;
    private String hivePropertyKey;
    private String defaultDeletionDelay;

    public LifeCycleConfiguration(LifecycleEventType type, String hivePropertyKey, String deletionDelay) {
        this.type = type;
        this.hivePropertyKey = hivePropertyKey;
        this.defaultDeletionDelay = deletionDelay;
    }

    public LifecycleEventType getType() { return this.type; };
    public String getHivePropertyKey() { return this.hivePropertyKey; };
    public String getDefaultDeletionDelay() { return this.defaultDeletionDelay; };

    public Boolean getBoolean(Map<String,String> tableParameters) {
        return Boolean.valueOf(tableParameters.get(this.type.getTableParameterName()));
    }
}
