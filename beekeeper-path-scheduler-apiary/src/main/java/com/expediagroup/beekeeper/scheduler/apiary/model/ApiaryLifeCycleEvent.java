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
package com.expediagroup.beekeeper.scheduler.apiary.model;

import com.expedia.apiary.extensions.receiver.common.event.*;

import com.expediagroup.beekeeper.core.model.LifeCycleEventType;

import static com.expediagroup.beekeeper.core.model.LifeCycleEventType.EXPIRED;
import static com.expediagroup.beekeeper.core.model.LifeCycleEventType.UNREFERENCED;

import java.util.ArrayList;

public enum ApiaryLifeCycleEvent {
    ADD_PARTITION(AddPartitionEvent.class) {
        @Override public ArrayList<EventModel> gatherEventPaths(ListenerEvent event, Boolean isUnreferenced, Boolean isExpired) {
            ArrayList<EventModel> eventPaths = new ArrayList<>();
            AddPartitionEvent addPartitionEvent = (AddPartitionEvent) event;
            eventPaths.add(new EventModel(EXPIRED, addPartitionEvent.getTableLocation()));
            return eventPaths;
        }
    },

    ALTER_PARTITION(AlterPartitionEvent.class) {
        @Override public ArrayList<EventModel> gatherEventPaths(ListenerEvent event, Boolean isUnreferenced, Boolean isExpired) {
            ArrayList<EventModel> eventPaths = new ArrayList<>();
            AlterPartitionEvent alterPartitionEvent = (AlterPartitionEvent) event;

            Boolean isMetadataUpdate = isMetadataUpdate(alterPartitionEvent.getOldPartitionLocation(), alterPartitionEvent.getPartitionLocation());

            if (isUnreferenced && !isMetadataUpdate) {
                eventPaths.add(new EventModel(UNREFERENCED, alterPartitionEvent.getOldPartitionLocation()));
            }

            if (isExpired) {
                eventPaths.add(new EventModel(EXPIRED, alterPartitionEvent.getTableLocation()));
            }

            return eventPaths;
        }
    },

    ALTER_TABLE(AlterTableEvent.class) {
        @Override public ArrayList<EventModel> gatherEventPaths(ListenerEvent event, Boolean isUnreferenced, Boolean isExpired) {
            ArrayList<EventModel> eventPaths = new ArrayList<>();
            AlterTableEvent alterTableEvent = (AlterTableEvent) event;

            Boolean isMetadataUpdate = isMetadataUpdate(alterTableEvent.getOldTableLocation(), alterTableEvent.getTableLocation());

            if (isUnreferenced && !isMetadataUpdate) {
                eventPaths.add(new EventModel(UNREFERENCED, alterTableEvent.getOldTableLocation()));
            }

            if (isExpired) {
                eventPaths.add(new EventModel(EXPIRED, alterTableEvent.getTableLocation()));
            }

            return eventPaths;
        }
    },

    CREATE_TABLE(CreateTableEvent.class) {
        @Override public ArrayList<EventModel> gatherEventPaths(ListenerEvent event, Boolean isUnreferenced, Boolean isExpired) {
            ArrayList<EventModel> eventPaths = new ArrayList<>();
            CreateTableEvent createTableEvent = (CreateTableEvent) event;
            eventPaths.add(new EventModel(EXPIRED, createTableEvent.getTableLocation()));
            return eventPaths;
        }
    },

    DROP_PARTITION(DropPartitionEvent.class) {
        @Override public ArrayList<EventModel> gatherEventPaths(ListenerEvent event, Boolean isUnreferenced, Boolean isExpired) {
            ArrayList<EventModel> eventPaths = new ArrayList<>();
            DropPartitionEvent dropPartitionEvent = (DropPartitionEvent) event;
            eventPaths.add(new EventModel(UNREFERENCED, dropPartitionEvent.getPartitionLocation()));
            return eventPaths;
        }
    },

    DROP_TABLE(DropTableEvent.class) {
        @Override public ArrayList<EventModel> gatherEventPaths(ListenerEvent event, Boolean isUnreferenced, Boolean isExpired) {
            ArrayList<EventModel> eventPaths = new ArrayList<>();
            DropTableEvent dropTableEvent = (DropTableEvent) event;
            eventPaths.add(new EventModel(UNREFERENCED, dropTableEvent.getTableLocation()));
            return eventPaths;
        }
    },

    INSERT(InsertTableEvent.class) {
        @Override public ArrayList<EventModel> gatherEventPaths(ListenerEvent event, Boolean isUnreferenced, Boolean isExpired) {
            return new ArrayList<>();
        }
    };

    private final Class<? extends ListenerEvent> eventClass;

    private ApiaryLifeCycleEvent(Class<? extends ListenerEvent> eventClass) {
        if (eventClass == null) {
            throw new NullPointerException("Parameter eventClass is required");
        } else {
            this.eventClass = eventClass;
        }
    }

    public class EventModel {
        public LifeCycleEventType lifeCycleEvent;
        public String cleanupPath;

        public EventModel(LifeCycleEventType lifeCycleEvent, String cleanup) {
            this.lifeCycleEvent = lifeCycleEvent;
            this.cleanupPath = cleanup;
        }
    }

    protected boolean isMetadataUpdate(String oldLocation, String location) {
        return location == null || oldLocation == null || oldLocation.equals(location);
    }

    public abstract ArrayList<EventModel> gatherEventPaths(ListenerEvent event, Boolean isUnreferenced, Boolean isExpired);

}

