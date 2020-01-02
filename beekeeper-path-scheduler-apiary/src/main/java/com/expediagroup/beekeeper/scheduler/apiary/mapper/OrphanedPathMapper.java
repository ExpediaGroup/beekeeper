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
package com.expediagroup.beekeeper.scheduler.apiary.mapper;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropTableEvent;

import com.expediagroup.beekeeper.scheduler.apiary.model.EventModel;

@Component
public class OrphanedPathMapper extends MessageEventMapper {

    private static final Logger log = LoggerFactory.getLogger(OrphanedPathMapper.class);

    @Autowired
    public OrphanedPathMapper(
        @Value("${properties.beekeeper.default-cleanup-delay}") String cleanupDelay,
        @Value("${properties.apiary.cleanup-delay-property-key}") String hivePropertyKey
    ) {
        super(cleanupDelay, hivePropertyKey, UNREFERENCED);
    }

    @Override
    protected List<EventModel> generateEventModels(ListenerEvent listenerEvent) {
        Boolean tableWatchingUnreferenced = this.checkIfTablePropertyExists(listenerEvent.getTableParameters());
        List<EventModel> eventPaths = new ArrayList<>();

        if (!tableWatchingUnreferenced) {
            return eventPaths;
        }

        switch(listenerEvent.getEventType()) {
            case ALTER_PARTITION:
                AlterPartitionEvent alterPartitionEvent = (AlterPartitionEvent) listenerEvent;
                if (!isMetadataUpdate(alterPartitionEvent.getOldPartitionLocation(), alterPartitionEvent.getPartitionLocation())) {
                    eventPaths.add(new EventModel(lifeCycleEventType, alterPartitionEvent.getOldPartitionLocation()));
                }
                break;
            case ALTER_TABLE:
                AlterTableEvent alterTableEvent = (AlterTableEvent) listenerEvent;
                if (!isMetadataUpdate(alterTableEvent.getOldTableLocation(), alterTableEvent.getTableLocation())) {
                    eventPaths.add(new EventModel(lifeCycleEventType, alterTableEvent.getOldTableLocation()));
                }
                break;
            case DROP_TABLE: eventPaths.add(new EventModel(lifeCycleEventType, ((DropTableEvent) listenerEvent).getTableLocation())); break;
            case DROP_PARTITION: eventPaths.add(new EventModel(lifeCycleEventType, ((DropPartitionEvent) listenerEvent).getPartitionLocation())); break;
        }

        return eventPaths;
    }
}
