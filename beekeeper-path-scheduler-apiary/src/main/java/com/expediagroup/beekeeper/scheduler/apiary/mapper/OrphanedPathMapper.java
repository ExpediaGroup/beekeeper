package com.expediagroup.beekeeper.scheduler.apiary.mapper;

import com.expedia.apiary.extensions.receiver.common.event.*;
import com.expediagroup.beekeeper.scheduler.apiary.model.EventModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;


import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

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
