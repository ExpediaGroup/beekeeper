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

import static com.expediagroup.beekeeper.core.model.LifeCycleEventType.EXPIRED;

@Component
public class ExpiredPathMapper extends MessageEventMapper {

    private static final Logger log = LoggerFactory.getLogger(ExpiredPathMapper.class);

    @Autowired
    public ExpiredPathMapper(
        @Value("${properties.beekeeper.default-expired-cleanup-delay}") String cleanupDelay,
        @Value("${properties.apiary.expired-cleanup-delay-property-key}") String hivePropertyKey
    ) {
        super(cleanupDelay, hivePropertyKey, EXPIRED);
    }

    @Override
    protected List<EventModel> generateEventModels(ListenerEvent listenerEvent) {
        Boolean tableWatchingExpired = this.checkIfTablePropertyExists(listenerEvent.getTableParameters());
        List<EventModel> eventPaths = new ArrayList<>();

        if (!tableWatchingExpired) {
            return eventPaths;
        }

        switch (listenerEvent.getEventType()) {
            case ALTER_PARTITION:
                eventPaths.add(new EventModel(lifeCycleEventType, ((AlterPartitionEvent) listenerEvent).getOldPartitionLocation()));
                break;
            case ALTER_TABLE:
                eventPaths.add(new EventModel(lifeCycleEventType, ((AlterTableEvent) listenerEvent).getOldTableLocation()));
                break;
            case CREATE_TABLE:
                eventPaths.add(new EventModel(lifeCycleEventType, ((CreateTableEvent) listenerEvent).getTableLocation()));
                break;
            case ADD_PARTITION:
                eventPaths.add(new EventModel(lifeCycleEventType, ((AddPartitionEvent) listenerEvent).getPartitionLocation()));
                break;
        }


        return eventPaths;
    }
}
