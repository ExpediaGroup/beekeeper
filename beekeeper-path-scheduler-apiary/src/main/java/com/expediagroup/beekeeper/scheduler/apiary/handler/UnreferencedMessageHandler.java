package com.expediagroup.beekeeper.scheduler.apiary.handler;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;

import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.scheduler.apiary.filter.EventTypeListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.ListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.MetadataOnlyListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.TableParameterListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.WhitelistedListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.model.EventModel;

@Component
public class UnreferencedMessageHandler extends MessageEventHandler {

  private static final Logger log = LoggerFactory.getLogger(UnreferencedMessageHandler.class);
  private static final LifecycleEventType LIFECYCLE_EVENT_TYPE = LifecycleEventType.UNREFERENCED;
  private static final List<Class<? extends ListenerEventFilter>> VALID_FILTERS = List.of(
      EventTypeListenerEventFilter.class,
      MetadataOnlyListenerEventFilter.class,
      TableParameterListenerEventFilter.class,
      WhitelistedListenerEventFilter.class
  );

  @Autowired
  public UnreferencedMessageHandler(
      @Value("${properties.beekeeper.default-expired-cleanup-delay}") String cleanupDelay,
      @Value("${properties.apiary.expired-cleanup-delay-property-key}") String hivePropertyKey
  ) {
    super(cleanupDelay, hivePropertyKey, LIFECYCLE_EVENT_TYPE, VALID_FILTERS);
  }

  @Override
  protected List<EventModel> generateEventModels(ListenerEvent event) {
    Boolean tableWatchingUnreferenced = checkIfTablePropertyExists(event.getTableParameters());
    List<EventModel> eventPaths = new ArrayList<>();

    if (!tableWatchingUnreferenced) {
      return eventPaths;
    }

    switch (event.getEventType()) {
    case ALTER_PARTITION:
      eventPaths.add(new EventModel(LIFECYCLE_EVENT_TYPE, ((AlterPartitionEvent) event).getOldPartitionLocation()));
      break;
    case ALTER_TABLE:
      eventPaths.add(new EventModel(LIFECYCLE_EVENT_TYPE, ((AlterTableEvent) event).getOldTableLocation()));
      break;
    case DROP_TABLE:
      eventPaths.add(new EventModel(LIFECYCLE_EVENT_TYPE, ((DropTableEvent) event).getTableLocation()));
      break;
    case DROP_PARTITION:
      eventPaths.add(new EventModel(LIFECYCLE_EVENT_TYPE, ((DropPartitionEvent) event).getPartitionLocation()));
      break;
    }

    return eventPaths;
  }
}
