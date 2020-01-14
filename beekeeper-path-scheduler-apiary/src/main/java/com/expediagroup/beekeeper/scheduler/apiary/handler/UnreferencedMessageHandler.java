package com.expediagroup.beekeeper.scheduler.apiary.handler;

import static com.expediagroup.beekeeper.scheduler.apiary.filter.FilterType.EVENT_TYPE;
import static com.expediagroup.beekeeper.scheduler.apiary.filter.FilterType.METADATA_ONLY;
import static com.expediagroup.beekeeper.scheduler.apiary.filter.FilterType.TABLE_PARAMETER;
import static com.expediagroup.beekeeper.scheduler.apiary.filter.FilterType.WHITELISTED;

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
import com.expediagroup.beekeeper.scheduler.apiary.filter.FilterType;
import com.expediagroup.beekeeper.scheduler.apiary.model.EventModel;

@Component
public class UnreferencedMessageHandler extends MessageEventHandler {

  private static final Logger log = LoggerFactory.getLogger(UnreferencedMessageHandler.class);
  private static final LifecycleEventType LIFECYCLE_EVENT_TYPE = LifecycleEventType.UNREFERENCED;
  private static final List<FilterType> VALID_FILTERS = List.of(
      EVENT_TYPE, METADATA_ONLY, TABLE_PARAMETER, WHITELISTED
  );

  @Autowired
  public UnreferencedMessageHandler(
      @Value("${properties.apiary.cleanup-delay-property-key}") String hivePropertyKey,
      @Value("${properties.beekeeper.default-cleanup-delay}") String cleanupDelay
  ) {
    super(cleanupDelay, hivePropertyKey, LIFECYCLE_EVENT_TYPE, VALID_FILTERS);
  }

  @Override
  protected List<EventModel> generateEventModels(ListenerEvent event) {
    List<EventModel> eventPaths = new ArrayList<>();

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
