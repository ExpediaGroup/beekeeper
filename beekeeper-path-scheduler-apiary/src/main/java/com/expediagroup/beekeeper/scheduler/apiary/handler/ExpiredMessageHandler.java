package com.expediagroup.beekeeper.scheduler.apiary.handler;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;

import com.expediagroup.beekeeper.core.model.EntityHousekeepingTable;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.scheduler.apiary.filter.EventTypeListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.ListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.TableParameterListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.model.ExpiredEventModel;

@Component
public class ExpiredMessageHandler extends MessageEventHandler<EntityHousekeepingTable, ExpiredEventModel> {

  private static final Logger log = LoggerFactory.getLogger(UnreferencedMessageHandler.class);
  private static final String EXPIRED_DATA_RETENTION_PERIOD_PROPERTY_KEY = "beekeeper.expired.data.retention.period";
  private static final LifecycleEventType LIFECYCLE_EVENT_TYPE = LifecycleEventType.EXPIRED;
  private static final List<Class<? extends ListenerEvent>> EVENT_CLASSES = List.of(AlterTableEvent.class);

  private final List<ListenerEventFilter> filters;

  @Autowired
  public ExpiredMessageHandler(
      @Value("${properties.beekeeper.default-expiration-delay}") String cleanupDelay
  ) {
    super(cleanupDelay, EXPIRED_DATA_RETENTION_PERIOD_PROPERTY_KEY, LIFECYCLE_EVENT_TYPE);
    this.filters = List.of(
        new EventTypeListenerEventFilter(EVENT_CLASSES),
        new TableParameterListenerEventFilter());
  }

  public ExpiredMessageHandler(
      @Value("${properties.beekeeper.default-expiration-delay}") String cleanupDelay,
      List<ListenerEventFilter> filters
  ) {
    super(cleanupDelay, EXPIRED_DATA_RETENTION_PERIOD_PROPERTY_KEY, LIFECYCLE_EVENT_TYPE);
    this.filters = filters;
  }

  @Override
  protected List<ListenerEventFilter> getFilters() {
    return filters;
  }

  @Override
  protected EntityHousekeepingTable generateHousekeepingEntity(ExpiredEventModel eventModel,
      ListenerEvent listenerEvent) {
    EntityHousekeepingTable.Builder builder = new EntityHousekeepingTable.Builder()
        .housekeepingStatus(SCHEDULED)
        .creationTimestamp(LocalDateTime.now())
        .cleanupDelay(extractCleanupDelay(listenerEvent))
        .lifecycleType(eventModel.getLifecycleEventType().name())
        .clientId(CLIENT_ID)
        .tableName(listenerEvent.getTableName())
        .databaseName(listenerEvent.getDbName());

    return builder.build();
  }

  @Override
  protected List<ExpiredEventModel> generateEventModels(ListenerEvent event) {
    List<ExpiredEventModel> eventPaths = new ArrayList<>();

    switch (event.getEventType()) {
    case ALTER_PARTITION:
    case ALTER_TABLE:
      eventPaths.add(new ExpiredEventModel(LIFECYCLE_EVENT_TYPE));
      break;
    }

    return eventPaths;
  }
}
