package com.expediagroup.beekeeper.scheduler.apiary.mapper;

import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.LifeCycleEventType;
import com.expediagroup.beekeeper.core.model.PathStatus;
import com.expediagroup.beekeeper.scheduler.apiary.model.EventModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class MessageEventMapper {

    protected final String CLIENT_ID = "apiary-metastore-event";
    protected final String FOREVER = "P356D";
    private static final Logger log = LoggerFactory.getLogger(MessageEventMapper.class);

    protected abstract List<EventModel> generateEventModels(ListenerEvent listenerEvent);

    protected final String cleanupDelay;
    protected final String hivePropertyKey;
    protected final LifeCycleEventType lifeCycleEventType;

    public MessageEventMapper(String cleanupDelay, String hivePropertyKey, LifeCycleEventType eventType) {
        this.cleanupDelay = cleanupDelay;
        this.hivePropertyKey = hivePropertyKey;
        this.lifeCycleEventType = eventType;
    }

    /**
     * Generates housekeeping paths for a given event.
     * @param listenerEvent Listener event from the current message
     * @return list of housekeeping paths. This can be an empty list if there are no valid paths.
     */
    public List<HousekeepingPath> generateHouseKeepingPaths(ListenerEvent listenerEvent) {
        return this.generateEventModels(listenerEvent).stream()
                .map(event -> generatePath(event.lifeCycleEvent, listenerEvent, event.cleanupPath))
                .collect(Collectors.toList());
    }

    protected final String getHivePropertyKey() { return this.hivePropertyKey; }
    protected final String getCleanupDelay() { return this.cleanupDelay; }
    protected final LifeCycleEventType getLifeCycleEventType() { return this.lifeCycleEventType; }


    /**
     * Checks if the current event is a metadata event.
     * This is determined if the old location string is the same as the new location string.
     * @param oldLocation Old event location
     * @param location New Event location
     * @return boolean True if old == new event location. False otherwise.
     */
    protected final boolean isMetadataUpdate(String oldLocation, String location) {
        return location == null || oldLocation == null || oldLocation.equals(location);
    }

    /**
     * Generates a path to clean up via EntityHousekeepingPath.Builder
     * @param lifeCycleType The type of lifecycle cleanup event this path is
     * @param listenerEvent The current event we're generating this path for
     * @param cleanupLocation The path of the current item to cleanup
     * @return EntityHouseKeepingPath Path object for the given parameters
     */
    protected final EntityHousekeepingPath generatePath(LifeCycleEventType lifeCycleType, ListenerEvent listenerEvent, String cleanupLocation) {
        EntityHousekeepingPath.Builder builder = new EntityHousekeepingPath.Builder()
                .pathStatus(PathStatus.SCHEDULED)
                .creationTimestamp(LocalDateTime.now())
                .cleanupDelay(extractCleanupDelay(listenerEvent))
                .lifeCycleType(lifeCycleType.name())
                .apiaryEventType(listenerEvent.getEventType().name())
                .clientId(CLIENT_ID)
                .tableName(listenerEvent.getTableName())
                .databaseName(listenerEvent.getDbName())
                .path(cleanupLocation);

        return builder.build();
    }


    /**
     * Get's a boolean flag if the given tableparameters exist for this mapper
     * @param tableParameters
     * @return boolean
     */
    protected final Boolean checkIfTablePropertyExists(Map<String,String> tableParameters) {
        LifeCycleEventType type = this.getLifeCycleEventType();
        return Boolean.valueOf(tableParameters.get(type.getTableParameterName()));
    }

    /**
     * Extracts the cleanup delay from the given event.
     * If the cleanupDelay on the event cannot be parsed, use the predefined default value.
     * @param listenerEvent Current event from Apiary
     * @return Duration Parsed Duration object from the event or the default value.
     */
    private final Duration extractCleanupDelay(ListenerEvent listenerEvent) {
        String propertyKey = this.getHivePropertyKey();
        String defaultValue = this.getCleanupDelay();
        String tableCleanupDelay = listenerEvent.getTableParameters().getOrDefault(propertyKey, defaultValue);

        try {
            return Duration.parse(tableCleanupDelay);
        } catch (DateTimeParseException e) {
            log.error("Text '{}' cannot be parsed to a Duration for table '{}.{}'. Using default setting.",
                    tableCleanupDelay, listenerEvent.getDbName(), listenerEvent.getTableName());
            return Duration.parse(defaultValue);
        }
    }
}
