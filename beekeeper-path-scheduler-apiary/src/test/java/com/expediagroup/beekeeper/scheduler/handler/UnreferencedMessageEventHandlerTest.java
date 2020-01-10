package com.expediagroup.beekeeper.scheduler.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;
import static com.expediagroup.beekeeper.scheduler.apiary.filter.FilterType.EVENT_TYPE;
import static com.expediagroup.beekeeper.scheduler.apiary.filter.FilterType.METADATA_ONLY;
import static com.expediagroup.beekeeper.scheduler.apiary.filter.FilterType.TABLE_PARAMETER;
import static com.expediagroup.beekeeper.scheduler.apiary.filter.FilterType.WHITELISTED;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.event.DropTableEvent;
import com.expedia.apiary.extensions.receiver.common.event.EventType;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;

import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.scheduler.apiary.filter.EventTypeListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.FilterType;
import com.expediagroup.beekeeper.scheduler.apiary.filter.ListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.MetadataOnlyListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.TableParameterListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.WhitelistedListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.handler.UnreferencedMessageHandler;

@ExtendWith(MockitoExtension.class)
public class UnreferencedMessageEventHandlerTest {

  private static final String UNREF_HIVE_KEY = "beekeeper.unreferenced.data.retention.period";
  private static final String UNREF_DEFAULT = "P3D";
  private static final String PATH = "path";
  private static final String OLD_PATH = "old_path";
  private static final String DATABASE = "database";
  private static final String TABLE = "table";
  private static final String OLD_TABLE = "old_table";
  private static final Integer CLEANUP_ATTEMPTS = 0;
  private static final String CLEANUP_DELAY = "P7D";
  private static final LocalDateTime CREATION_TIMESTAMP = LocalDateTime.now();

  private static final Map<String, String> defaultProperties = Map.of(
      UNREFERENCED.getTableParameterName(), "true",
      UNREF_HIVE_KEY, CLEANUP_DELAY
  );

  @InjectMocks private final UnreferencedMessageHandler msgHandler = new UnreferencedMessageHandler(
      UNREF_HIVE_KEY, UNREF_DEFAULT);
  @Spy private final EnumMap<FilterType, ListenerEventFilter> filterMap = new EnumMap<>(FilterType.class);
  @Mock private MessageEvent messageEvent;
  @Mock private TableParameterListenerEventFilter tableParameterListenerEventFilter;
  @Mock private MetadataOnlyListenerEventFilter metadataOnlyListenerEventFilter;
  @Mock private EventTypeListenerEventFilter eventTypeListenerEventFilter;
  @Mock private WhitelistedListenerEventFilter whiteListedFilter;
  @Mock private AlterPartitionEvent alterPartitionEvent;
  @Mock private AlterTableEvent alterTableEvent;
  @Mock private DropTableEvent dropTableEvent;
  @Mock private DropPartitionEvent dropPartitionEvent;

  @Before
  public void setup() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void ignoreUnconfiguredTables() {
    setupFilterMap(alterPartitionEvent, true, false, true, false, true, false, true, false);
    when(alterPartitionEvent.getTableParameters()).thenReturn(Collections.emptyMap());
    when(messageEvent.getEvent()).thenReturn(alterPartitionEvent);
    List<HousekeepingPath> paths = msgHandler.handleMessage(messageEvent);
    assertThat(paths.isEmpty()).isTrue();
  }

  @Test
  public void typicalHandleAlterPartitionEvent() {
    setupListenerEvent(alterPartitionEvent);
    setupFilterMap(alterPartitionEvent, true, false, true, false, true, false, true, false);
    when(alterPartitionEvent.getTableParameters()).thenReturn(defaultProperties);
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    when(alterPartitionEvent.getOldPartitionLocation()).thenReturn(OLD_PATH);
    List<HousekeepingPath> paths = msgHandler.handleMessage(messageEvent);
    assertPath(paths, CLEANUP_DELAY);
  }

  @Test
  public void typicalHandleAlterTableEvent() {
    setupListenerEvent(alterTableEvent);
    setupFilterMap(alterTableEvent, true, false, true, false, true, false, true, false);
    when(alterTableEvent.getTableParameters()).thenReturn(defaultProperties);
    when(alterTableEvent.getEventType()).thenReturn(EventType.ALTER_TABLE);
    when(alterTableEvent.getOldTableLocation()).thenReturn(OLD_PATH);
    List<HousekeepingPath> paths = msgHandler.handleMessage(messageEvent);
    assertPath(paths, CLEANUP_DELAY);
  }

  @Test
  public void typicalHandleDropPartitionEvent() {
    setupListenerEvent(dropPartitionEvent);
    setupFilterMap(dropPartitionEvent, true, false, true, false, true, false, true, false);
    when(dropPartitionEvent.getTableParameters()).thenReturn(defaultProperties);
    when(dropPartitionEvent.getEventType()).thenReturn(EventType.DROP_PARTITION);
    when(dropPartitionEvent.getPartitionLocation()).thenReturn(OLD_PATH);
    List<HousekeepingPath> paths = msgHandler.handleMessage(messageEvent);
    assertPath(paths, CLEANUP_DELAY);
  }

  @Test
  public void typicalHandleDropTableEvent() {
    setupListenerEvent(dropTableEvent);
    setupFilterMap(dropTableEvent, true, false, true, false, true, false, true, false);
    when(dropTableEvent.getTableParameters()).thenReturn(defaultProperties);
    when(dropTableEvent.getEventType()).thenReturn(EventType.DROP_TABLE);
    when(dropTableEvent.getTableLocation()).thenReturn(OLD_PATH);
    List<HousekeepingPath> paths = msgHandler.handleMessage(messageEvent);
    assertPath(paths, CLEANUP_DELAY);
  }

  @Test
  public void typicalHandleDefaultDelay() {
    setupListenerEvent(alterPartitionEvent);
    setupFilterMap(alterPartitionEvent, true, false, true, false, true, false, true, false);
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    when(alterPartitionEvent.getOldPartitionLocation()).thenReturn(OLD_PATH);
    when(alterPartitionEvent.getTableParameters()).thenReturn(
        Map.of(UNREFERENCED.getTableParameterName(), "true")
    );
    List<HousekeepingPath> paths = msgHandler.handleMessage(messageEvent);
    assertPath(paths, UNREF_DEFAULT);
  }

  @Test
  public void handleDefaultDelayOnDurationException() {
    setupListenerEvent(alterPartitionEvent);
    setupFilterMap(alterPartitionEvent, true, false, true, false, true, false, true, false);
    when(alterPartitionEvent.getEventType()).thenReturn(EventType.ALTER_PARTITION);
    when(alterPartitionEvent.getOldPartitionLocation()).thenReturn(OLD_PATH);
    when(alterPartitionEvent.getTableParameters()).thenReturn(
        Map.of(UNREFERENCED.getTableParameterName(), "true", UNREF_HIVE_KEY, "1")
    );
    List<HousekeepingPath> paths = msgHandler.handleMessage(messageEvent);
    assertPath(paths, UNREF_DEFAULT);
  }

  private void setupFilterMap(ListenerEvent listenerEvent,
      boolean whiteListEnabled, boolean whitelistValue,
      boolean eventTypeEnabled, boolean eventTypeValue,
      boolean metadataEnabled, boolean metadataValue,
      boolean tableParameterEnabled, boolean tableParameterValue
  ) {
    List<StubFilterMapValue> stubFilters = List.of(
        new StubFilterMapValue(whiteListedFilter, WHITELISTED, whiteListEnabled, whitelistValue),
        new StubFilterMapValue(eventTypeListenerEventFilter, EVENT_TYPE, eventTypeEnabled, eventTypeValue),
        new StubFilterMapValue(metadataOnlyListenerEventFilter, METADATA_ONLY, metadataEnabled, metadataValue),
        new StubFilterMapValue(tableParameterListenerEventFilter, TABLE_PARAMETER, tableParameterEnabled,
            tableParameterValue)
    );

    for (StubFilterMapValue stub : stubFilters) {
      if (stub.enabled) {
        when(stub.filterObj.filter(listenerEvent)).thenReturn(stub.returnValue);
        filterMap.put(stub.type, stub.filterObj);
      }
    }
  }

  private void setupListenerEvent(ListenerEvent listenerEvent) {
    when(messageEvent.getEvent()).thenReturn(listenerEvent);
    when(listenerEvent.getDbName()).thenReturn(DATABASE);
    when(listenerEvent.getTableName()).thenReturn(TABLE);
  }

  private void assertPath(List<HousekeepingPath> paths, String cleanupDelay) {
    assertThat(paths.size()).isEqualTo(1);
    HousekeepingPath path = paths.get(0);
    LocalDateTime now = LocalDateTime.now();
    assertThat(path.getPath()).isEqualTo(OLD_PATH);
    assertThat(path.getTableName()).isEqualTo(TABLE);
    assertThat(path.getDatabaseName()).isEqualTo(DATABASE);
    assertThat(path.getCleanupAttempts()).isEqualTo(CLEANUP_ATTEMPTS);
    assertThat(path.getCleanupDelay()).isEqualTo(Duration.parse(cleanupDelay));
    assertThat(path.getModifiedTimestamp()).isNull();
    assertThat(path.getCreationTimestamp()).isBetween(CREATION_TIMESTAMP, now);
    assertThat(path.getCleanupTimestamp()).isEqualTo(path.getCreationTimestamp().plus(Duration.parse(cleanupDelay)));
  }

  private class StubFilterMapValue {

    ListenerEventFilter filterObj;
    FilterType type;
    boolean enabled;
    boolean returnValue;

    StubFilterMapValue(ListenerEventFilter filterObj, FilterType type, boolean enabled, boolean returnValue) {
      this.filterObj = filterObj;
      this.type = type;
      this.enabled = enabled;
      this.returnValue = returnValue;
    }
  }
}
