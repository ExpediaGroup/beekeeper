package com.expediagroup.beekeeper.scheduler.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import static com.expedia.apiary.extensions.receiver.common.event.EventType.ALTER_PARTITION;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;
import static com.expediagroup.beekeeper.scheduler.apiary.filter.FilterType.TABLE_PARAMETER;
import static com.expediagroup.beekeeper.scheduler.apiary.filter.FilterType.WHITELISTED;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;

import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.scheduler.apiary.filter.FilterType;
import com.expediagroup.beekeeper.scheduler.apiary.filter.ListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.TableParameterListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.WhitelistedListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.handler.UnreferencedMessageHandler;

@ExtendWith(MockitoExtension.class)
public class MessageEventHandlerTest {

  private static final String UNREF_HIVE_KEY = "beekeeper.unreferenced.data.retention.period";
  private static final String UNREF_DEFAULT = "P3D";
  private static final Map<String, String> defaultProperties = Map.of(
      UNREFERENCED.getTableParameterName(), "true",
      UNREF_HIVE_KEY, UNREF_DEFAULT
  );

  @InjectMocks private final UnreferencedMessageHandler msgHandler = new UnreferencedMessageHandler(
      UNREF_HIVE_KEY, UNREF_DEFAULT);
  @Spy private final EnumMap<FilterType, ListenerEventFilter> filterMap = new EnumMap<>(FilterType.class);
  @Mock private MessageEvent messageEvent;
  @Mock private AlterPartitionEvent listenerEvent;
  @Mock private WhitelistedListenerEventFilter whiteListFilter;
  @Mock private TableParameterListenerEventFilter tableFilter;

  @Test
  public void typicalHandleMessage() {
    setupListenerEvent();
    filterMap.put(WHITELISTED, whiteListFilter);
    when(whiteListFilter.filter(listenerEvent, UNREFERENCED)).thenReturn(false);
    List<HousekeepingPath> paths = msgHandler.handleMessage(messageEvent);
    assertThat(paths.isEmpty()).isFalse();
  }

  @Test
  public void typicalFilterMessage() {
    when(messageEvent.getEvent()).thenReturn(listenerEvent);
    filterMap.put(WHITELISTED, whiteListFilter);
    when(whiteListFilter.filter(listenerEvent, UNREFERENCED)).thenReturn(true);
    List<HousekeepingPath> paths = msgHandler.handleMessage(messageEvent);
    assertThat(paths.isEmpty()).isTrue();
  }

  @Test
  public void ignoreUnconfiguredTables() {
    when(messageEvent.getEvent()).thenReturn(listenerEvent);
    filterMap.put(TABLE_PARAMETER, tableFilter);
    when(tableFilter.filter(listenerEvent, UNREFERENCED)).thenReturn(true);
    List<HousekeepingPath> paths = msgHandler.handleMessage(messageEvent);
    assertThat(paths.isEmpty()).isTrue();
  }

  private void setupListenerEvent() {
    when(messageEvent.getEvent()).thenReturn(listenerEvent);
    when(listenerEvent.getTableParameters()).thenReturn(defaultProperties);
    when(listenerEvent.getEventType()).thenReturn(ALTER_PARTITION);
  }
}
