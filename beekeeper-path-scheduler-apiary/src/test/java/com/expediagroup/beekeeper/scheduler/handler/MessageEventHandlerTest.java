package com.expediagroup.beekeeper.scheduler.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;

import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.scheduler.apiary.filter.ListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.filter.WhitelistedListenerEventFilter;
import com.expediagroup.beekeeper.scheduler.apiary.handler.UnreferencedMessageHandler;

@ExtendWith(MockitoExtension.class)
public class MessageEventHandlerTest {

  @InjectMocks private final UnreferencedMessageHandler msgHandler = new UnreferencedMessageHandler("", "");

  @Mock private MessageEvent messageEvent;
  @Mock private ListenerEvent listenerEvent;
  @Mock private WhitelistedListenerEventFilter filter;
  @Spy private List<ListenerEventFilter> filterList;

  @Before
  public void setup() throws Exception {
    filterList = new ArrayList<>();
    filterList.add(filter);
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void typicalHandleMessage() {
    when(messageEvent.getEvent()).thenReturn(listenerEvent);
    when(filter.filter(listenerEvent)).thenReturn(false);
    List<HousekeepingPath> paths = msgHandler.handleMessage(messageEvent);
    assertThat(paths.size()).isEqualTo(1);
  }
}
