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
package com.expediagroup.beekeeper.scheduler.apiary.messaging;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageReader;

import com.expediagroup.beekeeper.scheduler.apiary.filter.ListenerEventFilter;

public class FilteringMessageReader implements MessageReader {

  private final MessageReader delegate;
  private final List<ListenerEventFilter> filters;

  public FilteringMessageReader(MessageReader delegate, List<ListenerEventFilter> filters) {
    this.delegate = delegate;
    this.filters = filters;
  }

  @Override
  public Optional<MessageEvent> read() {
    Optional<MessageEvent> messageEvent = delegate.read();
    if (messageEvent.isEmpty()) {
      return Optional.empty();
    }
    if (isFiltered(messageEvent.get())) {
      delete(messageEvent.get());
      return Optional.empty();
    }
    return messageEvent;
  }

  private boolean isFiltered(MessageEvent messageEvent) {
    ListenerEvent listenerEvent = messageEvent.getEvent();
    if (listenerEvent == null) {
      return true;
    }
    return filters.stream()
        .anyMatch(filter -> filter.filter(listenerEvent));
  }

  @Override
  public void delete(MessageEvent messageEvent) {
    delegate.delete(messageEvent);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
