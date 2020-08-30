/**
 * Copyright (C) 2019-2020 Expedia, Inc.
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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageReader;

import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.scheduler.apiary.handler.MessageEventHandler;
import com.expediagroup.beekeeper.scheduler.apiary.model.BeekeeperEvent;

public class MessageReaderAdapter implements BeekeeperEventReader {

  private static final Logger log = LoggerFactory.getLogger(MessageReaderAdapter.class);

  private final MessageReader delegate;
  private final List<MessageEventHandler> handlers;

  public MessageReaderAdapter(MessageReader delegate,
      List<MessageEventHandler> handlers) {
    this.delegate = delegate;
    this.handlers = handlers;
  }

  @Override
  public Optional<BeekeeperEvent> read() {
    log.info("**** Reading beekeeper Event");
    if (delegate != null) {
      log.info("Delegate is not null ");
    } else {
      log.info("Delegate is null ");
    }
    Optional<MessageEvent> messageEvent = delegate.read();

    if (messageEvent.isEmpty()) {
      log.info("**** Message event is empty");
      return Optional.empty();
    }

    log.info("**** Message event not empty");

    MessageEvent message = messageEvent.get();

    List<HousekeepingEntity> housekeepingEntities = handlers.parallelStream()
        .map(eventHandler -> eventHandler.handleMessage(message))
        .flatMap(Collection::stream)
        .collect(Collectors.toList());

    if (housekeepingEntities.size() <= 0) {
      delete(new BeekeeperEvent(Collections.emptyList(), message));
      return Optional.empty();
    }

    return Optional.of(new BeekeeperEvent(housekeepingEntities, message));
  }

  @Override
  public void delete(BeekeeperEvent beekeeperEvent) {
    try {
      delegate.delete(beekeeperEvent.getMessageEvent());
      log.debug("Message deleted successfully");
    } catch (Exception e) {
      log.error("Could not delete message from queue: ", e);
    }
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
