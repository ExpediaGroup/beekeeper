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
import java.util.stream.Collectors;

import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.scheduler.apiary.mapper.MessageEventMapper;
import com.expediagroup.beekeeper.scheduler.apiary.model.BeekeeperEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageReader;

public class MessageReaderAdapter implements BeekeeperEventReader {

  private static final Logger log = LoggerFactory.getLogger(MessageReaderAdapter.class);

  private final MessageReader delegate;
  private final List<MessageEventMapper> eventMappers;

  public MessageReaderAdapter(MessageReader delegate, List<MessageEventMapper> eventMappers) {
    this.delegate = delegate;
    this.eventMappers = eventMappers;
  }

  @Override
  public Optional<BeekeeperEvent> read() {
    Optional<MessageEvent> messageEvent = delegate.read();

    if (!messageEvent.isPresent()) {
      return Optional.empty();
    }

    MessageEvent event = messageEvent.get();
    List<HousekeepingPath> housekeepingPaths = eventMappers.parallelStream()
            .map(eventMapper -> eventMapper.generateHouseKeepingPaths(event.getEvent()))
            .flatMap(x -> x.stream())
            .collect(Collectors.toList());

    BeekeeperEvent beekeeperEvent = housekeepingPaths.size() > 0 ? new BeekeeperEvent(housekeepingPaths, event) : null;
    return Optional.ofNullable(beekeeperEvent);
  }

  @Override
  public void delete(BeekeeperEvent pathEvent) {
    try {
      delegate.delete(pathEvent.getMessageEvent());
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
