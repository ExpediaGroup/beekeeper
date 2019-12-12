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
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageReader;

import com.expediagroup.beekeeper.scheduler.apiary.model.PathEvents;

public class MessageReaderAdapter implements PathEventReader {

  private static final Logger log = LoggerFactory.getLogger(MessageReaderAdapter.class);

  private final MessageReader delegate;
  private final MessageEventToPathEventMapper mapper;

  public MessageReaderAdapter(MessageReader delegate, MessageEventToPathEventMapper mapper) {
    this.delegate = delegate;
    this.mapper = mapper;
  }

  @Override
  public Optional<PathEvents> read() {
    Optional<MessageEvent> messageEvent = delegate.read();
    return messageEvent.flatMap(mapper::map);
  }

  @Override
  public void delete(PathEvents pathEvent) {
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
