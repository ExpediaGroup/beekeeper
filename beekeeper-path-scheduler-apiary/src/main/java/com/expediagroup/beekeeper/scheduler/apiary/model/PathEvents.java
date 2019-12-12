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
package com.expediagroup.beekeeper.scheduler.apiary.model;

import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;

import com.expediagroup.beekeeper.core.model.HousekeepingPath;

import java.util.List;

public class PathEvents {
  private final List<HousekeepingPath> housekeepingPaths;
  private final MessageEvent messageEvent;

  public PathEvents(List<HousekeepingPath> housekeepingPaths, MessageEvent messageEvent) {
    this.housekeepingPaths = housekeepingPaths;
    this.messageEvent = messageEvent;
  }

  public List<HousekeepingPath> getHousekeepingPaths() {
    return housekeepingPaths;
  }

  public MessageEvent getMessageEvent() {
    return messageEvent;
  }
}
