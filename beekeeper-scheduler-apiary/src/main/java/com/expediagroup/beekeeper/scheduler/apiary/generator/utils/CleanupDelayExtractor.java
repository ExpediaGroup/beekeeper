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
package com.expediagroup.beekeeper.scheduler.apiary.generator.utils;

import static java.lang.String.format;

import java.time.Duration;
import java.time.format.DateTimeParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;

import com.expediagroup.beekeeper.core.error.BeekeeperException;

public class CleanupDelayExtractor {

  private static final Logger log = LoggerFactory.getLogger(CleanupDelayExtractor.class);
  private final String propertyKey;
  private final Duration defaultValue;

  public CleanupDelayExtractor(String propertyKey, String defaultValue) {
    this.propertyKey = propertyKey;
    try {
      this.defaultValue = Duration.parse(defaultValue);
    } catch (DateTimeParseException e) {
      throw new BeekeeperException(
          format("Default delay value '%s' for key '%s' cannot be parsed to a Duration", defaultValue, propertyKey), e);
    }
  }

  public Duration extractCleanupDelay(ListenerEvent listenerEvent) {
    String tableCleanupDelay = listenerEvent.getTableParameters().get(propertyKey);
    try {
      Duration value = tableCleanupDelay == null ? defaultValue : Duration.parse(tableCleanupDelay);
      log.info("Using value '{}' for key {} for table {}.{}.", value, propertyKey, listenerEvent.getDbName(),
              listenerEvent.getTableName());
      return value;
    } catch (DateTimeParseException | NullPointerException e) {
      log.warn(
              "Overridden delay value '{}' for key '{}' cannot be parsed to a Duration for table '{}.{}'. Using default setting {}.",
              tableCleanupDelay, propertyKey, listenerEvent.getDbName(), listenerEvent.getTableName(), defaultValue);
      return defaultValue;
    }
  }
}
