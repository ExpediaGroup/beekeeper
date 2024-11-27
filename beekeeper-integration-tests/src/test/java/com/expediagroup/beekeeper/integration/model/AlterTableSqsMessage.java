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
package com.expediagroup.beekeeper.integration.model;

import static com.expedia.apiary.extensions.receiver.common.event.EventType.ALTER_TABLE;

import static com.expediagroup.beekeeper.integration.CommonTestVariables.TABLE_NAME_VALUE;

import java.io.IOException;
import java.net.URISyntaxException;

import com.google.gson.JsonPrimitive;

public class AlterTableSqsMessage extends SqsMessage {

  public AlterTableSqsMessage(
      String tableLocation,
      String oldTableLocation,
      boolean isUnreferenced,
      boolean isWhitelisted
  ) throws IOException, URISyntaxException {
    super(ALTER_TABLE);
    setTableLocation(tableLocation);
    setOldTableLocation(oldTableLocation);
    setOldTableName(TABLE_NAME_VALUE);
    setUnreferenced(isUnreferenced);
    setWhitelisted(isWhitelisted);
  }

  public AlterTableSqsMessage(
      String tableLocation,
      boolean isExpired
  ) throws IOException, URISyntaxException {
    super(ALTER_TABLE);
    setTableLocation(tableLocation);
    setOldTableLocation(DUMMY_LOCATION);
    setOldTableName(TABLE_NAME_VALUE);
    setExpired(isExpired);
  }

  public AlterTableSqsMessage(
      String tableLocation,
      boolean isExpired,
      boolean isIceberg
  ) throws IOException, URISyntaxException {
    super(ALTER_TABLE);
    setTableLocation(tableLocation);
    setOldTableLocation(DUMMY_LOCATION);
    setOldTableName(TABLE_NAME_VALUE);
    setExpired(isExpired);
    if (isIceberg) {
      setIceberg();
    }
  }

  public void setOldTableLocation(String oldTableLocation) {
    apiaryEventMessageJsonObject.add(EVENT_TABLE_OLD_LOCATION_KEY, new JsonPrimitive(oldTableLocation));
  }

  public void setOldTableName(String oldTableName) {
    apiaryEventMessageJsonObject.add(EVENT_TABLE_OLD_NAME_KEY, new JsonPrimitive(oldTableName));
  }
}
