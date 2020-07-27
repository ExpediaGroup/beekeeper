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

import static com.expedia.apiary.extensions.receiver.common.event.EventType.ALTER_PARTITION;

import java.io.IOException;
import java.net.URISyntaxException;

import com.google.gson.JsonPrimitive;

public class AlterPartitionSqsMessage extends SqsMessage {

  public AlterPartitionSqsMessage(
      String tableLocation,
      String partitionLocation,
      String oldPartitionLocation,
      boolean isUnreferenced,
      boolean isWhitelisted
  ) throws IOException, URISyntaxException {
    super(ALTER_PARTITION);
    setTableLocation(tableLocation);
    setPartitionLocation(partitionLocation);
    setOldPartitionLocation(oldPartitionLocation);
    setPartitionKeys(DUMMY_PARTITION_KEYS);
    setPartitionValues(DUMMY_PARTITION_VALUES);
    setOldPartitionValues(DUMMY_PARTITION_VALUES);
    setUnreferenced(isUnreferenced);
    setWhitelisted(isWhitelisted);
  }

  public AlterPartitionSqsMessage(
      String partitionLocation,
      String partitionKeys,
      String partitionValues,
      boolean isExpired
  ) throws IOException, URISyntaxException {
    super(ALTER_PARTITION);
    setTableLocation(DUMMY_LOCATION);
    setPartitionLocation(partitionLocation);
    setOldPartitionLocation(DUMMY_LOCATION);
    setPartitionKeys(partitionKeys);
    setPartitionValues(partitionValues);
    setExpired(isExpired);
  }

  public void setPartitionLocation(String partitionLocation) {
    apiaryEventMessageJsonObject.add(EVENT_TABLE_PARTITION_LOCATION_KEY, new JsonPrimitive(partitionLocation));
  }

  public void setOldPartitionLocation(String oldPartitionLocation) {
    apiaryEventMessageJsonObject.add(EVENT_TABLE_OLD_PARTITION_LOCATION_KEY, new JsonPrimitive(oldPartitionLocation));
  }

  public void setPartitionKeys(String partitionKeys) {
    apiaryEventMessageJsonObject.add(EVENT_TABLE_PARTITION_KEYS_KEY, PARSER.parse(partitionKeys).getAsJsonObject());
  }

  public void setPartitionValues(String partitionValues) {
    apiaryEventMessageJsonObject.add(EVENT_TABLE_PARTITION_VALUES_KEY, PARSER.parse(partitionValues).getAsJsonArray());
  }

  public void setOldPartitionValues(String oldPartitionValues) {
    apiaryEventMessageJsonObject.add(EVENT_TABLE_OLD_PARTITION_VALUES_KEY, PARSER.parse(oldPartitionValues).getAsJsonArray());
  }
}
