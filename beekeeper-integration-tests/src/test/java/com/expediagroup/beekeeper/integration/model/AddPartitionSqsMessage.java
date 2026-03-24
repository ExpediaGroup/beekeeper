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

import static com.expedia.apiary.extensions.receiver.common.event.EventType.ADD_PARTITION;

import java.io.IOException;
import java.net.URISyntaxException;

import com.google.gson.JsonPrimitive;

public class AddPartitionSqsMessage extends SqsMessage {

  public AddPartitionSqsMessage(
      String partitionLocation,
      String partitionKeys,
      String partitionValues,
      boolean isExpired
  ) throws IOException, URISyntaxException {
    super(ADD_PARTITION);
    setTableLocation(DUMMY_LOCATION);
    setPartitionLocation(partitionLocation);
    setPartitionKeys(partitionKeys);
    setPartitionValues(partitionValues);
    setExpired(isExpired);
  }

  private void setPartitionLocation(String partitionLocation) {
    apiaryEventMessageJsonObject.add(EVENT_TABLE_PARTITION_LOCATION_KEY, new JsonPrimitive(partitionLocation));
  }

  private void setPartitionKeys(String partitionKeys) {
    apiaryEventMessageJsonObject.add(EVENT_TABLE_PARTITION_KEYS_KEY, PARSER.parse(partitionKeys).getAsJsonObject());
  }

  private void setPartitionValues(String partitionValues) {
    apiaryEventMessageJsonObject.add(EVENT_TABLE_PARTITION_VALUES_KEY, PARSER.parse(partitionValues).getAsJsonArray());
  }
}
