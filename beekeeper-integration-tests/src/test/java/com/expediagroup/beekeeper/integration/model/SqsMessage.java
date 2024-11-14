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

import static java.nio.file.Files.readString;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.DATABASE_NAME_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.SHORT_CLEANUP_DELAY_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.TABLE_NAME_VALUE;
import static com.expediagroup.beekeeper.scheduler.apiary.filter.WhitelistedListenerEventFilter.BEEKEEPER_HIVE_EVENT_WHITELIST;
import static com.expediagroup.beekeeper.scheduler.apiary.generator.ExpiredHousekeepingMetadataGenerator.EXPIRED_DATA_RETENTION_PERIOD_PROPERTY_KEY;
import static com.expediagroup.beekeeper.scheduler.apiary.generator.UnreferencedHousekeepingPathGenerator.UNREFERENCED_DATA_RETENTION_PERIOD_PROPERTY_KEY;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import com.expedia.apiary.extensions.receiver.common.event.EventType;

public abstract class SqsMessage {

  private static final URL APIARY_EVENT_JSON_URL = SqsMessage.class.getResource("/message.json");
  protected static final JsonParser PARSER = new JsonParser();

  private static final String APIARY_EVENT_MESSAGE_KEY = "Message";
  public static final String EVENT_PROTOCOL_VERSION_KEY = "protocolVersion";
  public static final String EVENT_TYPE_KEY = "eventType";
  public static final String EVENT_DB_KEY = "dbName";
  public static final String EVENT_TABLE_NAME_KEY = "tableName";
  public static final String EVENT_TABLE_OLD_NAME_KEY = "oldTableName";
  public static final String EVENT_TABLE_PARAMETERS_KEY = "tableParameters";
  public static final String EVENT_TABLE_LOCATION_KEY = "tableLocation";
  public static final String EVENT_TABLE_OLD_LOCATION_KEY = "oldTableLocation";
  public static final String EVENT_TABLE_PARTITION_LOCATION_KEY = "partitionLocation";
  public static final String EVENT_TABLE_OLD_PARTITION_LOCATION_KEY = "oldPartitionLocation";
  public static final String EVENT_TABLE_PARTITION_KEYS_KEY = "partitionKeys";
  public static final String EVENT_TABLE_PARTITION_VALUES_KEY = "partitionValues";
  public static final String EVENT_TABLE_OLD_PARTITION_VALUES_KEY = "oldPartitionValues";

  public static final String DUMMY_LOCATION = "s3://dummy-location";
  public static final String DUMMY_PARTITION_KEYS = "{ \"col_1\": \"string\", \"col_2\": \"integer\", \"col_3\": \"string\" }";
  public static final String DUMMY_PARTITION_VALUES = "[ \"val_1\", \"val_2\", \"val_3\" ]";
  public static final String EVENT_PROTOCOL_VERSION_VALUE = "1.0";

  private EventType eventType;
  private JsonObject apiaryEventJsonObject;
  protected JsonObject apiaryEventMessageJsonObject;

  public SqsMessage(EventType eventType) throws URISyntaxException, IOException {
    this.eventType = eventType;
    apiaryEventJsonObject = PARSER.parse(readString(Path.of(APIARY_EVENT_JSON_URL.toURI()))).getAsJsonObject();
    apiaryEventMessageJsonObject = new JsonObject();
    apiaryEventMessageJsonObject.add(EVENT_PROTOCOL_VERSION_KEY, new JsonPrimitive(EVENT_PROTOCOL_VERSION_VALUE));
    apiaryEventMessageJsonObject.add(EVENT_TYPE_KEY, new JsonPrimitive(eventType.toString()));
    apiaryEventMessageJsonObject.add(EVENT_DB_KEY, new JsonPrimitive(DATABASE_NAME_VALUE));
    apiaryEventMessageJsonObject.add(EVENT_TABLE_NAME_KEY, new JsonPrimitive(TABLE_NAME_VALUE));
    apiaryEventMessageJsonObject.add(EVENT_TABLE_PARAMETERS_KEY, new JsonObject());
  }

  public void setTableLocation(String location) {
    apiaryEventMessageJsonObject.add(EVENT_TABLE_LOCATION_KEY, new JsonPrimitive(location));
  }

  public void setUnreferenced(boolean isUnreferenced) {
    JsonObject tableParameters = apiaryEventMessageJsonObject.getAsJsonObject(EVENT_TABLE_PARAMETERS_KEY);
    tableParameters.add(UNREFERENCED.getTableParameterName(), new JsonPrimitive(String.valueOf(isUnreferenced)));
    tableParameters.add(UNREFERENCED_DATA_RETENTION_PERIOD_PROPERTY_KEY, new JsonPrimitive(SHORT_CLEANUP_DELAY_VALUE));
  }

  public void setExpired(boolean isExpired) {
    JsonObject tableParameters = apiaryEventMessageJsonObject.getAsJsonObject(EVENT_TABLE_PARAMETERS_KEY);
    tableParameters.add(EXPIRED.getTableParameterName(), new JsonPrimitive(String.valueOf(isExpired)));
    tableParameters.add(EXPIRED_DATA_RETENTION_PERIOD_PROPERTY_KEY, new JsonPrimitive(SHORT_CLEANUP_DELAY_VALUE));
  }

  public void setWhitelisted(boolean isWhitelisted) {
    String whitelist = isWhitelisted ? eventType.toString() : "";
    JsonObject tableParameters = apiaryEventMessageJsonObject.getAsJsonObject(EVENT_TABLE_PARAMETERS_KEY);
    tableParameters.add(BEEKEEPER_HIVE_EVENT_WHITELIST, new JsonPrimitive(whitelist));
  }

  public final String getFormattedString() {
    apiaryEventJsonObject.add(APIARY_EVENT_MESSAGE_KEY, new JsonPrimitive(apiaryEventMessageJsonObject.toString()));
    return apiaryEventJsonObject.toString();
  }

  public JsonObject getApiaryEventMessageJsonObject() {
    return apiaryEventMessageJsonObject;
  }

  // adds a key value pair to the `tableParameters` object in the `apiaryEventMessageJsonObject`
  public void addTableParameter(String key, String value) {
    JsonObject tableParameters = apiaryEventMessageJsonObject.getAsJsonObject(EVENT_TABLE_PARAMETERS_KEY);
    tableParameters.add(key, new JsonPrimitive(value));
  }

}
