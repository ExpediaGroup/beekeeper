/**
 * Copyright (C) 2019-2024 Expedia, Inc.
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

import static org.assertj.core.api.Assertions.assertThat;

import static com.expediagroup.beekeeper.integration.model.SqsMessage.DUMMY_LOCATION;
import static com.expediagroup.beekeeper.integration.model.SqsMessage.DUMMY_PARTITION_KEYS;
import static com.expediagroup.beekeeper.integration.model.SqsMessage.DUMMY_PARTITION_VALUES;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;

public class SqsMessageTest {

  private static final Set<String> COMMON_KEYS = Set.of(
      "protocolVersion",
      "eventType",
      "tableParameters",
      "dbName",
      "tableName",
      "tableLocation"
  );

  @Test
  public void testCreateTableFormat() throws IOException, URISyntaxException {
    Set<String> specificKeys = new HashSet<>();
    CreateTableSqsMessage message = new CreateTableSqsMessage(DUMMY_LOCATION, true);
    assertKeys(message, specificKeys, "CREATE_TABLE");
  }

  @Test
  public void testAddPartitionFormat() throws IOException, URISyntaxException {
    Set<String> specificKeys = Set.of(
        "partitionKeys",
        "partitionValues",
        "partitionLocation",
        "tableParameters"
    );
    AddPartitionSqsMessage message = new AddPartitionSqsMessage(
        DUMMY_LOCATION,
        DUMMY_PARTITION_KEYS,
        DUMMY_PARTITION_VALUES,
        true
    );
    assertKeys(message, specificKeys, "ADD_PARTITION");
  }

  @Test
  public void testAlterPartitionFormat() throws IOException, URISyntaxException {
    Set<String> specificKeys = Set.of(
        "partitionKeys",
        "partitionValues",
        "partitionLocation",
        "oldPartitionValues",
        "oldPartitionLocation"
    );
    AlterPartitionSqsMessage message = new AlterPartitionSqsMessage(
        DUMMY_LOCATION,
        DUMMY_PARTITION_KEYS,
        DUMMY_PARTITION_VALUES,
        true
    );
    assertKeys(message, specificKeys, "ALTER_PARTITION");
  }

  @Test
  public void testAlterTableFormat() throws IOException, URISyntaxException {
    Set<String> specificKeys = Set.of(
        "oldTableName",
        "oldTableLocation"
    );
    AlterTableSqsMessage message = new AlterTableSqsMessage(DUMMY_LOCATION, true);
    assertKeys(message, specificKeys, "ALTER_TABLE");
  }

  @Test
  public void testDropPartitionFormat() throws IOException, URISyntaxException {
    Set<String> specificKeys = Set.of(
        "partitionKeys",
        "partitionValues",
        "partitionLocation",
        "tableParameters"
    );
    DropPartitionSqsMessage message = new DropPartitionSqsMessage(DUMMY_LOCATION, true, true);
    assertKeys(message, specificKeys, "DROP_PARTITION");
  }

  @Test
  public void testDropTableFormat() throws IOException, URISyntaxException {
    Set<String> specificKeys = new HashSet<>();
    DropTableSqsMessage message = new DropTableSqsMessage(DUMMY_LOCATION, true, true);
    assertKeys(message, specificKeys, "DROP_TABLE");
  }

  private void assertKeys(SqsMessage sqsMessage, Set<String> specificKeys, String eventType) {
    SortedSet<String> mergedSet = new TreeSet<>() {{
      addAll(specificKeys);
      addAll(COMMON_KEYS);
    }};

    JsonObject object = sqsMessage.getApiaryEventMessageJsonObject();

    assertThat(object.get("eventType").getAsString()).isEqualTo(eventType);
    assertThat(object.keySet()).isEqualTo(mergedSet);
  }

  @Test
  public void testSetTableType() throws IOException, URISyntaxException {
    CreateTableSqsMessage message = new CreateTableSqsMessage(DUMMY_LOCATION, true);

    message.setTableType("ICEBERG");

    JsonObject object = message.getApiaryEventMessageJsonObject();
    JsonObject tableParameters = object.getAsJsonObject("tableParameters");

    assertThat(tableParameters.get("table_type").getAsString()).isEqualTo("ICEBERG");
  }

  //test method to verify setOutputFormat functionality
  @Test
  public void testSetOutputFormat() throws IOException, URISyntaxException {
    CreateTableSqsMessage message = new CreateTableSqsMessage(DUMMY_LOCATION, true);

    String outputFormatValue = "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat";
    message.setOutputFormat(outputFormatValue);

    JsonObject object = message.getApiaryEventMessageJsonObject();
    JsonObject tableParameters = object.getAsJsonObject("tableParameters");

    assertThat(tableParameters.get("output_format").getAsString()).isEqualTo(outputFormatValue);
  }
}
