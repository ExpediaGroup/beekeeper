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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

public class SqsMessageTest {

  private static final Logger log = LoggerFactory.getLogger(SqsMessageTest.class);

  private static final String DUMMY_VAL = "s3://location";
  private static final Set<String> COMMON_KEYS = Set.of("protocolVersion", "eventType", "tableParameters",
      "dbName", "tableName", "tableLocation");

  @Test
  public void testCreateTableFormat() throws IOException, URISyntaxException {
    Set<String> specificKeys = new HashSet<>();
    CreateTableSqsMessage message = new CreateTableSqsMessage(DUMMY_VAL, true);
    assertKeys(message, specificKeys, "CREATE_TABLE");
  }

  @Test
  public void testAddPartitionFormat() throws IOException, URISyntaxException {
    Set<String> specificKeys = Set.of(
        "partitionKeys",
        "partitionValues",
        "partitionLocation",
        "tableParameters");
    AddPartitionSqsMessage message = new AddPartitionSqsMessage(DUMMY_VAL, DUMMY_VAL, DUMMY_VAL, true);
    assertKeys(message, specificKeys, "ADD_PARTITION");
  }

  @Test
  public void testAlterPartitionFormat() throws IOException, URISyntaxException {
    Set<String> specificKeys = Set.of(
        "partitionKeys",
        "partitionValues",
        "partitionLocation",
        "oldPartitionValues",
        "oldPartitionLocation");
    AlterPartitionSqsMessage message = new AlterPartitionSqsMessage(DUMMY_VAL, DUMMY_VAL, DUMMY_VAL, true, true);
    assertKeys(message, specificKeys, "ALTER_PARTITION");
  }

  @Test
  public void testAlterTableFormat() throws IOException, URISyntaxException {
    Set<String> specificKeys = Set.of(
        "oldTableName",
        "oldTableLocation");
    AlterTableSqsMessage message = new AlterTableSqsMessage(DUMMY_VAL, DUMMY_VAL, true, true);
    assertKeys(message, specificKeys, "ALTER_TABLE");
  }

  @Test
  public void testDropPartitionFormat() throws IOException, URISyntaxException {
    Set<String> specificKeys = Set.of(
        "partitionKeys",
        "partitionValues",
        "partitionLocation",
        "tableParameters");
    DropPartitionSqsMessage message = new DropPartitionSqsMessage(DUMMY_VAL, true, true);
    assertKeys(message, specificKeys, "DROP_PARTITION");
  }

  @Test
  public void testDropTableFormat() throws IOException, URISyntaxException {
    Set<String> specificKeys = new HashSet<>();
    DropTableSqsMessage message = new DropTableSqsMessage(DUMMY_VAL, true, true);
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

    System.out.println(sqsMessage.getFormattedString());
  }
}
