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
package com.expediagroup.beekeeper.scheduler.service.predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Test;

class IsIcebergTablePredicateTest {

  private IsIcebergTablePredicate predicate = new IsIcebergTablePredicate();

  @Test
  void testIsIcebergTableByTableType() {
    Table table = new Table();
    Map<String, String> tableParameters = new HashMap<>();
    tableParameters.put("table_type", "ICEBERG");
    table.setParameters(tableParameters);

    assertThat(predicate.test(table)).isTrue();
  }

  @Test
  void testIsIcebergTableByOutputFormat() {
    Table table = new Table();
    StorageDescriptor sd = new StorageDescriptor();
    sd.setOutputFormat("org.apache.iceberg.mr.hive.HiveIcebergOutputFormat");
    table.setSd(sd);

    assertThat(predicate.test(table)).isTrue();
  }

  @Test
  void testIsIcebergTableByBoth() {
    Table table = new Table();
    Map<String, String> tableParameters = new HashMap<>();
    tableParameters.put("table_type", "ICEBERG");
    table.setParameters(tableParameters);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setOutputFormat("org.apache.iceberg.mr.hive.HiveIcebergOutputFormat");
    table.setSd(sd);

    assertThat(predicate.test(table)).isTrue();
  }

  @Test
  void testIsNotIcebergTableWithDifferentTableTypeAndOutputFormat() {
    Table table = new Table();
    Map<String, String> tableParameters = new HashMap<>();
    tableParameters.put("table_type", "NICEBERG");
    table.setParameters(tableParameters);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setOutputFormat("org.apache.not.an.ice.berg.table");
    table.setSd(sd);

    assertThat(predicate.test(table)).isFalse();
  }

  @Test
  void testIsNotIcebergTableWithWrongParameter() {
    Table table = new Table();
    table.setParameters(Collections.singletonMap("table_type", "NICEBERG"));

    assertThat(predicate.test(table)).isFalse();
  }

  @Test
  void testIsNotIcebergTableWithWrongStorageDescriptor() {
    Table table = new Table();
    StorageDescriptor sd = new StorageDescriptor();
    sd.setOutputFormat("org.apache.not.an.ice.berg.table");
    table.setSd(sd);

    assertThat(predicate.test(table)).isFalse();
  }

  @Test
  void testIsNotIcebergTableWithNoParametersOrSd() {
    Table table = new Table();

    assertThat(predicate.test(table)).isFalse();
  }

  @Test
  void testIsIcebergTableWithStorageDescriptorButDifferentTableType() {
    Table table = new Table();
    StorageDescriptor sd = new StorageDescriptor();
    sd.setOutputFormat("org.apache.iceberg.mr.hive.HiveIcebergOutputFormat");
    table.setSd(sd);
    table.setParameters(Collections.singletonMap("table_type", "NICEBERG"));

    assertThat(predicate.test(table)).isTrue();
  }

  @Test
  void testIsIcebergTableWithTableTypeButDifferentSd() {
    Table table = new Table();
    StorageDescriptor sd = new StorageDescriptor();
    sd.setOutputFormat("org.apache.not.an.ice.berg.table");
    table.setSd(sd);
    table.setParameters(Collections.singletonMap("table_type", "ICEBERG"));

    assertThat(predicate.test(table)).isTrue();
  }

  @Test
  void testNullTableThrowsException() {
    assertThrows(NullPointerException.class, () -> predicate.test(null));
  }
}
