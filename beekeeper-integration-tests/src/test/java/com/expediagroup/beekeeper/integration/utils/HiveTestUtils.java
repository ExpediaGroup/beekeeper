/**
 * Copyright (C) 2019-2023 Expedia, Inc.
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
package com.expediagroup.beekeeper.integration.utils;

import static com.expediagroup.beekeeper.integration.CommonTestVariables.DATABASE_NAME_VALUE;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.thrift.TException;

import com.expediagroup.beekeeper.core.model.LifecycleEventType;

public class HiveTestUtils {

  private HiveMetaStoreClient metastoreClient;

  public HiveTestUtils(HiveMetaStoreClient metastoreClient) {
    this.metastoreClient = metastoreClient;
  }

  private final List<FieldSchema> DATA_COLUMNS = Arrays
      .asList(new FieldSchema("id", "bigint", ""), new FieldSchema("name", "string", ""),
          new FieldSchema("city", "tinyint", ""));

  private final List<FieldSchema> PARTITION_COLUMNS = Arrays
      .asList(new FieldSchema("event_date", "string", ""), new FieldSchema("event_hour", "string", ""),
          new FieldSchema("event_type", "string", ""));

  public Table createTable(String path, String tableName, boolean partitioned) throws TException {
    return createTable(path, tableName, partitioned, true);
  }

  public Table createTable(String path, String tableName, boolean partitioned, boolean withBeekeeperProperty)
      throws TException {
    Table hiveTable = new Table();
    hiveTable.setDbName(DATABASE_NAME_VALUE);
    hiveTable.setTableName(tableName);
    hiveTable.setTableType(TableType.EXTERNAL_TABLE.name());
    hiveTable.putToParameters("EXTERNAL", "TRUE");
    if (withBeekeeperProperty) {
      hiveTable.putToParameters(LifecycleEventType.EXPIRED.getTableParameterName(), "true");
    }
    if (partitioned) {
      hiveTable.setPartitionKeys(PARTITION_COLUMNS);
    }
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(DATA_COLUMNS);
    sd.setLocation(path);
    sd.setParameters(new HashMap<>());
    sd.setInputFormat(TextInputFormat.class.getName());
    sd.setOutputFormat(TextOutputFormat.class.getName());
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setSerializationLib("org.apache.hadoop.hive.serde2.OpenCSVSerde");
    hiveTable.setSd(sd);
    metastoreClient.createTable(hiveTable);

    return hiveTable;
  }

  /**
   * @param path Path of the table
   * @param hiveTable Table to add partitions to
   * @param partitionValues The list of partition values, e.g. ["2020-01-01", "0", "A"]
   * @throws Exception May be thrown if there is a problem when trying to write the data to the file, or when the client
   *                   adds the partition to the table.
   */
  public void addPartitionsToTable(String path, Table hiveTable, List<String> partitionValues) throws Exception {
    String eventDate = "/event_date=" + partitionValues.get(0); // 2020-01-01
    String eventHour = eventDate + "/event_hour=" + partitionValues.get(1); // 0
    String eventType = eventHour + "/event_type=" + partitionValues.get(2); // A
    URI partitionUri = URI.create(path + eventType);

    metastoreClient
        .add_partitions(Collections
            .singletonList(newTablePartition(hiveTable,
                List.of(partitionValues.get(0), partitionValues.get(1), partitionValues.get(2)), partitionUri)));
  }

  private Partition newTablePartition(Table hiveTable, List<String> values, URI location) {
    Partition partition = new Partition();
    partition.setDbName(hiveTable.getDbName());
    partition.setTableName(hiveTable.getTableName());
    partition.setValues(values);
    partition.setSd(new StorageDescriptor(hiveTable.getSd()));
    partition.getSd().setLocation(location.toString());
    return partition;
  }

  public Table createTableWithProperties(String path, String tableName, boolean partitioned,
      Map<String, String> tableProperties, boolean withBeekeeperProperty)
      throws TException {
    Table hiveTable = new Table();
    hiveTable.setDbName(DATABASE_NAME_VALUE);
    hiveTable.setTableName(tableName);
    hiveTable.setTableType(TableType.EXTERNAL_TABLE.name());
    hiveTable.putToParameters("EXTERNAL", "TRUE");

    if (tableProperties != null) {
      hiveTable.getParameters().putAll(tableProperties);
    }
    if (withBeekeeperProperty) {
      hiveTable.putToParameters(LifecycleEventType.EXPIRED.getTableParameterName(), "true");
    }
    if (partitioned) {
      hiveTable.setPartitionKeys(PARTITION_COLUMNS);
    }
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(DATA_COLUMNS);
    sd.setLocation(path);
    sd.setParameters(new HashMap<>());
    sd.setOutputFormat(TextOutputFormat.class.getName());
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setSerializationLib("org.apache.hadoop.hive.serde2.OpenCSVSerde");
    hiveTable.setSd(sd);
    metastoreClient.createTable(hiveTable);

    return hiveTable;
  }
}
