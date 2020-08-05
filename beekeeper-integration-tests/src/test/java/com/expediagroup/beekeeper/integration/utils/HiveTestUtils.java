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
package com.expediagroup.beekeeper.integration.utils;

import static com.expediagroup.beekeeper.integration.CommonTestVariables.DATABASE_NAME_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.TABLE_NAME_VALUE;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

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

public class HiveTestUtils {

  public HiveTestUtils() {}

  private final List<FieldSchema> DATA_COLUMNS = Arrays
      .asList(new FieldSchema("id", "bigint", ""), new FieldSchema("name", "string", ""),
          new FieldSchema("city", "tinyint", ""));

  private final List<FieldSchema> PARTITION_COLUMNS = Arrays
      .asList(new FieldSchema("event_date", "string", ""), new FieldSchema("event_hour", "string", ""),
          new FieldSchema("event_type", "string", ""));

  public static final String PART_00000 = "part-00000";

  public Table createTable(HiveMetaStoreClient metastoreClient, String path, String tableName, boolean partitioned)
    throws TException {
    Table hiveTable = new Table();
    hiveTable.setDbName(DATABASE_NAME_VALUE);
    hiveTable.setTableName(tableName);
    hiveTable.setTableType(TableType.EXTERNAL_TABLE.name());
    hiveTable.putToParameters("EXTERNAL", "TRUE");
    if (partitioned) {
      hiveTable.setPartitionKeys(PARTITION_COLUMNS);
    }
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(DATA_COLUMNS);
    sd.setLocation(path);
    sd.setParameters(new HashMap<String, String>());
    sd.setInputFormat(TextInputFormat.class.getName());
    sd.setOutputFormat(TextOutputFormat.class.getName());
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setSerializationLib("org.apache.hadoop.hive.serde2.OpenCSVSerde");
    hiveTable.setSd(sd);
    metastoreClient.createTable(hiveTable);

    return hiveTable;
  }

  /**
   * @param metastoreClient
   * @param path of the table
   * @param hiveTable
   * @param partitionValues The list of partition values, e.g. ["2020-01-01", "0", "A"]
   * @param dataEntry The data to add to the table, e.g. "1\tadam\tlondon\n2\tsusan\tglasgow\n"
   * @throws Exception May be thrown if there is a problem when trying to write the data to the file, or when the client
   *           adds the partition to the table.
   */
  public void addPartitionsToTable(
      HiveMetaStoreClient metastoreClient,
      String path,
      Table hiveTable,
      List<String> partitionValues,
      String dataEntry)
    throws Exception {
    String eventDate = "/event_date=" + partitionValues.get(0); // 2020-01-01
    String eventHour = eventDate + "/event_hour=" + partitionValues.get(1); // 0
    String eventType = eventHour + "/event_type=" + partitionValues.get(2); // A
    URI partitionUri = URI.create(path + eventType);
    
    metastoreClient
        .add_partitions(
            Collections
                .singletonList(newTablePartition(hiveTable,
                    List.of(partitionValues.get(0), partitionValues.get(1), partitionValues.get(2)),
                    partitionUri)));
  }

  public static URI toUri(String baseLocation) {
    return URI.create(String.format("%s/%s", DATABASE_NAME_VALUE, TABLE_NAME_VALUE));
  }

  public static Partition newTablePartition(Table hiveTable, List<String> values, URI location) {
    Partition partition = new Partition();
    partition.setDbName(hiveTable.getDbName());
    partition.setTableName(hiveTable.getTableName());
    partition.setValues(values);
    partition.setSd(new StorageDescriptor(hiveTable.getSd()));
    partition.getSd().setLocation(location.toString());
    return partition;
  }

}
