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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.thrift.TException;

public class HiveTestUtils {

  public HiveTestUtils() {
  }

  private final List<FieldSchema> DATA_COLUMNS = Arrays
      .asList(new FieldSchema("id", "bigint", ""), new FieldSchema("name", "string", ""),
          new FieldSchema("city", "tinyint", ""));

  private final List<FieldSchema> PARTITION_COLUMNS = Arrays
      .asList(new FieldSchema("continent", "string", ""), new FieldSchema("country", "string", ""));

  public Table createUnpartitionedTable(HiveMetaStoreClient metastoreClient, String path) throws TException {
    return createTable(metastoreClient, path, false);
  }

  public Table createPartitionedTable(HiveMetaStoreClient metastoreClient, String path) throws TException {
    return createTable(metastoreClient, path, true);
  }

  private Table createTable(HiveMetaStoreClient metastoreClient, String path, boolean partitioned)
    throws TException {

    Table hiveTable = new Table();
    hiveTable.setDbName(DATABASE_NAME_VALUE);
    hiveTable.setTableName(TABLE_NAME_VALUE);
    hiveTable.setTableType(TableType.EXTERNAL_TABLE.name());
    hiveTable.putToParameters("EXTERNAL", "TRUE");
    if (partitioned) {
      hiveTable.setPartitionKeys(PARTITION_COLUMNS);
    }

    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(DATA_COLUMNS);

    // ******
    sd.setLocation(path);

    sd.setParameters(new HashMap<String, String>());
    sd.setInputFormat(TextInputFormat.class.getName());
    sd.setOutputFormat(TextOutputFormat.class.getName());
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setSerializationLib("org.apache.hadoop.hive.serde2.OpenCSVSerde");

    hiveTable.setSd(sd);
    System.out.println("********** " + metastoreClient);

    System.out
        .println(
            "***** table exists before adding: " + metastoreClient.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE));

    metastoreClient.createTable(hiveTable);

    // ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(true, database, table);
    // ColumnStatisticsData statsData = new ColumnStatisticsData(_Fields.LONG_STATS, new LongColumnStatsData(1L, 2L));
    // ColumnStatisticsObj cso1 = new ColumnStatisticsObj("id", "bigint", statsData);
    // List<ColumnStatisticsObj> statsObj = Collections.singletonList(cso1);
    // metastoreClient.updateTableColumnStatistics(new ColumnStatistics(statsDesc, statsObj));

    return hiveTable;
  }

}
