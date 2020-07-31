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

  public Table createUnpartitionedTable(HiveMetaStoreClient metastoreClient, String location) throws TException {
    return createTable(metastoreClient, location, false);
  }

  public Table createPartitionedTable(HiveMetaStoreClient metastoreClient, String location) throws TException {
    return createTable(metastoreClient, location, true);
  }

  private Table createTable(HiveMetaStoreClient metastoreClient, String location, boolean partitioned)
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
    sd.setLocation(location);

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
