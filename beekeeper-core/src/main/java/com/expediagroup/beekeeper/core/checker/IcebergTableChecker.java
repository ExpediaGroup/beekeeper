package com.expediagroup.beekeeper.core.checker;

import java.util.Map;
import java.util.function.Supplier;

import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expediagroup.beekeeper.core.error.BeekeeperException;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

public class IcebergTableChecker {

  private static final Logger log = LoggerFactory.getLogger(IcebergTableChecker.class);

  private final Supplier<CloseableMetaStoreClient> metaStoreClientSupplier;

  public IcebergTableChecker(Supplier<CloseableMetaStoreClient> metaStoreClientSupplier) {
    this.metaStoreClientSupplier = metaStoreClientSupplier;
  }

  public void throwExceptionIfIceberg(String databaseName, String tableName) {
    try (CloseableMetaStoreClient client = metaStoreClientSupplier.get()) {
      Table table = client.getTable(databaseName, tableName);

      Map<String, String> parameters = table.getParameters();
      String tableType = parameters.getOrDefault("table_type", "").toLowerCase();
      String format = parameters.getOrDefault("format", "").toLowerCase();
      String outputFormat = table.getSd().getOutputFormat().toLowerCase();

      if (tableType.contains("iceberg") || format.contains("iceberg") || outputFormat.contains("iceberg")) {
        throw new BeekeeperException("Iceberg tables are not currently supported in Beekeeper");
      }
    } catch (Exception e) {
      throw new BeekeeperException("Error checking if table is Iceberg", e);
    }
  }
}
