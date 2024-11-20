package com.expediagroup.beekeeper.core.config;

import org.apache.hadoop.hive.conf.HiveConf;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.expediagroup.beekeeper.core.checker.IcebergTableChecker;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.closeable.CloseableMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.supplier.HiveMetaStoreClientSupplier;

import java.util.function.Supplier;

@Configuration
public class CoreBeans {

  @Bean
  public HiveConf hiveConf(@Value("${properties.metastore-uri}") String metastoreUri) {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUri);
    return hiveConf;
  }

  @Bean
  public CloseableMetaStoreClientFactory metaStoreClientFactory() {
    return new CloseableMetaStoreClientFactory();
  }

  @Bean
  public Supplier<CloseableMetaStoreClient> metaStoreClientSupplier(
      CloseableMetaStoreClientFactory metaStoreClientFactory,
      HiveConf hiveConf) {
    String name = "beekeeper-core";
    return new HiveMetaStoreClientSupplier(metaStoreClientFactory, hiveConf, name);
  }

  @Bean
  public IcebergTableChecker icebergTableChecker(Supplier<CloseableMetaStoreClient> metaStoreClientSupplier) {
    return new IcebergTableChecker(metaStoreClientSupplier);
  }
}
