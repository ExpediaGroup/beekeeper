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
package com.expediagroup.beekeeper.vacuum;

import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;
import com.google.common.base.Supplier;

import com.expediagroup.beekeeper.scheduler.service.SchedulerService;
import com.expediagroup.beekeeper.scheduler.service.UnreferencedHousekeepingPathSchedulerService;
import com.expediagroup.beekeeper.vacuum.repository.BeekeeperRepository;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.closeable.CloseableMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.supplier.HiveMetaStoreClientSupplier;

@Configuration
@EntityScan(basePackages = { "com.expediagroup.beekeeper.core.model" })
@EnableJpaRepositories(basePackages = { "com.expediagroup.beekeeper.core.repository",
                                        "com.expediagroup.beekeeper.vacuum.repository" })
public class CommonBeans {

  @Bean
  public EC2ContainerCredentialsProviderWrapper ec2ContainerCredentialsProviderWrapper() {
    return new EC2ContainerCredentialsProviderWrapper();
  }

  @Bean
  public HiveConf hiveConf(@Value("${metastore-uri}") String metastoreUri) {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUri);

    // set AWS authentication methods
    conf.set("fs.s3.impl", S3AFileSystem.class.getName());
    conf.set("fs.s3a.impl", S3AFileSystem.class.getName());
    conf.set("fs.s3n.impl", S3AFileSystem.class.getName());
    conf.set("fs.s3.aws.credentials.provider", EC2ContainerCredentialsProviderWrapper.class.getName());
    conf.set("fs.s3a.aws.credentials.provider", EC2ContainerCredentialsProviderWrapper.class.getName());
    conf.set("fs.s3n.aws.credentials.provider", EC2ContainerCredentialsProviderWrapper.class.getName());

    return conf;
  }

  @Bean
  public CloseableMetaStoreClientFactory metaStoreClientFactory() {
    return new CloseableMetaStoreClientFactory();
  }

  @Bean
  Supplier<CloseableMetaStoreClient> metaStoreClientSupplier(
      CloseableMetaStoreClientFactory metaStoreClientFactory,
      HiveConf hiveConf) {
    String name = "beekeeper-vacuum-tool";
    return new HiveMetaStoreClientSupplier(metaStoreClientFactory, hiveConf, name);
  }

  @Bean
  public SchedulerService schedulerService(BeekeeperRepository beekeeperRepository) {
    return new UnreferencedHousekeepingPathSchedulerService(beekeeperRepository);
  }
}
