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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;
import com.google.common.base.Supplier;

import com.expediagroup.beekeeper.scheduler.service.SchedulerService;
import com.expediagroup.beekeeper.scheduler.service.UnreferencedPathSchedulerService;
import com.expediagroup.beekeeper.vacuum.repository.BeekeeperRepository;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.closeable.CloseableMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.supplier.HiveMetaStoreClientSupplier;

@ExtendWith(MockitoExtension.class)
class CommonBeansTest {

  private static final String AWS_S3_ENDPOINT_PROPERTY = "aws.s3.endpoint";
  private static final String AWS_REGION_PROPERTY = "aws.region";
  private static final String REGION = "us-west-2";
  private static final String AWS_ENDPOINT = String.join(".", "s3", REGION, "amazonaws.com");
  private static final String ENDPOINT = "endpoint";
  private final CommonBeans commonBeans = new CommonBeans();
  private final String metastoreUri = "thrift://localhost:1234";
  private @Mock BeekeeperRepository repository;

  @AfterAll
  static void teardown() {
    System.clearProperty(AWS_REGION_PROPERTY);
    System.clearProperty(AWS_S3_ENDPOINT_PROPERTY);
  }

  @BeforeEach
  void setUp() {
    System.setProperty(AWS_REGION_PROPERTY, REGION);
    System.setProperty(AWS_S3_ENDPOINT_PROPERTY, ENDPOINT);
  }

  @Test
  void ec2ContainerCredentialsProviderWrapper() {
    EC2ContainerCredentialsProviderWrapper ec2CredentialsWrapper = new EC2ContainerCredentialsProviderWrapper();
    assertThat(ec2CredentialsWrapper).isEqualToComparingFieldByField(
        commonBeans.ec2ContainerCredentialsProviderWrapper());
  }

  @Test
  void hiveConf() {
    HiveConf hiveConf = commonBeans.hiveConf(metastoreUri);
    assertThat(hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname)).isEqualTo(metastoreUri);
    assertThat(hiveConf.get("fs.s3.impl")).isEqualTo(S3AFileSystem.class.getName());
    assertThat(hiveConf.get("fs.s3.aws.credentials.provider")).isEqualTo(
        EC2ContainerCredentialsProviderWrapper.class.getName());
  }

  @Test
  void metaStoreClientSupplier() {
    CloseableMetaStoreClientFactory metaStoreClientFactory = commonBeans.metaStoreClientFactory();
    HiveConf hiveConf = commonBeans.hiveConf(metastoreUri);
    Supplier<CloseableMetaStoreClient> closeableMetaStoreClientSupplier = commonBeans.metaStoreClientSupplier(
        metaStoreClientFactory, hiveConf);
    HiveMetaStoreClientSupplier hiveMetaStoreClientSupplier = new HiveMetaStoreClientSupplier(metaStoreClientFactory,
        hiveConf, "beekeeper-vacuum-tool");
    assertThat(closeableMetaStoreClientSupplier).isEqualToComparingFieldByField(hiveMetaStoreClientSupplier);
  }

  @Test
  void schedulerService() {
    SchedulerService schedulerService = new UnreferencedPathSchedulerService(repository);
    assertThat(schedulerService).isEqualToComparingFieldByField(commonBeans.schedulerService(repository));
  }
}
