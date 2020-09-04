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
package com.expediagroup.beekeeper.metadata.cleanup.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URL;
import java.util.List;
import java.util.function.Supplier;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import io.micrometer.core.instrument.MeterRegistry;

import com.amazonaws.services.s3.AmazonS3;

import com.expediagroup.beekeeper.cleanup.aws.S3Client;
import com.expediagroup.beekeeper.cleanup.aws.S3PathCleaner;
import com.expediagroup.beekeeper.cleanup.hive.HiveClient;
import com.expediagroup.beekeeper.cleanup.hive.HiveClientFactory;
import com.expediagroup.beekeeper.cleanup.hive.HiveMetadataCleaner;
import com.expediagroup.beekeeper.cleanup.metadata.MetadataCleaner;
import com.expediagroup.beekeeper.cleanup.monitoring.BytesDeletedReporter;
import com.expediagroup.beekeeper.cleanup.monitoring.DeletedMetadataReporter;
import com.expediagroup.beekeeper.cleanup.path.PathCleaner;
import com.expediagroup.beekeeper.cleanup.service.CleanupService;
import com.expediagroup.beekeeper.core.repository.HousekeepingMetadataRepository;
import com.expediagroup.beekeeper.metadata.cleanup.handler.ExpiredMetadataHandler;
import com.expediagroup.beekeeper.metadata.cleanup.service.PagingMetadataCleanupService;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.closeable.CloseableMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.supplier.HiveMetaStoreClientSupplier;

@ExtendWith(MockitoExtension.class)
public class CommonBeansTest {

  private static final String AWS_S3_ENDPOINT_PROPERTY = "aws.s3.endpoint";
  private static final String AWS_REGION_PROPERTY = "aws.region";
  private static final String REGION = "us-west-2";
  private static final String AWS_ENDPOINT = String.join(".", "s3", REGION, "amazonaws.com");
  private static final String ENDPOINT = "endpoint";
  private static final String BUCKET = "bucket";
  private static final String KEY = "key";
  private final String metastoreUri = "thrift://localhost:1234";

  private boolean dryRunEnabled = false;
  private final CommonBeans commonBeans = new CommonBeans();
  private @Mock HousekeepingMetadataRepository metadataRepository;
  private @Mock MetadataCleaner metadataCleaner;
  private @Mock PathCleaner pathCleaner;
  private @Mock MeterRegistry meterRegistry;

  @BeforeEach
  public void awsSetUp() {
    System.setProperty(AWS_REGION_PROPERTY, REGION);
    System.setProperty(AWS_S3_ENDPOINT_PROPERTY, ENDPOINT);
  }

  @AfterAll
  public static void awsTearDown() {
    System.clearProperty(AWS_REGION_PROPERTY);
    System.clearProperty(AWS_S3_ENDPOINT_PROPERTY);
  }

  @Test
  public void typicalHiveConf() {
    HiveConf hiveConf = commonBeans.hiveConf(metastoreUri);
    assertThat(hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname)).isEqualTo(metastoreUri);
  }

  @Test
  public void hiveConfFailure() {
    assertThrows(IllegalArgumentException.class, () -> {
      commonBeans.hiveConf(null);
    });
  }

  @Test
  public void verifyMetaStoreClientSupplier() {
    CloseableMetaStoreClientFactory metaStoreClientFactory = commonBeans.metaStoreClientFactory();
    HiveConf hiveConf = Mockito.mock(HiveConf.class);

    Supplier<CloseableMetaStoreClient> metaStoreClientSupplier = commonBeans
        .metaStoreClientSupplier(metaStoreClientFactory, hiveConf);
    assertThat(metaStoreClientSupplier).isInstanceOf(HiveMetaStoreClientSupplier.class);
  }

  @Test
  public void verifyHiveClient() {
    Supplier<CloseableMetaStoreClient> metaStoreClientSupplier = Mockito.mock(Supplier.class);
    HiveClientFactory hiveClientFactory = commonBeans.hiveClientFactory(metaStoreClientSupplier, false);
    HiveClient hiveClient = hiveClientFactory.newInstance();
    assertThat(hiveClient).isInstanceOf(HiveClient.class);
  }

  @Test
  public void verifyHiveMetadataCleaner() {
    DeletedMetadataReporter reporter = commonBeans.deletedMetadataReporter(meterRegistry, false);
    HiveClientFactory hiveClientFactory = Mockito.mock(HiveClientFactory.class);
    MetadataCleaner metadataCleaner = commonBeans.metadataCleaner(hiveClientFactory, reporter);
    assertThat(metadataCleaner).isInstanceOf(HiveMetadataCleaner.class);
  }

  @Test
  public void typicalAmazonClient() {
    AmazonS3 amazonS3 = commonBeans.amazonS3();
    URL url = amazonS3.getUrl(BUCKET, KEY);
    assertThat(url.getHost()).isEqualTo(String.join(".", BUCKET, AWS_ENDPOINT));
  }

  @Test
  public void endpointConfiguredAmazonClient() {
    AmazonS3 amazonS3 = commonBeans.amazonS3Test();
    URL url = amazonS3.getUrl(BUCKET, KEY);
    assertThat(url.getHost()).isEqualTo(String.join(".", BUCKET, ENDPOINT));
  }

  @Test
  public void verifyS3Client() {
    AmazonS3 amazonS3 = commonBeans.amazonS3Test();
    S3Client s3Client = new S3Client(amazonS3, false);
    S3Client beansS3Client = commonBeans.s3Client(amazonS3, false);
    assertThat(s3Client).isEqualToComparingFieldByField(beansS3Client);
  }

  @Test
  void verifyS3pathCleaner() {
    BytesDeletedReporter reporter = commonBeans.bytesDeletedReporter(meterRegistry, false);
    S3Client s3Client = commonBeans.s3Client(commonBeans.amazonS3(), false);
    PathCleaner pathCleaner = commonBeans.pathCleaner(s3Client, reporter);
    assertThat(pathCleaner).isInstanceOf(S3PathCleaner.class);
  }

  @Test
  public void verifyExpiredMetadataHandler() {
    ExpiredMetadataHandler expiredMetadataHandler = commonBeans.expiredMetadataHandler(metadataRepository,
        metadataCleaner, pathCleaner);
    assertThat(expiredMetadataHandler).isInstanceOf(ExpiredMetadataHandler.class);
  }

  @Test
  public void verifyCleanupService() {
    CleanupService cleanupService = commonBeans.cleanupService(
        List.of(commonBeans.expiredMetadataHandler(metadataRepository, metadataCleaner, pathCleaner)), 2,
        dryRunEnabled);
    assertThat(cleanupService).isInstanceOf(PagingMetadataCleanupService.class);
  }
}
