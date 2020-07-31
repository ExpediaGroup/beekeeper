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
package com.expediagroup.beekeeper.integration;

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import static com.expediagroup.beekeeper.integration.CommonTestVariables.AWS_REGION;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.DATABASE_NAME_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.TABLE_NAME_VALUE;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.thrift.TException;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.google.common.collect.ImmutableMap;

import com.expediagroup.beekeeper.integration.utils.ContainerTestUtils;
import com.expediagroup.beekeeper.integration.utils.HiveTestUtils;

import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;

public class test extends BeekeeperIntegrationTestBase {

  private static final int TIMEOUT = 30;

  private static final String BUCKET = "test-path-bucket";
  private static final String DB_AND_TABLE_PREFIX = DATABASE_NAME_VALUE + "/" + TABLE_NAME_VALUE;
  private static final String OBJECT_KEY_ROOT = DB_AND_TABLE_PREFIX + "/id1/partition1";
  private static final String OBJECT_KEY1 = DB_AND_TABLE_PREFIX + "/id1/partition1/file1";
  private static final String OBJECT_KEY2 = DB_AND_TABLE_PREFIX + "/id1/partition1/file2";
  private static final String OBJECT_KEY_SENTINEL = DB_AND_TABLE_PREFIX + "/id1/partition1_$folder$";
  private static final String ABSOLUTE_PATH = "s3://" + BUCKET + "/" + OBJECT_KEY_ROOT;

  private static final String OBJECT_KEY_OTHER = DB_AND_TABLE_PREFIX + "/id1/partition10/file1";
  private static final String OBJECT_KEY_OTHER_SENTINEL = DB_AND_TABLE_PREFIX + "/id1/partition10_$folder$";

  private static final String SCHEDULER_DELAY_MS = "5000";
  private static final String CONTENT = "Content";
  private static final String HEALTHCHECK_URI = "http://localhost:8008/actuator/health";
  private static final String PROMETHEUS_URI = "http://localhost:8008/actuator/prometheus";

  private static AmazonS3 amazonS3;
  private static LocalStackContainer s3Container;

  private static final String S3_ACCESS_KEY = "access";
  private static final String S3_SECRET_KEY = "secret";

  private final ExecutorService executorService = Executors.newFixedThreadPool(1);

  private static final String ENDPOINT = ContainerTestUtils.awsServiceEndpoint(s3Container, S3);

  // Hive
  private static Map<String, String> metastoreProperties;

  // @RegisterExtension
  // public static ThriftHiveMetaStoreJUnitExtension thriftHive = new ThriftHiveMetaStoreJUnitExtension(
  // DATABASE_NAME_VALUE, metastoreProperties);

  public static @Rule ThriftHiveMetaStoreJUnitRule thriftHive = new ThriftHiveMetaStoreJUnitRule(DATABASE_NAME_VALUE,
      metastoreProperties);

  private HiveTestUtils hiveTestUtils = new HiveTestUtils();

  @BeforeAll
  public static void init() {
    s3Container = ContainerTestUtils.awsContainer(S3);
    s3Container.start();

    System.setProperty("spring.profiles.active", "test");
    System.setProperty("properties.scheduler-delay-ms", SCHEDULER_DELAY_MS);
    System.setProperty("properties.dry-run-enabled", "false");
    System.setProperty("properties.metastore-uri", thriftHive.getThriftConnectionUri());
    System.setProperty("aws.s3.endpoint", ENDPOINT);
    System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");
    System.setProperty("com.amazonaws.services.s3.disablePutObjectMD5Validation", "true");

    amazonS3 = ContainerTestUtils.s3Client(s3Container, AWS_REGION);
    amazonS3.createBucket(new CreateBucketRequest(BUCKET, AWS_REGION));
    //
    metastoreProperties = ImmutableMap
        .<String, String>builder()
        .put("fs.s3a.endpoint", ContainerTestUtils.awsServiceEndpoint(s3Container, S3))
        .put("fs.s3a.access.key", S3_ACCESS_KEY)
        .put("fs.s3a.secret.key", S3_SECRET_KEY)
        .build();
  }

  @Test
  public void cleanupPathsForFile() throws TException {
    String path = "s3://" + BUCKET + "/" + OBJECT_KEY1;

    String uri = thriftHive.getThriftConnectionUri();
    System.out.println("***** Got uri - " + uri);

    HiveMetaStoreClient metastoreClient = thriftHive.client();

    hiveTestUtils.createUnpartitionedTable(metastoreClient, path);

  }

}
