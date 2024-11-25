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

import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.SECRET_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import static com.expediagroup.beekeeper.cleanup.monitoring.BytesDeletedReporter.METRIC_NAME;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DELETED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SKIPPED;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.AWS_REGION;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.DATABASE_NAME_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.TABLE_NAME_VALUE;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.thrift.TException;
import org.awaitility.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.google.common.collect.ImmutableMap;

import com.expediagroup.beekeeper.core.model.HousekeepingEntity;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.integration.utils.ContainerTestUtils;
import com.expediagroup.beekeeper.integration.utils.HiveTestUtils;
import com.expediagroup.beekeeper.path.cleanup.BeekeeperPathCleanup;

import com.hotels.beeju.extensions.ThriftHiveMetaStoreJUnitExtension;

@Testcontainers
public class BeekeeperPathCleanupIntegrationTest extends BeekeeperIntegrationTestBase {

  private static final int TIMEOUT = 30;
  private static final String SPRING_PROFILES_ACTIVE_PROPERTY = "spring.profiles.active";
  private static final String SCHEDULER_DELAY_MS_PROPERTY = "properties.scheduler-delay-ms";
  private static final String DRY_RUN_ENABLED_PROPERTY = "properties.dry-run-enabled";
  private static final String AWS_S3_ENDPOINT_PROPERTY = "aws.s3.endpoint";
  private static final String METASTORE_URI_PROPERTY = "properties.metastore-uri";
  private static final String AWS_DISABLE_GET_VALIDATION_PROPERTY = "com.amazonaws.services.s3.disableGetObjectMD5Validation";
  private static final String AWS_DISABLE_PUT_VALIDATION_PROPERTY = "com.amazonaws.services.s3.disablePutObjectMD5Validation";

  private static final String S3_ACCESS_KEY = "access";
  private static final String S3_SECRET_KEY = "secret";

  private static final String BUCKET = "test-path-bucket";
  private static final String DB_AND_TABLE_PREFIX = DATABASE_NAME_VALUE + "/" + TABLE_NAME_VALUE;
  private static final String OBJECT_KEY_ROOT = DB_AND_TABLE_PREFIX + "/id1/partition1";
  private static final String OBJECT_KEY1 = DB_AND_TABLE_PREFIX + "/id1/partition1/file1";
  private static final String OBJECT_KEY2 = DB_AND_TABLE_PREFIX + "/id1/partition1/file2";
  private static final String OBJECT_KEY_SENTINEL = DB_AND_TABLE_PREFIX + "/id1/partition1_$folder$";
  private static final String ABSOLUTE_PATH = "s3://" + BUCKET + "/" + OBJECT_KEY_ROOT;
  private static final String TABLE_PATH = "s3a://" + BUCKET + "/" + DATABASE_NAME_VALUE + "/" + TABLE_NAME_VALUE + "/";

  private static final String OBJECT_KEY_OTHER = DB_AND_TABLE_PREFIX + "/id1/partition10/file1";
  private static final String OBJECT_KEY_OTHER_SENTINEL = DB_AND_TABLE_PREFIX + "/id1/partition10_$folder$";

  private static final String SPRING_PROFILES_ACTIVE = "test";
  private static final String SCHEDULER_DELAY_MS = "5000";
  private static final String DRY_RUN_ENABLED = "false";
  private static final String CONTENT = "Content";
  private static final String HEALTHCHECK_URI = "http://localhost:8008/actuator/health";
  private static final String PROMETHEUS_URI = "http://localhost:8008/actuator/prometheus";

  @Container
  private static final LocalStackContainer S3_CONTAINER = ContainerTestUtils.awsContainer(S3);
  static {
    S3_CONTAINER.start();
  }
  private static AmazonS3 amazonS3;
  private static final String S3_ENDPOINT = ContainerTestUtils.awsServiceEndpoint(S3_CONTAINER, S3);
  private final ExecutorService executorService = Executors.newFixedThreadPool(1);

  private static Map<String, String> metastoreProperties = ImmutableMap
      .<String, String>builder()
      .put(ENDPOINT, S3_ENDPOINT)
      .put(ACCESS_KEY, S3_ACCESS_KEY)
      .put(SECRET_KEY, S3_SECRET_KEY)
      .build();

  @RegisterExtension
  public ThriftHiveMetaStoreJUnitExtension thriftHiveMetaStore = new ThriftHiveMetaStoreJUnitExtension(
      DATABASE_NAME_VALUE, metastoreProperties);

  private HiveTestUtils hiveTestUtils;
  private HiveMetaStoreClient metastoreClient;

  @BeforeAll
  public static void init() {
    System.setProperty(SPRING_PROFILES_ACTIVE_PROPERTY, SPRING_PROFILES_ACTIVE);
    System.setProperty(SCHEDULER_DELAY_MS_PROPERTY, SCHEDULER_DELAY_MS);
    System.setProperty(DRY_RUN_ENABLED_PROPERTY, DRY_RUN_ENABLED);
    System.setProperty(AWS_S3_ENDPOINT_PROPERTY, S3_ENDPOINT);
    System.setProperty(AWS_DISABLE_GET_VALIDATION_PROPERTY, "true");
    System.setProperty(AWS_DISABLE_PUT_VALIDATION_PROPERTY, "true");

    amazonS3 = ContainerTestUtils.s3Client(S3_CONTAINER, AWS_REGION);
    amazonS3.createBucket(new CreateBucketRequest(BUCKET, AWS_REGION));
  }

  @AfterAll
  public static void teardown() {
    System.clearProperty(SPRING_PROFILES_ACTIVE_PROPERTY);
    System.clearProperty(SCHEDULER_DELAY_MS_PROPERTY);
    System.clearProperty(DRY_RUN_ENABLED_PROPERTY);
    System.clearProperty(AWS_S3_ENDPOINT_PROPERTY);
    System.clearProperty(METASTORE_URI_PROPERTY);
    System.clearProperty(AWS_DISABLE_GET_VALIDATION_PROPERTY);
    System.clearProperty(AWS_DISABLE_PUT_VALIDATION_PROPERTY);

    amazonS3.shutdown();
    S3_CONTAINER.stop();
  }

  @BeforeEach
  public void setup() {
    System.setProperty(METASTORE_URI_PROPERTY, thriftHiveMetaStore.getThriftConnectionUri());
    metastoreClient = thriftHiveMetaStore.client();
    hiveTestUtils = new HiveTestUtils(metastoreClient);

    amazonS3.listObjectsV2(BUCKET)
        .getObjectSummaries()
        .forEach(object -> amazonS3.deleteObject(BUCKET, object.getKey()));
    executorService.execute(() -> BeekeeperPathCleanup.main(new String[] {}));
    await().atMost(Duration.ONE_MINUTE)
        .until(BeekeeperPathCleanup::isRunning);
  }

  @AfterEach
  public void stop() throws InterruptedException {
    BeekeeperPathCleanup.stop();
    executorService.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test
  public void cleanupPathsForFile() throws SQLException, TException {
    hiveTestUtils.createTable(TABLE_PATH, TABLE_NAME_VALUE, false);
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_OTHER, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");

    String path = "s3://" + BUCKET + "/" + OBJECT_KEY1;
    insertUnreferencedPath(path);
    await().atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getUnreferencedPaths().get(0).getHousekeepingStatus() == DELETED);

    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY1)).isFalse();
    // deleting a file shouldn't delete a folder sentinel
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_SENTINEL)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_OTHER)).isTrue();
  }

  @Test
  public void cleanupPathsForDirectory() throws SQLException, TException {
    hiveTestUtils.createTable(TABLE_PATH, TABLE_NAME_VALUE, false);
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY2, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_OTHER, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");
    amazonS3.putObject(BUCKET, OBJECT_KEY_OTHER_SENTINEL, "");

    insertUnreferencedPath(ABSOLUTE_PATH);
    await().atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getUnreferencedPaths().get(0).getHousekeepingStatus() == DELETED);

    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY1)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY2)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_SENTINEL)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_OTHER)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_OTHER_SENTINEL)).isTrue();
  }

  @Test
  public void cleanupPathsForDirectoryWithSpace() throws SQLException, TException {
    hiveTestUtils.createTable(TABLE_PATH, TABLE_NAME_VALUE, false);
    String objectKeyRoot = DB_AND_TABLE_PREFIX + "/ /id1/partition1";
    String objectKey1 = objectKeyRoot + "/file1";
    String objectKey2 = objectKeyRoot + "/file2";
    String objectKeySentinel = objectKeyRoot + "_$folder$";
    String absolutePath = "s3://" + BUCKET + "/" + objectKeyRoot;
    amazonS3.putObject(BUCKET, objectKey1, CONTENT);
    amazonS3.putObject(BUCKET, objectKey2, CONTENT);
    amazonS3.putObject(BUCKET, objectKeySentinel, "");

    insertUnreferencedPath(absolutePath);
    await().atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getUnreferencedPaths().get(0).getHousekeepingStatus() == DELETED);

    assertThat(amazonS3.doesObjectExist(BUCKET, objectKey1)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, objectKey2)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, objectKeySentinel)).isFalse();
  }

  @Test
  public void cleanupPathsForDirectoryWithTrailingSlash() throws SQLException, TException {
    hiveTestUtils.createTable(TABLE_PATH, TABLE_NAME_VALUE, false);
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY2, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_OTHER, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");

    insertUnreferencedPath(ABSOLUTE_PATH + "/");
    await().atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getUnreferencedPaths().get(0).getHousekeepingStatus() == DELETED);

    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY1)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY2)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_SENTINEL)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_OTHER)).isTrue();
  }

  @Test
  public void cleanupSentinelForParent() throws SQLException, TException {
    hiveTestUtils.createTable(TABLE_PATH, TABLE_NAME_VALUE, false);
    String parentSentinel = DB_AND_TABLE_PREFIX + "/id1_$folder$";
    String tableSentinel = DB_AND_TABLE_PREFIX + "_$folder$";
    String databaseSentinel = "database_$folder$";
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY2, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");
    amazonS3.putObject(BUCKET, parentSentinel, "");
    amazonS3.putObject(BUCKET, tableSentinel, "");
    amazonS3.putObject(BUCKET, databaseSentinel, "");

    insertUnreferencedPath(ABSOLUTE_PATH);
    await().atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getUnreferencedPaths().get(0).getHousekeepingStatus() == DELETED);

    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY1)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY2)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_SENTINEL)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, parentSentinel)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, tableSentinel)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, databaseSentinel)).isTrue();
  }

  @Test
  public void cleanupSentinelForNonEmptyParent() throws SQLException, TException {
    hiveTestUtils.createTable(TABLE_PATH, TABLE_NAME_VALUE, false);
    String parentSentinel = DB_AND_TABLE_PREFIX + "/id1_$folder$";
    String tableSentinel = DB_AND_TABLE_PREFIX + "_$folder$";
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY2, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");
    amazonS3.putObject(BUCKET, OBJECT_KEY_OTHER, CONTENT);
    amazonS3.putObject(BUCKET, parentSentinel, "");
    amazonS3.putObject(BUCKET, tableSentinel, "");

    insertUnreferencedPath(ABSOLUTE_PATH);
    await().atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getUnreferencedPaths().get(0).getHousekeepingStatus() == DELETED);

    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY1)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY2)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_SENTINEL)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, parentSentinel)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, tableSentinel)).isTrue();
  }

  @Test
  public void shouldSkipCleanupForIcebergTable() throws Exception {
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("table_type", "ICEBERG");
    tableProperties.put("format", "ICEBERG/PARQUET");
    String outputFormat = "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat";

    hiveTestUtils.createTableWithProperties(
        TABLE_PATH, TABLE_NAME_VALUE, false, tableProperties, outputFormat, true);

    String objectKey = DATABASE_NAME_VALUE + "/" + TABLE_NAME_VALUE + "/file1";
    String path = "s3://" + BUCKET + "/" + DATABASE_NAME_VALUE + "/" + TABLE_NAME_VALUE + "/";

    amazonS3.putObject(BUCKET, objectKey, CONTENT);
    insertUnreferencedPath(path);

    await().atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getUnreferencedPaths().get(0).getHousekeepingStatus() == SKIPPED);

    assertThat(amazonS3.doesObjectExist(BUCKET, objectKey))
        .withFailMessage("S3 object %s should still exist as cleanup was skipped.", objectKey)
        .isTrue();
  }

  @Test
  public void metrics() throws SQLException, TException {
    hiveTestUtils.createTable(TABLE_PATH, TABLE_NAME_VALUE, false);
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");

    insertUnreferencedPath(ABSOLUTE_PATH);
    await().atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getUnreferencedPaths().get(0).getHousekeepingStatus() == DELETED);

    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY1)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_SENTINEL)).isFalse();
    assertMetrics();
  }

  private void assertMetrics() {
    Set<MeterRegistry> meterRegistry = ((CompositeMeterRegistry) BeekeeperPathCleanup.meterRegistry()).getRegistries();
    assertThat(meterRegistry).hasSize(2);
    meterRegistry.forEach(registry -> {
      List<Meter> meters = registry.getMeters();
      assertThat(meters).extracting("id", Meter.Id.class).extracting("name")
          .contains("path-cleanup-job", "s3-paths-deleted", "s3-" + METRIC_NAME);
    });
  }

  @Test
  public void healthCheck() {
    CloseableHttpClient client = HttpClientBuilder.create().build();
    HttpGet request = new HttpGet(HEALTHCHECK_URI);
    await().atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> client.execute(request).getStatusLine().getStatusCode() == 200);
  }

  @Test
  public void prometheus() {
    CloseableHttpClient client = HttpClientBuilder.create().build();
    HttpGet request = new HttpGet(PROMETHEUS_URI);
    await().atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> client.execute(request).getStatusLine().getStatusCode() == 200);
  }
}
