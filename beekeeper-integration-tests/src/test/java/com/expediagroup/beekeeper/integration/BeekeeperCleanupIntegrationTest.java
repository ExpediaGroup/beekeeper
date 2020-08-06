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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DELETED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.awaitility.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CreateBucketRequest;

import com.expediagroup.beekeeper.cleanup.monitoring.BytesDeletedReporter;
import com.expediagroup.beekeeper.path.cleanup.BeekeeperPathCleanup;

public class BeekeeperCleanupIntegrationTest {

  private static final String REGION = "us-west-2";
  private static final String BUCKET = "test-path-bucket";

  private static final String OBJECT_KEY_ROOT = "database/table/id1/partition1";
  private static final String OBJECT_KEY1 = "database/table/id1/partition1/file1";
  private static final String OBJECT_KEY2 = "database/table/id1/partition1/file2";
  private static final String OBJECT_KEY_SENTINEL = "database/table/id1/partition1_$folder$";
  private static final String ABSOLUTE_PATH = "s3://" + BUCKET + "/" + OBJECT_KEY_ROOT;

  private static final String OBJECT_KEY_OTHER = "database/table/id1/partition10/file1";
  private static final String OBJECT_KEY_OTHER_SENTINEL = "database/table/id1/partition10_$folder$";

  private static final String TABLE_NAME = "table";
  private static final String BEEKEEPER_PATH_HOUSEKEEPING_TABLE = "housekeeping_path";
  private static final String BEEKEEPER_METADATA_HOUSEKEEPING_TABLE = "housekeeping_metadata";
  private static final String FLYWAY_TABLE = "flyway_schema_history";
  private static final String SCHEDULER_DELAY_MS = "5000";
  private static final String AWS_ACCESS_KEY_ID = "accessKey";
  private static final String AWS_SECRET_KEY = "secretKey";
  private static final String CONTENT = "Content";
  private static final String HEALTHCHECK_URI = "http://localhost:8008/actuator/health";
  private static final String PROMETHEUS_URI = "http://localhost:8008/actuator/prometheus";

  private static AmazonS3 amazonS3;
  private static LocalStackContainer s3Container;
  private static MySQLContainer mySQLContainer;
  private static MySqlTestUtils mySqlTestUtils;

  private final ExecutorService executorService = Executors.newFixedThreadPool(1);

  @BeforeAll
  static void init() throws SQLException {
    s3Container = ContainerTestUtils.awsContainer(S3);
    mySQLContainer = ContainerTestUtils.mySqlContainer();
    s3Container.start();
    mySQLContainer.start();

    String jdbcUrl = mySQLContainer.getJdbcUrl() + "?useSSL=false";
    String username = mySQLContainer.getUsername();
    String password = mySQLContainer.getPassword();

    System.setProperty("spring.datasource.url", jdbcUrl);
    System.setProperty("spring.datasource.username", username);
    System.setProperty("spring.datasource.password", password);
    System.setProperty("spring.profiles.active", "test");
    System.setProperty("properties.scheduler-delay-ms", SCHEDULER_DELAY_MS);
    System.setProperty("properties.dry-run-enabled", "false");
    System.setProperty("aws.s3.endpoint", ContainerTestUtils.awsServiceEndpoint(s3Container, S3));
    System.setProperty("aws.accessKeyId", AWS_ACCESS_KEY_ID);
    System.setProperty("aws.secretKey", AWS_SECRET_KEY);
    System.setProperty("aws.region", REGION);

    amazonS3 = ContainerTestUtils.s3Client(s3Container, REGION);
    amazonS3.createBucket(new CreateBucketRequest(BUCKET, REGION));

    mySqlTestUtils = new MySqlTestUtils(jdbcUrl, username, password);
  }

  @AfterAll
  static void teardown() throws SQLException {
    amazonS3.shutdown();
    mySqlTestUtils.close();
    s3Container.stop();
    mySQLContainer.stop();
  }

  @BeforeEach
  void setup() throws SQLException {
    amazonS3.listObjectsV2(BUCKET)
        .getObjectSummaries()
        .forEach(object -> amazonS3.deleteObject(BUCKET, object.getKey()));
    mySqlTestUtils.dropTable(BEEKEEPER_PATH_HOUSEKEEPING_TABLE);
    mySqlTestUtils.dropTable(BEEKEEPER_METADATA_HOUSEKEEPING_TABLE);
    mySqlTestUtils.dropTable(FLYWAY_TABLE);
    executorService.execute(() -> BeekeeperPathCleanup.main(new String[] {}));
    await().atMost(Duration.ONE_MINUTE)
        .until(BeekeeperPathCleanup::isRunning);
  }

  @AfterEach
  void stop() throws InterruptedException {
    BeekeeperPathCleanup.stop();
    executorService.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test
  void cleanupPathsForFile() throws SQLException {
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_OTHER, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");

    String path = "s3://" + BUCKET + "/" + OBJECT_KEY1;
    mySqlTestUtils.insertPath(BEEKEEPER_PATH_HOUSEKEEPING_TABLE, path, TABLE_NAME);
    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> mySqlTestUtils.getPaths(BEEKEEPER_PATH_HOUSEKEEPING_TABLE, UNREFERENCED.toString())
            .get(0)
            .getHousekeepingStatus() == DELETED);

    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY1)).isFalse();
    // deleting a file shouldn't delete a folder sentinel
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_SENTINEL)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_OTHER)).isTrue();
  }

  @Test
  void cleanupPathsForDirectory() throws SQLException {
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY2, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_OTHER, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");
    amazonS3.putObject(BUCKET, OBJECT_KEY_OTHER_SENTINEL, "");

    mySqlTestUtils.insertPath(BEEKEEPER_PATH_HOUSEKEEPING_TABLE, ABSOLUTE_PATH, TABLE_NAME);
    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> mySqlTestUtils.getPaths(BEEKEEPER_PATH_HOUSEKEEPING_TABLE, UNREFERENCED.toString())
            .get(0)
            .getHousekeepingStatus() == DELETED);

    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY1)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY2)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_SENTINEL)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_OTHER)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_OTHER_SENTINEL)).isTrue();
  }

  @Test
  void cleanupPathsForDirectoryWithSpace() throws SQLException {
    String objectKeyRoot = "database/table/ /id1/partition1";
    String objectKey1 = objectKeyRoot + "/file1";
    String objectKey2 = objectKeyRoot + "/file2";
    String objectKeySentinel = objectKeyRoot + "_$folder$";
    String absolutePath = "s3://" + BUCKET + "/" + objectKeyRoot;
    amazonS3.putObject(BUCKET, objectKey1, CONTENT);
    amazonS3.putObject(BUCKET, objectKey2, CONTENT);
    amazonS3.putObject(BUCKET, objectKeySentinel, "");

    mySqlTestUtils.insertPath(BEEKEEPER_PATH_HOUSEKEEPING_TABLE, absolutePath, TABLE_NAME);
    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> mySqlTestUtils.getPaths(BEEKEEPER_PATH_HOUSEKEEPING_TABLE, UNREFERENCED.toString())
            .get(0)
            .getHousekeepingStatus() == DELETED);

    assertThat(amazonS3.doesObjectExist(BUCKET, objectKey1)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, objectKey2)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, objectKeySentinel)).isFalse();
  }

  @Test
  void cleanupPathsForDirectoryWithTrailingSlash() throws SQLException {
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY2, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_OTHER, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");

    mySqlTestUtils.insertPath(BEEKEEPER_PATH_HOUSEKEEPING_TABLE, ABSOLUTE_PATH + "/", TABLE_NAME);
    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> mySqlTestUtils.getPaths(BEEKEEPER_PATH_HOUSEKEEPING_TABLE, UNREFERENCED.toString())
            .get(0)
            .getHousekeepingStatus() == DELETED);

    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY1)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY2)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_SENTINEL)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_OTHER)).isTrue();
  }

  @Test
  void cleanupSentinelForParent() throws SQLException {
    String parentSentinel = "database/table/id1_$folder$";
    String tableSentinel = "database/table_$folder$";
    String databaseSentinel = "database_$folder$";
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY2, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");
    amazonS3.putObject(BUCKET, parentSentinel, "");
    amazonS3.putObject(BUCKET, tableSentinel, "");
    amazonS3.putObject(BUCKET, databaseSentinel, "");

    mySqlTestUtils.insertPath(BEEKEEPER_PATH_HOUSEKEEPING_TABLE, ABSOLUTE_PATH, TABLE_NAME);
    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> mySqlTestUtils.getPaths(BEEKEEPER_PATH_HOUSEKEEPING_TABLE, UNREFERENCED.toString())
            .get(0)
            .getHousekeepingStatus() == DELETED);

    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY1)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY2)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_SENTINEL)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, parentSentinel)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, tableSentinel)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, databaseSentinel)).isTrue();
  }

  @Test
  void cleanupSentinelForNonEmptyParent() throws SQLException {
    String parentSentinel = "database/table/id1_$folder$";
    String tableSentinel = "database/table_$folder$";
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY2, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");
    amazonS3.putObject(BUCKET, OBJECT_KEY_OTHER, CONTENT);
    amazonS3.putObject(BUCKET, parentSentinel, "");
    amazonS3.putObject(BUCKET, tableSentinel, "");

    mySqlTestUtils.insertPath(BEEKEEPER_PATH_HOUSEKEEPING_TABLE, ABSOLUTE_PATH, TABLE_NAME);
    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> mySqlTestUtils.getPaths(BEEKEEPER_PATH_HOUSEKEEPING_TABLE, UNREFERENCED.toString())
            .get(0)
            .getHousekeepingStatus() == DELETED);

    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY1)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY2)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_SENTINEL)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, parentSentinel)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, tableSentinel)).isTrue();
  }

  @Test
  void metrics() throws SQLException {
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");

    mySqlTestUtils.insertPath(BEEKEEPER_PATH_HOUSEKEEPING_TABLE, ABSOLUTE_PATH, TABLE_NAME);
    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> mySqlTestUtils.getPaths(BEEKEEPER_PATH_HOUSEKEEPING_TABLE, UNREFERENCED.toString())
            .get(0)
            .getHousekeepingStatus() == DELETED);

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
          .contains("cleanup-job", "s3-paths-deleted", "s3-" + BytesDeletedReporter.METRIC_NAME);
    });
  }

  @Test
  public void healthCheck() {
    CloseableHttpClient client = HttpClientBuilder.create().build();
    HttpGet request = new HttpGet(HEALTHCHECK_URI);
    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> client.execute(request).getStatusLine().getStatusCode() == 200);
  }

  @Test
  public void prometheus() {
    CloseableHttpClient client = HttpClientBuilder.create().build();
    HttpGet request = new HttpGet(PROMETHEUS_URI);
    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> client.execute(request).getStatusLine().getStatusCode() == 200);
  }
}
