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
import static com.expediagroup.beekeeper.core.monitoring.BytesDeletedReporter.METRIC_NAME;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.AWS_REGION;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.DATABASE_NAME_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.TABLE_NAME_VALUE;

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
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CreateBucketRequest;

import com.expediagroup.beekeeper.cleanup.BeekeeperCleanup;
import com.expediagroup.beekeeper.integration.utils.ContainerTestUtils;

@Testcontainers
public class BeekeeperCleanupIntegrationTest extends BeekeeperIntegrationTestBase {

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

  @Container
  private static final LocalStackContainer S3_CONTAINER = ContainerTestUtils.awsContainer(S3);
  private static AmazonS3 amazonS3;

  private final ExecutorService executorService = Executors.newFixedThreadPool(1);

  @BeforeAll
  public static void init() {
    System.setProperty("spring.profiles.active", "test");
    System.setProperty("properties.scheduler-delay-ms", SCHEDULER_DELAY_MS);
    System.setProperty("properties.dry-run-enabled", "false");
    System.setProperty("aws.s3.endpoint", ContainerTestUtils.awsServiceEndpoint(S3_CONTAINER, S3));

    amazonS3 = ContainerTestUtils.s3Client(S3_CONTAINER, AWS_REGION);
    amazonS3.createBucket(new CreateBucketRequest(BUCKET, AWS_REGION));
  }

  @AfterAll
  public static void teardown() {
    amazonS3.shutdown();
  }

  @BeforeEach
  public void setup() {
    amazonS3.listObjectsV2(BUCKET)
        .getObjectSummaries()
        .forEach(object -> amazonS3.deleteObject(BUCKET, object.getKey()));
    executorService.execute(() -> BeekeeperCleanup.main(new String[] {}));
    await().atMost(Duration.ONE_MINUTE)
        .until(BeekeeperCleanup::isRunning);
  }

  @AfterEach
  public void stop() throws InterruptedException {
    BeekeeperCleanup.stop();
    executorService.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test
  public void cleanupPathsForFile() throws SQLException {
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
  public void cleanupPathsForDirectory() throws SQLException {
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
  public void cleanupPathsForDirectoryWithSpace() throws SQLException {
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
  public void cleanupPathsForDirectoryWithTrailingSlash() throws SQLException {
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
  public void cleanupSentinelForParent() throws SQLException {
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
  public void cleanupSentinelForNonEmptyParent() throws SQLException {
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
  public void metrics() throws SQLException {
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
    Set<MeterRegistry> meterRegistry = ((CompositeMeterRegistry) BeekeeperCleanup.meterRegistry()).getRegistries();
    assertThat(meterRegistry).hasSize(2);
    meterRegistry.forEach(registry -> {
      List<Meter> meters = registry.getMeters();
      assertThat(meters).extracting("id", Meter.Id.class).extracting("name")
          .contains("cleanup-job", "s3-paths-deleted", "s3-" + METRIC_NAME);
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
