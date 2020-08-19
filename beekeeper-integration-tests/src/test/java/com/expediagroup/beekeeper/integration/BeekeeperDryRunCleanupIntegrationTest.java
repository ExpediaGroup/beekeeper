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

import static com.expediagroup.beekeeper.integration.CommonTestVariables.AWS_REGION;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.DATABASE_NAME_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.TABLE_NAME_VALUE;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.awaitility.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import ch.qos.logback.classic.spi.ILoggingEvent;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CreateBucketRequest;

import com.expediagroup.beekeeper.cleanup.BeekeeperCleanup;
import com.expediagroup.beekeeper.cleanup.monitoring.BytesDeletedReporter;
import com.expediagroup.beekeeper.integration.utils.ContainerTestUtils;
import com.expediagroup.beekeeper.integration.utils.TestAppender;

@Testcontainers
@ExtendWith(MockitoExtension.class)
public class BeekeeperDryRunCleanupIntegrationTest extends BeekeeperIntegrationTestBase {

  private static final int TIMEOUT = 30;
  private static final String SPRING_PROFILES_ACTIVE_PROPERTY = "spring.profiles.active";
  private static final String SCHEDULER_DELAY_MS_PROPERTY = "properties.scheduler-delay-ms";
  private static final String DRY_RUN_ENABLED_PROPERTY = "properties.dry-run-enabled";
  private static final String AWS_S3_ENDPOINT_PROPERTY = "aws.s3.endpoint";

  private static final String BUCKET = "test-path-bucket";
  private static final String DB_AND_TABLE_PREFIX = DATABASE_NAME_VALUE + "/" + TABLE_NAME_VALUE;
  private static final String OBJECT_KEY_ROOT = DB_AND_TABLE_PREFIX + "/id1/partition1";
  private static final String OBJECT_KEY1 = DB_AND_TABLE_PREFIX + "/id1/partition1/file1";
  private static final String OBJECT_KEY2 = DB_AND_TABLE_PREFIX + "/id1/partition1/file2";
  private static final String OBJECT_KEY_SENTINEL = DB_AND_TABLE_PREFIX + "/id1/partition1_$folder$";
  private static final String ABSOLUTE_PATH = "s3://" + BUCKET + "/" + OBJECT_KEY_ROOT;

  private static final String OBJECT_KEY_OTHER = DB_AND_TABLE_PREFIX + "/id1/partition10/file1";
  private static final String OBJECT_KEY_OTHER_SENTINEL = DB_AND_TABLE_PREFIX + "/id1/partition10_$folder$";

  private static final String SPRING_PROFILES_ACTIVE = "test";
  private static final String SCHEDULER_DELAY_MS = "5000";
  private static final String DRY_RUN_ENABLED = "true";
  private static final String CONTENT = "Content";

  private static final String S3_CLIENT_CLASS_NAME = "S3Client";

  @Container
  private static final LocalStackContainer S3_CONTAINER = ContainerTestUtils.awsContainer(S3);
  private static AmazonS3 amazonS3;

  private final ExecutorService executorService = Executors.newFixedThreadPool(1);
  private final TestAppender appender = new TestAppender();

  @BeforeAll
  public static void init() {
    System.setProperty(SPRING_PROFILES_ACTIVE_PROPERTY, SPRING_PROFILES_ACTIVE);
    System.setProperty(SCHEDULER_DELAY_MS_PROPERTY, SCHEDULER_DELAY_MS);
    System.setProperty(DRY_RUN_ENABLED_PROPERTY, DRY_RUN_ENABLED);
    System.setProperty(AWS_S3_ENDPOINT_PROPERTY, ContainerTestUtils.awsServiceEndpoint(S3_CONTAINER, S3));

    amazonS3 = ContainerTestUtils.s3Client(S3_CONTAINER, AWS_REGION);
    amazonS3.createBucket(new CreateBucketRequest(BUCKET, AWS_REGION));
  }

  @AfterAll
  public static void teardown() {
    System.clearProperty(SPRING_PROFILES_ACTIVE_PROPERTY);
    System.clearProperty(SCHEDULER_DELAY_MS_PROPERTY);
    System.clearProperty(DRY_RUN_ENABLED_PROPERTY);
    System.clearProperty(AWS_S3_ENDPOINT_PROPERTY);

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

    // clear all logs before asserting them
    appender.clear();
  }

  @AfterEach
  public void stop() throws InterruptedException {
    BeekeeperCleanup.stop();
    executorService.awaitTermination(2, TimeUnit.SECONDS);
  }

  @Test
  public void filesNotDeletedInDirectory() throws SQLException {
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY2, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_OTHER, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");
    amazonS3.putObject(BUCKET, OBJECT_KEY_OTHER_SENTINEL, "");

    insertUnreferencedPath(ABSOLUTE_PATH);
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> logsContainLineFromS3Client(OBJECT_KEY_SENTINEL));

    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY1)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY2)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_SENTINEL)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_OTHER)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_OTHER_SENTINEL)).isTrue();
  }

  @Test
  public void logsForDirectory() throws SQLException {
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY2, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_OTHER, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");
    amazonS3.putObject(BUCKET, OBJECT_KEY_OTHER_SENTINEL, "");

    insertUnreferencedPath(ABSOLUTE_PATH);
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> logsContainLineFromS3Client(OBJECT_KEY_SENTINEL));

    assertS3ClientLogs(3);
  }

  @Test
  public void logsForFile() throws SQLException {
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_OTHER, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");

    String filePath = "s3://" + BUCKET + "/" + OBJECT_KEY1;
    insertUnreferencedPath(filePath);
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> logsContainLineFromS3Client(OBJECT_KEY1));

    assertS3ClientLogs(1);
  }

  @Test
  public void logsForParentDeletion() throws SQLException {
    String parentSentinel = DB_AND_TABLE_PREFIX + "/id1_$folder$";
    String tableSentinel = DB_AND_TABLE_PREFIX + "_$folder$";
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY2, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");
    amazonS3.putObject(BUCKET, parentSentinel, "");
    amazonS3.putObject(BUCKET, tableSentinel, "");

    insertUnreferencedPath(ABSOLUTE_PATH);
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> logsContainLineFromS3Client(parentSentinel));

    assertS3ClientLogs(4);
  }

  @Test
  public void logsForNonEmptyParentDeletion() throws SQLException {
    String parentSentinel = DB_AND_TABLE_PREFIX + "/id1_$folder$";
    String tableSentinel = DB_AND_TABLE_PREFIX + "_$folder$";
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY2, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");
    amazonS3.putObject(BUCKET, OBJECT_KEY_OTHER, CONTENT);
    amazonS3.putObject(BUCKET, parentSentinel, "");
    amazonS3.putObject(BUCKET, tableSentinel, "");

    insertUnreferencedPath(ABSOLUTE_PATH);
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> logsContainLineFromS3Client(OBJECT_KEY_SENTINEL));

    assertS3ClientLogs(3);
  }

  @Test
  public void metrics() throws SQLException {
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");

    insertUnreferencedPath(ABSOLUTE_PATH);
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> logsContainLineFromS3Client(OBJECT_KEY_SENTINEL));

    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY1)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_SENTINEL)).isTrue();
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(this::assertMetrics);
  }

  private boolean assertMetrics() {
    MeterRegistry meterRegistry = BeekeeperCleanup.meterRegistry();
    List<Meter> meters = meterRegistry.getMeters();
    assertThat(meters).extracting("id", Meter.Id.class).extracting("name")
        .contains("cleanup-job", "s3-paths-deleted", "s3-" + BytesDeletedReporter.DRY_RUN_METRIC_NAME);
    return true;
  }

  private boolean logsContainLineFromS3Client(String messageFragment) {
    for (ILoggingEvent event : TestAppender.events) {
      boolean messageIsInLogs = event.getFormattedMessage().contains(messageFragment);
      if (messageIsInLogs) {
        return true;
      }
    }
    return false;
  }

  private void assertS3ClientLogs(int expected) {
    int logsFromS3Client = 0;
    List<ILoggingEvent> events = TestAppender.events;
    for (ILoggingEvent event : events) {
      if (event.getLoggerName().contains(S3_CLIENT_CLASS_NAME)) {
        logsFromS3Client++;
      }
    }
    assertThat(logsFromS3Client).isEqualTo(expected);
  }
}
