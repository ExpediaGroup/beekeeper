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
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;

import ch.qos.logback.classic.spi.ILoggingEvent;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CreateBucketRequest;

import com.expediagroup.beekeeper.cleanup.BeekeeperCleanup;
import com.expediagroup.beekeeper.core.monitoring.BytesDeletedReporter;

@ExtendWith(MockitoExtension.class)
class BeekeeperDryRunCleanupIntegrationTest {

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

  private static final String S3_CLIENT_CLASS_NAME = "S3Client";

  private static AmazonS3 amazonS3;
  private static LocalStackContainer s3Container;
  private static MySQLContainer mySQLContainer;
  private static MySqlTestUtils mySqlTestUtils;
  private final ExecutorService executorService = Executors.newFixedThreadPool(1);
  private final TestAppender appender = new TestAppender();

  @BeforeAll
  static void init() throws SQLException {
    s3Container = ContainerTestUtils.awsContainer(S3);
    mySQLContainer = ContainerTestUtils.mySqlContainer();
    s3Container.start();
    mySQLContainer.start();

    String jdbcUrl = mySQLContainer.getJdbcUrl() + "?useSSL=false&allowPublicKeyRetrieval=true";
    String username = mySQLContainer.getUsername();
    String password = mySQLContainer.getPassword();

    System.setProperty("spring.datasource.url", jdbcUrl);
    System.setProperty("spring.datasource.username", username);
    System.setProperty("spring.datasource.password", password);
    System.setProperty("spring.profiles.active", "test");
    System.setProperty("properties.scheduler-delay-ms", SCHEDULER_DELAY_MS);
    System.setProperty("properties.dry-run-enabled", "true");
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
    executorService.execute(() -> BeekeeperCleanup.main(new String[] {}));
    await().atMost(Duration.ONE_MINUTE)
        .until(BeekeeperCleanup::isRunning);

    // clear all logs before asserting them
    appender.clear();
  }

  @AfterEach
  void stop() throws InterruptedException {
    BeekeeperCleanup.stop();
    executorService.awaitTermination(2, TimeUnit.SECONDS);
  }

  @Test
  void filesNotDeletedInDirectory() throws SQLException {
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY2, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_OTHER, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");
    amazonS3.putObject(BUCKET, OBJECT_KEY_OTHER_SENTINEL, "");

    mySqlTestUtils.insertPath(BEEKEEPER_PATH_HOUSEKEEPING_TABLE, ABSOLUTE_PATH, TABLE_NAME);
    await().atMost(30, TimeUnit.SECONDS).until(() -> logsContainLineFromS3Client(OBJECT_KEY_SENTINEL));

    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY1)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY2)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_SENTINEL)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_OTHER)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_OTHER_SENTINEL)).isTrue();
  }

  @Test
  void logsForDirectory() throws SQLException {
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY2, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_OTHER, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");
    amazonS3.putObject(BUCKET, OBJECT_KEY_OTHER_SENTINEL, "");

    mySqlTestUtils.insertPath(BEEKEEPER_PATH_HOUSEKEEPING_TABLE, ABSOLUTE_PATH, TABLE_NAME);
    await().atMost(30, TimeUnit.SECONDS).until(() -> logsContainLineFromS3Client(OBJECT_KEY_SENTINEL));

    assertS3ClientLogs(3);
  }

  @Test
  void logsForFile() throws SQLException {
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_OTHER, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");

    String filePath = "s3://" + BUCKET + "/" + OBJECT_KEY1;
    mySqlTestUtils.insertPath(BEEKEEPER_PATH_HOUSEKEEPING_TABLE, filePath, TABLE_NAME);
    await().atMost(30, TimeUnit.SECONDS).until(() -> logsContainLineFromS3Client(OBJECT_KEY1));

    assertS3ClientLogs(1);
  }

  @Test
  void logsForParentDeletion() throws SQLException {
    String parentSentinel = "database/table/id1_$folder$";
    String tableSentinel = "database/table_$folder$";
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY2, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");
    amazonS3.putObject(BUCKET, parentSentinel, "");
    amazonS3.putObject(BUCKET, tableSentinel, "");

    mySqlTestUtils.insertPath(BEEKEEPER_PATH_HOUSEKEEPING_TABLE, ABSOLUTE_PATH, TABLE_NAME);
    await().atMost(30, TimeUnit.SECONDS).until(() -> logsContainLineFromS3Client(parentSentinel));

    assertS3ClientLogs(4);
  }

  @Test
  void logsForNonEmptyParentDeletion() throws SQLException {
    String parentSentinel = "database/table/id1_$folder$";
    String tableSentinel = "database/table_$folder$";
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY2, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");
    amazonS3.putObject(BUCKET, OBJECT_KEY_OTHER, CONTENT);
    amazonS3.putObject(BUCKET, parentSentinel, "");
    amazonS3.putObject(BUCKET, tableSentinel, "");

    mySqlTestUtils.insertPath(BEEKEEPER_PATH_HOUSEKEEPING_TABLE, ABSOLUTE_PATH, TABLE_NAME);
    await().atMost(30, TimeUnit.SECONDS).until(() -> logsContainLineFromS3Client(OBJECT_KEY_SENTINEL));

    assertS3ClientLogs(3);
  }

  @Test
  void metrics() throws SQLException {
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);
    amazonS3.putObject(BUCKET, OBJECT_KEY_SENTINEL, "");

    mySqlTestUtils.insertPath(BEEKEEPER_PATH_HOUSEKEEPING_TABLE, ABSOLUTE_PATH, TABLE_NAME);
    await().atMost(30, TimeUnit.SECONDS).until(() -> logsContainLineFromS3Client(OBJECT_KEY_SENTINEL));

    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY1)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY_SENTINEL)).isTrue();
    await().atMost(30, TimeUnit.SECONDS).until(this::assertMetrics);
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
