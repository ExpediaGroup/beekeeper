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

import static java.time.ZoneOffset.UTC;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpCoreContext;
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

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;
import com.expediagroup.beekeeper.core.model.HousekeepingStatus;
import com.expediagroup.beekeeper.integration.model.AlterPartitionSqsMessage;
import com.expediagroup.beekeeper.integration.model.AlterTableSqsMessage;
import com.expediagroup.beekeeper.integration.model.DropPartitionSqsMessage;
import com.expediagroup.beekeeper.integration.model.DropTableSqsMessage;
import com.expediagroup.beekeeper.scheduler.apiary.BeekeeperPathSchedulerApiary;

public class BeekeeperPathSchedulerApiaryIntegrationTest {

  private static final int TIMEOUT = 5;
  private static final String QUEUE = "apiary-receiver-queue";
  private static final String REGION = "us-west-2";
  private static final String PATH_TABLE = "path";
  private static final String UNPARTITIONED_TABLE_HOUSEKEEPING_TABLE = "unpartitioned_table_housekeeping";
  private static final String FLYWAY_TABLE = "flyway_schema_history";
  private static final String AWS_ACCESS_KEY_ID = "accessKey";
  private static final String AWS_SECRET_KEY = "secretKey";

  private static final String CLEANUP_DELAY_UNREFERENCED = "P7D";
  private static final int CLEANUP_ATTEMPTS = 0;
  private static final String CLIENT_ID = "apiary-metastore-event";
  private static final LocalDateTime CREATION_TIMESTAMP = LocalDateTime.now(UTC).minus(1L, ChronoUnit.MINUTES);
  private static final String DATABASE = "some_db";
  private static final String TABLE = "some_table";
  private static final String HEALTHCHECK_URI = "http://localhost:8080/actuator/health";
  private static final String PROMETHEUS_URI = "http://localhost:8080/actuator/prometheus";

  private static final String SCHEDULED_EXPIRATION_METRIC = "paths-scheduled-expiration";
  private static final String SCHEDULED_ORPHANED_METRIC = "paths-scheduled";

  private static AmazonSQS amazonSQS;
  private static LocalStackContainer sqsContainer;
  private static MySQLContainer mySQLContainer;
  private static MySqlTestUtils mySqlTestUtils;

  private final ExecutorService executorService = Executors.newFixedThreadPool(1);

  @BeforeAll
  static void init() throws SQLException {
    sqsContainer = ContainerTestUtils.awsContainer(SQS);
    mySQLContainer = ContainerTestUtils.mySqlContainer();
    sqsContainer.start();
    mySQLContainer.start();

    String jdbcUrl = mySQLContainer.getJdbcUrl() + "?useSSL=false";
    String username = mySQLContainer.getUsername();
    String password = mySQLContainer.getPassword();
    String queueUrl = ContainerTestUtils.queueUrl(sqsContainer, QUEUE);

    System.setProperty("spring.datasource.url", jdbcUrl);
    System.setProperty("spring.datasource.username", username);
    System.setProperty("spring.datasource.password", password);
    System.setProperty("properties.apiary.queue-url", queueUrl);
    System.setProperty("aws.accessKeyId", AWS_ACCESS_KEY_ID);
    System.setProperty("aws.secretKey", AWS_SECRET_KEY);
    System.setProperty("aws.region", REGION);

    amazonSQS = ContainerTestUtils.sqsClient(sqsContainer, REGION);
    amazonSQS.createQueue(QUEUE);

    mySqlTestUtils = new MySqlTestUtils(jdbcUrl, username, password);
  }

  @AfterAll
  static void teardown() throws SQLException {
    amazonSQS.shutdown();
    mySqlTestUtils.close();
    sqsContainer.stop();
    mySQLContainer.stop();
  }

  @BeforeEach
  void setup() throws SQLException {
    amazonSQS.purgeQueue(new PurgeQueueRequest(ContainerTestUtils.queueUrl(sqsContainer, QUEUE)));
    mySqlTestUtils.dropTable(PATH_TABLE);
    mySqlTestUtils.dropTable(UNPARTITIONED_TABLE_HOUSEKEEPING_TABLE);
    mySqlTestUtils.dropTable(FLYWAY_TABLE);
    executorService.execute(() -> BeekeeperPathSchedulerApiary.main(new String[] {}));
    await().atMost(Duration.ONE_MINUTE).until(BeekeeperPathSchedulerApiary::isRunning);
  }

  @AfterEach
  void stop() throws InterruptedException {
    BeekeeperPathSchedulerApiary.stop();
    executorService.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test
  void unreferencedAlterTableEvent() throws SQLException, IOException {
    AlterTableSqsMessage alterTableSqsMessage = new AlterTableSqsMessage("s3://tableLocation",
        "s3://oldTableLocation", true, true);
    amazonSQS.sendMessage(sendMessageRequest(alterTableSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.unreferencedRowsInTable(PATH_TABLE) == 1);
    List<EntityHousekeepingPath> unreferencedPaths = mySqlTestUtils.getUnreferencedPaths();
    assertUnreferencedPathFields(unreferencedPaths.get(0));
    assertThat(unreferencedPaths.get(0).getPath()).isEqualTo("s3://oldTableLocation");
  }

  @Test
  void unreferencedMultipleAlterTableEvents() throws SQLException, IOException {
    AlterTableSqsMessage alterTableSqsMessage = new AlterTableSqsMessage("s3://tableLocation",
        "s3://oldTableLocation", true, true);
    amazonSQS.sendMessage(sendMessageRequest(alterTableSqsMessage.getFormattedString()));
    alterTableSqsMessage.setTableLocation("s3://tableLocation2");
    alterTableSqsMessage.setOldTableLocation("s3://tableLocation");
    amazonSQS.sendMessage(sendMessageRequest(alterTableSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.unreferencedRowsInTable(PATH_TABLE) == 2);
    List<EntityHousekeepingPath> unreferencedPaths = mySqlTestUtils.getUnreferencedPaths();

    assertUnreferencedPathFields(unreferencedPaths.get(0));
    assertUnreferencedPathFields(unreferencedPaths.get(1));
    assertThat(Set.of(unreferencedPaths.get(0).getPath(), unreferencedPaths.get(1).getPath()))
        .isEqualTo(Set.of("s3://oldTableLocation", "s3://tableLocation"));
  }

  @Test
  void unreferencedAlterPartitionEvent() throws SQLException, IOException {
    AlterPartitionSqsMessage alterPartitionSqsMessage = new AlterPartitionSqsMessage(
        "s3://expiredTableLocation",
        "s3://partitionLocation",
        "s3://unreferencedPartitionLocation",
        true, true);
    amazonSQS.sendMessage(sendMessageRequest(alterPartitionSqsMessage.getFormattedString()));
    AlterPartitionSqsMessage alterPartitionSqsMessage2 = new AlterPartitionSqsMessage(
        "s3://expiredTableLocation2",
        "s3://partitionLocation2",
        "s3://partitionLocation",
        true, true);
    amazonSQS.sendMessage(sendMessageRequest(alterPartitionSqsMessage2.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.unreferencedRowsInTable(PATH_TABLE) == 2);
    List<EntityHousekeepingPath> unreferencedPaths = mySqlTestUtils.getUnreferencedPaths();

    assertUnreferencedPathFields(unreferencedPaths.get(0));
    assertUnreferencedPathFields(unreferencedPaths.get(1));
    assertThat(Set.of(unreferencedPaths.get(0).getPath(), unreferencedPaths.get(1).getPath()))
        .isEqualTo(Set.of("s3://unreferencedPartitionLocation", "s3://partitionLocation"));
  }

  @Test
  void unreferencedMultipleAlterPartitionEvent() throws IOException, SQLException {
    List.of(
        new AlterPartitionSqsMessage("s3://expiredTableLocation", "s3://partitionLocation",
            "s3://unreferencedPartitionLocation", true, true),
        new AlterPartitionSqsMessage("s3://expiredTableLocation2", "s3://partitionLocation2", "s3://partitionLocation",
            true, true)
    ).forEach(msg -> amazonSQS.sendMessage(sendMessageRequest(msg.getFormattedString())));

    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.unreferencedRowsInTable(PATH_TABLE) == 2);
    List<EntityHousekeepingPath> unreferencedPaths = mySqlTestUtils.getUnreferencedPaths();

    unreferencedPaths.forEach(hkPath -> assertUnreferencedPathFields(hkPath));
    Set<String> unreferencedPathSet = unreferencedPaths.stream()
        .map(hkPath -> hkPath.getPath())
        .collect(Collectors.toSet());
    assertThat(unreferencedPathSet).isEqualTo(Set.of("s3://unreferencedPartitionLocation", "s3://partitionLocation"));
  }

  @Test
  void unreferencedDropPartitionEvent() throws SQLException, IOException {
    DropPartitionSqsMessage dropPartitionSqsMessage = new DropPartitionSqsMessage(
        "s3://partitionLocation", true, true);
    amazonSQS.sendMessage(sendMessageRequest(dropPartitionSqsMessage.getFormattedString()));
    dropPartitionSqsMessage.setPartitionLocation("s3://partitionLocation2");
    amazonSQS.sendMessage(sendMessageRequest(dropPartitionSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.unreferencedRowsInTable(PATH_TABLE) == 2);
    List<EntityHousekeepingPath> unreferencedPaths = mySqlTestUtils.getUnreferencedPaths();

    assertUnreferencedPathFields(unreferencedPaths.get(0));
    assertUnreferencedPathFields(unreferencedPaths.get(1));
    assertThat(Set.of(unreferencedPaths.get(0).getPath(), unreferencedPaths.get(1).getPath()))
        .isEqualTo(Set.of("s3://partitionLocation", "s3://partitionLocation2"));
  }

  @Test
  void unreferencedDropTableEvent() throws SQLException, IOException {
    DropTableSqsMessage dropTableSqsMessage = new DropTableSqsMessage("s3://tableLocation", true, true);
    amazonSQS.sendMessage(sendMessageRequest(dropTableSqsMessage.getFormattedString()));
    dropTableSqsMessage.setTableLocation("s3://tableLocation2");
    amazonSQS.sendMessage(sendMessageRequest(dropTableSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.unreferencedRowsInTable(PATH_TABLE) == 2);
    List<EntityHousekeepingPath> unreferencedPaths = mySqlTestUtils.getUnreferencedPaths();

    assertUnreferencedPathFields(unreferencedPaths.get(0));
    assertUnreferencedPathFields(unreferencedPaths.get(1));
    assertThat(Set.of(unreferencedPaths.get(0).getPath(), unreferencedPaths.get(1).getPath()))
        .isEqualTo(Set.of("s3://tableLocation", "s3://tableLocation2"));
  }

  @Test
  void healthCheck() {
    CloseableHttpClient client = HttpClientBuilder.create().build();
    HttpGet request = new HttpGet(HEALTHCHECK_URI);
    HttpCoreContext context = new HttpCoreContext();
    await().atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> client.execute(request).getStatusLine().getStatusCode() == 200);
  }

  @Test
  public void prometheus() {
    CloseableHttpClient client = HttpClientBuilder.create().build();
    HttpGet request = new HttpGet(PROMETHEUS_URI);
    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> client.execute(request).getStatusLine().getStatusCode() == 200);
  }

  private void assertMetrics(boolean isExpired) {
    String pathMetric = isExpired ? SCHEDULED_EXPIRATION_METRIC : SCHEDULED_ORPHANED_METRIC;
    Set<MeterRegistry> meterRegistry = ((CompositeMeterRegistry) BeekeeperPathSchedulerApiary.meterRegistry()).getRegistries();
    assertThat(meterRegistry).hasSize(2);
    meterRegistry.forEach(registry -> {
      List<Meter> meters = registry.getMeters();
      assertThat(meters)
          .extracting("id", Meter.Id.class)
          .extracting("name")
          .contains(pathMetric);
    });
  }

  private SendMessageRequest sendMessageRequest(String payload) {
    return new SendMessageRequest(ContainerTestUtils.queueUrl(sqsContainer, QUEUE), payload);
  }

  private void assertUnreferencedPathFields(EntityHousekeepingPath savedPath) {
    assertThat(savedPath.getLifecycleType()).isEqualTo(UNREFERENCED.toString());
    assertThat(savedPath.getCleanupDelay()).isEqualTo(java.time.Duration.parse(CLEANUP_DELAY_UNREFERENCED));
    assertPathFields(savedPath);
    assertMetrics(false);
  }

  private void assertPathFields(EntityHousekeepingPath savedPath) {
    assertThat(savedPath.getDatabaseName()).isEqualTo(DATABASE);
    assertThat(savedPath.getTableName()).isEqualTo(TABLE);
    assertThat(savedPath.getHousekeepingStatus()).isEqualTo(HousekeepingStatus.SCHEDULED);
    assertThat(savedPath.getCreationTimestamp()).isAfterOrEqualTo(CREATION_TIMESTAMP);
    assertThat(savedPath.getModifiedTimestamp()).isAfterOrEqualTo(CREATION_TIMESTAMP);

    assertThat(timestampWithinRangeInclusive(
        savedPath.getCleanupTimestamp(),
        savedPath.getCreationTimestamp().plus(savedPath.getCleanupDelay()).minusSeconds(5),
        savedPath.getCreationTimestamp().plus(savedPath.getCleanupDelay()).plusSeconds(5)
    )).isTrue();

    assertThat(savedPath.getCleanupAttempts()).isEqualTo(CLEANUP_ATTEMPTS);
    assertThat(savedPath.getClientId()).isEqualTo(CLIENT_ID);
  }

  private boolean timestampWithinRangeInclusive(LocalDateTime timestamp, LocalDateTime lowerBound,
      LocalDateTime upperBound) {
    return !timestamp.isBefore(lowerBound) && !timestamp.isAfter(upperBound);
  }
}
