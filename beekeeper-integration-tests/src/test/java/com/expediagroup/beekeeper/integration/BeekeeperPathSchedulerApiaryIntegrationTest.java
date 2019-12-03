/**
 * Copyright (C) 2019 Expedia, Inc.
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

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
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

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import com.expediagroup.beekeeper.core.model.EntityHousekeepingPath;
import com.expediagroup.beekeeper.core.model.PathStatus;
import com.expediagroup.beekeeper.scheduler.apiary.BeekeeperPathSchedulerApiary;

public class BeekeeperPathSchedulerApiaryIntegrationTest {

  private static final String QUEUE = "apiary-receiver-queue";
  private static final String REGION = "us-west-2";
  private static final String PATH_TABLE = "path";
  private static final String FLYWAY_TABLE = "flyway_schema_history";
  private static final String AWS_ACCESS_KEY_ID = "accessKey";
  private static final String AWS_SECRET_KEY = "secretKey";

  private static final String CLEANUP_DELAY = "P7D";
  private static final int CLEANUP_ATTEMPTS = 0;
  private static final String CLIENT_ID = "apiary-metastore-event";
  private static final LocalDateTime CREATION_TIMESTAMP = LocalDateTime.now(UTC)
      .minus(1L, ChronoUnit.MINUTES);
  private static final String DATABASE = "some_db";
  private static final String TABLE = "some_table";
  private static final String HEALTHCHECK_URI = "http://localhost:8080/actuator/health";

  private static AmazonSQS amazonSQS;
  private static String alterPartitionEvent;
  private static LocalStackContainer sqsContainer;
  private static MySQLContainer mySQLContainer;
  private static MySqlTestUtils mySqlTestUtils;

  private final ExecutorService executorService = Executors.newFixedThreadPool(1);

  @BeforeAll
  public static void init() throws IOException, SQLException {
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

    alterPartitionEvent = new String(
        IOUtils.toByteArray(BeekeeperPathSchedulerApiaryIntegrationTest.class.getResource("/alter_partition.json")),
        UTF_8);
  }

  @AfterAll
  public static void teardown() throws SQLException {
    amazonSQS.shutdown();
    mySqlTestUtils.close();
    sqsContainer.stop();
    mySQLContainer.stop();
  }

  @BeforeEach
  public void setup() throws SQLException {
    amazonSQS.purgeQueue(new PurgeQueueRequest(ContainerTestUtils.queueUrl(sqsContainer, QUEUE)));
    mySqlTestUtils.dropTable(PATH_TABLE);
    mySqlTestUtils.dropTable(FLYWAY_TABLE);
    executorService.execute(() -> BeekeeperPathSchedulerApiary.main(new String[] {}));
    await().atMost(Duration.ONE_MINUTE)
        .until(BeekeeperPathSchedulerApiary::isRunning);
  }

  @AfterEach
  public void stop() throws InterruptedException {
    BeekeeperPathSchedulerApiary.stop();
    executorService.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test
  public void typicalSchedule() throws SQLException {
    String path = "s3://path";
    amazonSQS.sendMessage(sendMessageRequest(format(alterPartitionEvent, path)));
    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> mySqlTestUtils.rowsInTable(PATH_TABLE) == 1);
    List<EntityHousekeepingPath> paths = mySqlTestUtils.getPaths();
    EntityHousekeepingPath savedPath = paths.get(0);
    assertPathFields(savedPath);
    assertThat(savedPath.getPath()).isEqualTo(path);
    assertMetrics();
  }

  @Test
  public void typicalScheduleMultipleMessages() throws SQLException {
    String path1 = "s3://path1";
    String path2 = "s3://path2";
    String path3 = "s3://path3";
    amazonSQS.sendMessage(sendMessageRequest(format(alterPartitionEvent, path1)));
    amazonSQS.sendMessage(sendMessageRequest(format(alterPartitionEvent, path2)));
    amazonSQS.sendMessage(sendMessageRequest(format(alterPartitionEvent, path3)));
    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> mySqlTestUtils.rowsInTable(PATH_TABLE) == 3);
    List<EntityHousekeepingPath> paths = mySqlTestUtils.getPaths();
    assertPathFields(paths.get(0));
    assertPathFields(paths.get(1));
    assertPathFields(paths.get(2));
    assertThat(paths).extracting("path")
        .containsExactlyInAnyOrder(path1, path2, path3);
    assertMetrics();
  }

  @Test
  public void healthCheck() {
    CloseableHttpClient client = HttpClientBuilder.create().build();
    HttpGet request = new HttpGet(HEALTHCHECK_URI);
    HttpCoreContext context = new HttpCoreContext();
    await().atMost(30, TimeUnit.SECONDS)
      .until(() -> client.execute(request, context).getStatusLine().getStatusCode() == 200);
  }

  private void assertMetrics() {
    MeterRegistry meterRegistry = BeekeeperPathSchedulerApiary.meterRegistry();
    List<Meter> meters = meterRegistry.getMeters();
    assertThat(meters).extracting("id", Meter.Id.class).extracting("name")
      .contains("paths-scheduled");
  }

  private SendMessageRequest sendMessageRequest(String payload) {
    return new SendMessageRequest(ContainerTestUtils.queueUrl(sqsContainer, QUEUE), payload);
  }

  private void assertPathFields(EntityHousekeepingPath savedPath) {
    assertThat(savedPath.getDatabaseName()).isEqualTo(DATABASE);
    assertThat(savedPath.getTableName()).isEqualTo(TABLE);
    assertThat(savedPath.getPathStatus()).isEqualTo(PathStatus.SCHEDULED);
    assertThat(savedPath.getCleanupDelay()).isEqualTo(java.time.Duration.parse(CLEANUP_DELAY));
    assertThat(savedPath.getCreationTimestamp()).isAfterOrEqualTo(CREATION_TIMESTAMP);
    assertThat(savedPath.getModifiedTimestamp()).isAfterOrEqualTo(CREATION_TIMESTAMP);
    assertThat(savedPath.getCleanupTimestamp()).isEqualTo(savedPath.getCreationTimestamp()
        .plus(savedPath.getCleanupDelay()));
    assertThat(savedPath.getCleanupAttempts()).isEqualTo(CLEANUP_ATTEMPTS);
    assertThat(savedPath.getClientId()).isEqualTo(CLIENT_ID);
  }
}
