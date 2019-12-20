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

import static java.time.ZoneOffset.UTC;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.expediagroup.beekeeper.core.model.LifeCycleEventType.UNREFERENCED;
import static com.expediagroup.beekeeper.core.model.LifeCycleEventType.EXPIRED;
import com.expediagroup.beekeeper.integration.model.*;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpCoreContext;
import org.awaitility.Duration;
import org.junit.Ignore;
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

class BeekeeperPathSchedulerApiaryIntegrationTest {

  private static final int TIMEOUT = 5;
  private static final String QUEUE = "apiary-receiver-queue";
  private static final String REGION = "us-west-2";
  private static final String PATH_TABLE = "path";
  private static final String FLYWAY_TABLE = "flyway_schema_history";
  private static final String AWS_ACCESS_KEY_ID = "accessKey";
  private static final String AWS_SECRET_KEY = "secretKey";

  private static final String CLEANUP_DELAY_UNREFERENCED = "P7D";
  private static final String CLEANUP_DELAY_EXPIRED = "P14D";
  private static final int CLEANUP_ATTEMPTS = 0;
  private static final String CLIENT_ID = "apiary-metastore-event";
  private static final LocalDateTime CREATION_TIMESTAMP = LocalDateTime.now(UTC)
          .minus(1L, ChronoUnit.MINUTES);
  private static final String DATABASE = "some_db";
  private static final String TABLE = "some_table";
  private static final String HEALTHCHECK_URI = "http://localhost:8080/actuator/health";

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
  void noActionsRequested() throws IOException {
    AlterPartitionSqsMessage alterPartitionSqsMessage = new AlterPartitionSqsMessage(
            "s3://expiredTableLocation",
            "s3://partitionLocation",
            "s3://unreferencedPartitionLocation",
            false, false);
    amazonSQS.sendMessage(sendMessageRequest(alterPartitionSqsMessage.getFormattedString()));
    CreateTableSqsMessage createTableSqsMessage = new CreateTableSqsMessage(
            "s3://expiredTableLocation",false,false);
    amazonSQS.sendMessage(sendMessageRequest(createTableSqsMessage.getFormattedString()));

    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.unreferencedRowsInTable(PATH_TABLE) == 0);
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.expiredRowsInTable(PATH_TABLE) == 0);
  }

  // TODO: Consider removing this test as it doesn't seem like a feasible case
  @Test @Ignore
  void createTableRaceCondition() throws SQLException, IOException {
    CreateTableSqsMessage createTableSqsMessage = new CreateTableSqsMessage(
            "s3://expiredTableLocation",true,true);
    amazonSQS.sendMessage(sendMessageRequest(createTableSqsMessage.getFormattedString()));
    createTableSqsMessage.setTableLocation("s3://expiredTableLocation2");
    amazonSQS.sendMessage(sendMessageRequest(createTableSqsMessage.getFormattedString()));
    createTableSqsMessage.setTableLocation("s3://expiredTableLocation3");
    amazonSQS.sendMessage(sendMessageRequest(createTableSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.expiredRowsInTable(PATH_TABLE) == 1);
    List<EntityHousekeepingPath> expiredPaths = mySqlTestUtils.getExpiredPaths();

    // todo: would this ever happen? same table created multiple times in quick succession for race case?
    assertExpiredPathFields(expiredPaths.get(0));
    assertThat(expiredPaths.get(0).getPath()).isEqualTo("s3://expiredTableLocation3");
  }

  private Callable<Boolean> createTableRaceConditionLambda(Long equalValue) throws SQLException {
    List<EntityHousekeepingPath> expiredPaths = mySqlTestUtils.getExpiredPaths();
    Long retVal = Long.valueOf(0);
    try {
      retVal = expiredPaths.get(0).getId();
    } catch (Exception e) {
      retVal = Long.valueOf(-1);
    } finally {
      final Long ret = retVal;
      return () -> ret == equalValue;
    }
  }

  @Test
  void createTableExpiredOnly() throws SQLException, IOException {
    CreateTableSqsMessage createTableSqsMessage = new CreateTableSqsMessage(
            "s3://expiredTableLocation",false,true);
    amazonSQS.sendMessage(sendMessageRequest(createTableSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.expiredRowsInTable(PATH_TABLE) == 1);
    List<EntityHousekeepingPath> expiredPaths = mySqlTestUtils.getExpiredPaths();

    assertExpiredPathFields(expiredPaths.get(0));
    assertThat(expiredPaths.get(0).getPath()).isEqualTo("s3://expiredTableLocation");
  }

  @Test
  void createTableUnreferencedOnly() throws IOException {
    CreateTableSqsMessage createTableSqsMessage = new CreateTableSqsMessage(
            "s3://expiredTableLocation",true,false);
    amazonSQS.sendMessage(sendMessageRequest(createTableSqsMessage.getFormattedString()));
    amazonSQS.sendMessage(sendMessageRequest(createTableSqsMessage.getFormattedString()));
    amazonSQS.sendMessage(sendMessageRequest(createTableSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.expiredRowsInTable(PATH_TABLE) == 0);
  }

  @Test
  void addPartitionUnreferencedAndExpired() throws SQLException, IOException {
    AddPartitionSqsMessage addPartitionSqsMessage = new AddPartitionSqsMessage(
            "s3://partitionLocation", true, true);
    amazonSQS.sendMessage(sendMessageRequest(addPartitionSqsMessage.getFormattedString()));
    addPartitionSqsMessage.setPartitionLocation("s3://partitionLocation2");
    amazonSQS.sendMessage(sendMessageRequest(addPartitionSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.expiredRowsInTable(PATH_TABLE) == 1);
    List<EntityHousekeepingPath> expiredPaths = mySqlTestUtils.getExpiredPaths();

    assertExpiredPathFields(expiredPaths.get(0));
    assertThat(expiredPaths.get(0).getPath()).isEqualTo("s3://table_location");
  }

  @Test
  void alterTableUnreferencedAndExpired() throws SQLException, IOException {
    AlterTableSqsMessage alterTableSqsMessage = new AlterTableSqsMessage(
            "s3://tableLocation",
            "s3://oldTableLocation",
            true, true);
    amazonSQS.sendMessage(sendMessageRequest(alterTableSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.unreferencedRowsInTable(PATH_TABLE) == 1);
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.expiredRowsInTable(PATH_TABLE) == 1);
    List<EntityHousekeepingPath> unreferencedPaths = mySqlTestUtils.getUnreferencedPaths();
    List<EntityHousekeepingPath> expiredPaths = mySqlTestUtils.getExpiredPaths();

    assertUnreferencedPathFields(unreferencedPaths.get(0));
    assertThat(unreferencedPaths.get(0).getPath()).isEqualTo("s3://oldTableLocation");

    assertExpiredPathFields(expiredPaths.get(0));
    assertThat(expiredPaths.get(0).getPath()).isEqualTo("s3://tableLocation");
  }

  @Test
  void alterTableUnreferencedAndExpiredMultiple() throws SQLException, IOException {
    AlterTableSqsMessage alterTableSqsMessage = new AlterTableSqsMessage(
            "s3://tableLocation",
            "s3://oldTableLocation",
            true, true);
    amazonSQS.sendMessage(sendMessageRequest(alterTableSqsMessage.getFormattedString()));
    alterTableSqsMessage.setTableLocation("s3://tableLocation2");
    alterTableSqsMessage.setOldTableLocation("s3://tableLocation");
    amazonSQS.sendMessage(sendMessageRequest(alterTableSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.unreferencedRowsInTable(PATH_TABLE) == 2);
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.expiredRowsInTable(PATH_TABLE) == 1);
    List<EntityHousekeepingPath> unreferencedPaths = mySqlTestUtils.getUnreferencedPaths();
    List<EntityHousekeepingPath> expiredPaths = mySqlTestUtils.getExpiredPaths();

    assertUnreferencedPathFields(unreferencedPaths.get(0));
    assertUnreferencedPathFields(unreferencedPaths.get(1));
    assertThat(Set.of(unreferencedPaths.get(0).getPath(),unreferencedPaths.get(1).getPath()))
            .isEqualTo(Set.of("s3://oldTableLocation","s3://tableLocation"));

    assertExpiredPathFields(expiredPaths.get(0));
    assertThat(expiredPaths.get(0).getPath()).isEqualTo("s3://tableLocation2");
  }

  @Test
  void alterTableUnreferencedAndExpiredSameS3Location() throws IOException, SQLException {
    AlterTableSqsMessage alterTableSqsMessage = new AlterTableSqsMessage(
            "s3://tableLocation",
            "s3://tableLocation",
            true, true);
    amazonSQS.sendMessage(sendMessageRequest(alterTableSqsMessage.getFormattedString()));
    amazonSQS.sendMessage(sendMessageRequest(alterTableSqsMessage.getFormattedString()));
    amazonSQS.sendMessage(sendMessageRequest(alterTableSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.unreferencedRowsInTable(PATH_TABLE) == 0);
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.expiredRowsInTable(PATH_TABLE) == 1);
    List<EntityHousekeepingPath> expiredPaths = mySqlTestUtils.getExpiredPaths();

    assertExpiredPathFields(expiredPaths.get(0));
    assertThat(expiredPaths.get(0).getPath()).isEqualTo("s3://tableLocation");
  }


  @Test
  void alterPartitionUnreferencedAndExpiredSameS3Location() throws IOException, SQLException {
    AlterPartitionSqsMessage alterPartitionSqsMessage = new AlterPartitionSqsMessage(
            "s3://tableLocation",
            "s3://samePartitionLocation",
            "s3://samePartitionLocation",
            true, true);
    amazonSQS.sendMessage(sendMessageRequest(alterPartitionSqsMessage.getFormattedString()));
    amazonSQS.sendMessage(sendMessageRequest(alterPartitionSqsMessage.getFormattedString()));
    amazonSQS.sendMessage(sendMessageRequest(alterPartitionSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.unreferencedRowsInTable(PATH_TABLE) == 0);
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.expiredRowsInTable(PATH_TABLE) == 1);
    List<EntityHousekeepingPath> expiredPaths = mySqlTestUtils.getExpiredPaths();

    assertExpiredPathFields(expiredPaths.get(0));
    assertThat(expiredPaths.get(0).getPath()).isEqualTo("s3://tableLocation");
  }

  @Test
  void alterPartitionUnreferencedAndExpired() throws IOException, SQLException {
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
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.expiredRowsInTable(PATH_TABLE) == 1);
    List<EntityHousekeepingPath> unreferencedPaths = mySqlTestUtils.getUnreferencedPaths();
    List<EntityHousekeepingPath> expiredPaths = mySqlTestUtils.getExpiredPaths();

    assertUnreferencedPathFields(unreferencedPaths.get(0));
    assertUnreferencedPathFields(unreferencedPaths.get(1));
    assertThat(Set.of(
      unreferencedPaths.get(0).getPath(),
      unreferencedPaths.get(1).getPath())).isEqualTo(
               Set.of(
      "s3://unreferencedPartitionLocation",
      "s3://partitionLocation"));

    assertExpiredPathFields(expiredPaths.get(0));
    assertThat(expiredPaths.get(0).getPath()).isEqualTo("s3://expiredTableLocation2");
  }

  @Test
  void alterPartitionExpiredOnly() throws SQLException, IOException {
    AlterPartitionSqsMessage alterPartitionSqsMessage = new AlterPartitionSqsMessage(
            "s3://expiredTableLocation",
            "s3://partitionLocation",
            "s3://unreferencedPartitionLocation",
            false, true);
    amazonSQS.sendMessage(sendMessageRequest(alterPartitionSqsMessage.getFormattedString()));
    AlterPartitionSqsMessage alterPartitionSqsMessage2 = new AlterPartitionSqsMessage(
            "s3://expiredTableLocation2",
            "s3://partitionLocation2",
            "s3://partitionLocation",
            false, true);
    amazonSQS.sendMessage(sendMessageRequest(alterPartitionSqsMessage2.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.expiredRowsInTable(PATH_TABLE) == 1);
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.unreferencedRowsInTable(PATH_TABLE) == 0);
    List<EntityHousekeepingPath> expiredPaths = mySqlTestUtils.getExpiredPaths();

    assertExpiredPathFields(expiredPaths.get(0));
    assertThat(expiredPaths.get(0).getPath()).isEqualTo("s3://expiredTableLocation2");
  }

  @Test
  void alterPartitionUnreferencedOnly() throws SQLException, IOException {
    AlterPartitionSqsMessage alterPartitionSqsMessage = new AlterPartitionSqsMessage(
            "s3://expiredTableLocation",
            "s3://partitionLocation",
            "s3://unreferencedPartitionLocation",
            true, false);
    amazonSQS.sendMessage(sendMessageRequest(alterPartitionSqsMessage.getFormattedString()));
    AlterPartitionSqsMessage alterPartitionSqsMessage2 = new AlterPartitionSqsMessage(
            "s3://expiredTableLocation2",
            "s3://partitionLocation2",
            "s3://partitionLocation",
            true, false);
    amazonSQS.sendMessage(sendMessageRequest(alterPartitionSqsMessage2.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.unreferencedRowsInTable(PATH_TABLE) == 2);
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.expiredRowsInTable(PATH_TABLE) == 0);
    List<EntityHousekeepingPath> unreferencedPaths = mySqlTestUtils.getUnreferencedPaths();

    assertUnreferencedPathFields(unreferencedPaths.get(0));
    assertUnreferencedPathFields(unreferencedPaths.get(1));
    assertThat(Set.of(unreferencedPaths.get(0).getPath(),unreferencedPaths.get(1).getPath()))
            .isEqualTo(Set.of("s3://unreferencedPartitionLocation","s3://partitionLocation"));
  }

  @Test
  void dropPartitionUnreferencedAndExpired() throws SQLException, IOException {
    DropPartitionSqsMessage dropPartitionSqsMessage = new DropPartitionSqsMessage(
            "s3://partitionLocation",
            true, true);
    amazonSQS.sendMessage(sendMessageRequest(dropPartitionSqsMessage.getFormattedString()));
    dropPartitionSqsMessage.setPartitionLocation("s3://partitionLocation2");
    amazonSQS.sendMessage(sendMessageRequest(dropPartitionSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.unreferencedRowsInTable(PATH_TABLE) == 2);
    List<EntityHousekeepingPath> unreferencedPaths = mySqlTestUtils.getUnreferencedPaths();

    assertUnreferencedPathFields(unreferencedPaths.get(0));
    assertUnreferencedPathFields(unreferencedPaths.get(1));
    assertThat(Set.of(unreferencedPaths.get(0).getPath(),unreferencedPaths.get(1).getPath()))
            .isEqualTo(Set.of("s3://partitionLocation","s3://partitionLocation2"));
  }

  @Test
  void dropPartitionUnreferencedOnly() throws SQLException, IOException {
    DropPartitionSqsMessage dropPartitionSqsMessage = new DropPartitionSqsMessage(
            "s3://partitionLocation",
            true, false);
    amazonSQS.sendMessage(sendMessageRequest(dropPartitionSqsMessage.getFormattedString()));
    dropPartitionSqsMessage.setPartitionLocation("s3://partitionLocation2");
    amazonSQS.sendMessage(sendMessageRequest(dropPartitionSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.unreferencedRowsInTable(PATH_TABLE) == 2);
    List<EntityHousekeepingPath> unreferencedPaths = mySqlTestUtils.getUnreferencedPaths();

    assertUnreferencedPathFields(unreferencedPaths.get(0));
    assertUnreferencedPathFields(unreferencedPaths.get(1));
    assertThat(Set.of(unreferencedPaths.get(0).getPath(),unreferencedPaths.get(1).getPath()))
            .isEqualTo(Set.of("s3://partitionLocation","s3://partitionLocation2"));
  }

  @Test
  void dropPartitionExpiredOnly() throws IOException {
    DropPartitionSqsMessage dropPartitionSqsMessage = new DropPartitionSqsMessage(
            "s3://partitionLocation",
            false, true);
    amazonSQS.sendMessage(sendMessageRequest(dropPartitionSqsMessage.getFormattedString()));
    amazonSQS.sendMessage(sendMessageRequest(dropPartitionSqsMessage.getFormattedString()));
    amazonSQS.sendMessage(sendMessageRequest(dropPartitionSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.unreferencedRowsInTable(PATH_TABLE) == 0);
  }

  @Test
  void dropTableUnreferencedAndExpired() throws SQLException, IOException {
    DropTableSqsMessage dropTableSqsMessage = new DropTableSqsMessage(
            "s3://tableLocation",
            true, true);
    amazonSQS.sendMessage(sendMessageRequest(dropTableSqsMessage.getFormattedString()));
    dropTableSqsMessage.setTableLocation("s3://tableLocation2");
    amazonSQS.sendMessage(sendMessageRequest(dropTableSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.unreferencedRowsInTable(PATH_TABLE) == 2);
    List<EntityHousekeepingPath> unreferencedPaths = mySqlTestUtils.getUnreferencedPaths();

    assertUnreferencedPathFields(unreferencedPaths.get(0));
    assertUnreferencedPathFields(unreferencedPaths.get(1));
    assertThat(Set.of(unreferencedPaths.get(0).getPath(),unreferencedPaths.get(1).getPath()))
            .isEqualTo(Set.of("s3://tableLocation","s3://tableLocation2"));
  }

  @Test
  void dropTableUnreferencedOnly() throws SQLException, IOException {
    DropTableSqsMessage dropTableSqsMessage = new DropTableSqsMessage(
            "s3://tableLocation",
            true, false);
    amazonSQS.sendMessage(sendMessageRequest(dropTableSqsMessage.getFormattedString()));
    dropTableSqsMessage.setTableLocation("s3://tableLocation2");
    amazonSQS.sendMessage(sendMessageRequest(dropTableSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.unreferencedRowsInTable(PATH_TABLE) == 2);
    List<EntityHousekeepingPath> unreferencedPaths = mySqlTestUtils.getUnreferencedPaths();

    assertUnreferencedPathFields(unreferencedPaths.get(0));
    assertUnreferencedPathFields(unreferencedPaths.get(1));
    assertThat(Set.of(unreferencedPaths.get(0).getPath(),unreferencedPaths.get(1).getPath()))
            .isEqualTo(Set.of("s3://tableLocation","s3://tableLocation2"));
  }

  @Test
  void dropTableExpiredOnly() throws IOException {
    DropTableSqsMessage dropTableSqsMessage = new DropTableSqsMessage(
            "s3://tableLocation",
            false, true);
    amazonSQS.sendMessage(sendMessageRequest(dropTableSqsMessage.getFormattedString()));
    amazonSQS.sendMessage(sendMessageRequest(dropTableSqsMessage.getFormattedString()));
    amazonSQS.sendMessage(sendMessageRequest(dropTableSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> mySqlTestUtils.unreferencedRowsInTable(PATH_TABLE) == 0);
  }

  @Test
  void healthCheck() {
    CloseableHttpClient client = HttpClientBuilder.create().build();
    HttpGet request = new HttpGet(HEALTHCHECK_URI);
    HttpCoreContext context = new HttpCoreContext();
    await().atMost(TIMEOUT, TimeUnit.SECONDS)
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

  private void assertExpiredPathFields(EntityHousekeepingPath savedPath) {
    assertThat(savedPath.getLifecycleType()).isEqualTo(EXPIRED.toString());
    assertThat(savedPath.getCleanupDelay()).isEqualTo(java.time.Duration.parse(CLEANUP_DELAY_EXPIRED));
    assertPathFields(savedPath);
    assertMetrics();
  }

  private void assertUnreferencedPathFields(EntityHousekeepingPath savedPath) {
    assertThat(savedPath.getLifecycleType()).isEqualTo(UNREFERENCED.toString());
    assertThat(savedPath.getCleanupDelay()).isEqualTo(java.time.Duration.parse(CLEANUP_DELAY_UNREFERENCED));
    assertPathFields(savedPath);
    assertMetrics();
  }

  private void assertPathFields(EntityHousekeepingPath savedPath) {
    assertThat(savedPath.getDatabaseName()).isEqualTo(DATABASE);
    assertThat(savedPath.getTableName()).isEqualTo(TABLE);
    assertThat(savedPath.getPathStatus()).isEqualTo(PathStatus.SCHEDULED);
    assertThat(savedPath.getCreationTimestamp()).isAfterOrEqualTo(CREATION_TIMESTAMP);
    assertThat(savedPath.getModifiedTimestamp()).isAfterOrEqualTo(CREATION_TIMESTAMP);
    assertThat(savedPath.getCleanupTimestamp()).isEqualTo(savedPath.getCreationTimestamp()
            .plus(savedPath.getCleanupDelay()));
    assertThat(savedPath.getCleanupAttempts()).isEqualTo(CLEANUP_ATTEMPTS);
    assertThat(savedPath.getClientId()).isEqualTo(CLIENT_ID);
  }
}
