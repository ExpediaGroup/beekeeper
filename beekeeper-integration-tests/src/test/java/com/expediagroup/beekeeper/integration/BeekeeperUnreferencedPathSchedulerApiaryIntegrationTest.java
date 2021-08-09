/**
 * Copyright (C) 2019-2021 Expedia, Inc.
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
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.AWS_REGION;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.CLEANUP_ATTEMPTS_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.CLIENT_ID_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.CREATION_TIMESTAMP_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.DATABASE_NAME_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.SHORT_CLEANUP_DELAY_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.TABLE_NAME_VALUE;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
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

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.integration.model.AlterPartitionSqsMessage;
import com.expediagroup.beekeeper.integration.model.AlterTableSqsMessage;
import com.expediagroup.beekeeper.integration.model.DropPartitionSqsMessage;
import com.expediagroup.beekeeper.integration.model.DropTableSqsMessage;
import com.expediagroup.beekeeper.integration.utils.ContainerTestUtils;
import com.expediagroup.beekeeper.scheduler.apiary.BeekeeperSchedulerApiary;

@Testcontainers
public class BeekeeperUnreferencedPathSchedulerApiaryIntegrationTest extends BeekeeperIntegrationTestBase {

  private static final int TIMEOUT = 5;
  private static final String APIARY_QUEUE_URL_PROPERTY = "properties.apiary.queue-url";

  private static final String QUEUE = "apiary-receiver-queue";
  private static final String SCHEDULED_ORPHANED_METRIC = "paths-scheduled";
  private static final String HEALTHCHECK_URI = "http://localhost:8080/actuator/health";
  private static final String PROMETHEUS_URI = "http://localhost:8080/actuator/prometheus";

  @Container
  private static final LocalStackContainer SQS_CONTAINER = ContainerTestUtils.awsContainer(SQS);
  private static AmazonSQS amazonSQS;

  @BeforeAll
  public static void init() {
    String queueUrl = ContainerTestUtils.queueUrl(SQS_CONTAINER, QUEUE);
    System.setProperty(APIARY_QUEUE_URL_PROPERTY, queueUrl);

    amazonSQS = ContainerTestUtils.sqsClient(SQS_CONTAINER, AWS_REGION);
    amazonSQS.createQueue(QUEUE);
  }

  @AfterAll
  public static void teardown() {
    System.clearProperty(APIARY_QUEUE_URL_PROPERTY);

    amazonSQS.shutdown();
  }

  @BeforeEach
  public void setup() {
    amazonSQS.purgeQueue(new PurgeQueueRequest(ContainerTestUtils.queueUrl(SQS_CONTAINER, QUEUE)));
    executorService.execute(() -> BeekeeperSchedulerApiary.main(new String[] {}));
    await().atMost(Duration.ONE_MINUTE).until(BeekeeperSchedulerApiary::isRunning);
  }

  @AfterEach
  public void stop() throws InterruptedException {
    BeekeeperSchedulerApiary.stop();
    executorService.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test
  public void unreferencedAlterTableEvent() throws SQLException, IOException, URISyntaxException {
    AlterTableSqsMessage alterTableSqsMessage = new AlterTableSqsMessage("s3://tableLocation", "s3://oldTableLocation",
        true, true);
    amazonSQS.sendMessage(sendMessageRequest(alterTableSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> getUnreferencedPathsRowCount() == 1);

    List<HousekeepingPath> unreferencedPaths = getUnreferencedPaths();
    assertUnreferencedPath(unreferencedPaths.get(0), "s3://oldTableLocation");
  }

  @Test
  public void unreferencedMultipleAlterTableEvents() throws SQLException, IOException, URISyntaxException {
    AlterTableSqsMessage alterTableSqsMessage = new AlterTableSqsMessage("s3://tableLocation", "s3://oldTableLocation",
        true, true);
    amazonSQS.sendMessage(sendMessageRequest(alterTableSqsMessage.getFormattedString()));
    alterTableSqsMessage.setTableLocation("s3://tableLocation2");
    alterTableSqsMessage.setOldTableLocation("s3://tableLocation");
    amazonSQS.sendMessage(sendMessageRequest(alterTableSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> getUnreferencedPathsRowCount() == 2);

    List<HousekeepingPath> unreferencedPaths = getUnreferencedPaths();
    assertUnreferencedPath(unreferencedPaths.get(0), "s3://oldTableLocation");
    assertUnreferencedPath(unreferencedPaths.get(1), "s3://tableLocation");
  }

  @Test
  public void unreferencedAlterPartitionEvent() throws SQLException, IOException, URISyntaxException {
    AlterPartitionSqsMessage alterPartitionSqsMessage = new AlterPartitionSqsMessage("s3://expiredTableLocation",
        "s3://partitionLocation", "s3://unreferencedPartitionLocation", true, true);
    amazonSQS.sendMessage(sendMessageRequest(alterPartitionSqsMessage.getFormattedString()));
    alterPartitionSqsMessage.setTableLocation("s3://expiredTableLocation2");
    alterPartitionSqsMessage.setPartitionLocation("s3://partitionLocation2");
    alterPartitionSqsMessage.setOldPartitionLocation("s3://partitionLocation");
    amazonSQS.sendMessage(sendMessageRequest(alterPartitionSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> getUnreferencedPathsRowCount() == 2);

    List<HousekeepingPath> unreferencedPaths = getUnreferencedPaths();
    assertUnreferencedPath(unreferencedPaths.get(0), "s3://partitionLocation");
    assertUnreferencedPath(unreferencedPaths.get(1), "s3://unreferencedPartitionLocation");
  }

  @Test
  public void unreferencedMultipleAlterPartitionEvent() throws IOException, SQLException, URISyntaxException {
    List.of(
        new AlterPartitionSqsMessage("s3://expiredTableLocation", "s3://partitionLocation",
            "s3://unreferencedPartitionLocation", true, true),
        new AlterPartitionSqsMessage("s3://expiredTableLocation2", "s3://partitionLocation2",
            "s3://partitionLocation", true, true)
    ).forEach(msg -> amazonSQS.sendMessage(sendMessageRequest(msg.getFormattedString())));

    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> getUnreferencedPathsRowCount() == 2);

    List<HousekeepingPath> unreferencedPaths = getUnreferencedPaths();
    assertUnreferencedPath(unreferencedPaths.get(0), "s3://partitionLocation");
    assertUnreferencedPath(unreferencedPaths.get(1), "s3://unreferencedPartitionLocation");
  }

  @Test
  public void unreferencedDropPartitionEvent() throws SQLException, IOException, URISyntaxException {
    DropPartitionSqsMessage dropPartitionSqsMessage = new DropPartitionSqsMessage("s3://partitionLocation", true, true);
    amazonSQS.sendMessage(sendMessageRequest(dropPartitionSqsMessage.getFormattedString()));
    dropPartitionSqsMessage.setPartitionLocation("s3://partitionLocation2");
    amazonSQS.sendMessage(sendMessageRequest(dropPartitionSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> getUnreferencedPathsRowCount() == 2);

    List<HousekeepingPath> unreferencedPaths = getUnreferencedPaths();
    assertUnreferencedPath(unreferencedPaths.get(0), "s3://partitionLocation");
    assertUnreferencedPath(unreferencedPaths.get(1), "s3://partitionLocation2");
  }

  @Test
  public void unreferencedDropTableEvent() throws SQLException, IOException, URISyntaxException {
    DropTableSqsMessage dropTableSqsMessage = new DropTableSqsMessage("s3://tableLocation", true, true);
    amazonSQS.sendMessage(sendMessageRequest(dropTableSqsMessage.getFormattedString()));
    dropTableSqsMessage.setTableLocation("s3://tableLocation2");
    amazonSQS.sendMessage(sendMessageRequest(dropTableSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> getUnreferencedPathsRowCount() == 2);

    List<HousekeepingPath> unreferencedPaths = getUnreferencedPaths();
    assertUnreferencedPath(unreferencedPaths.get(0), "s3://tableLocation");
    assertUnreferencedPath(unreferencedPaths.get(1), "s3://tableLocation2");
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
    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> client.execute(request).getStatusLine().getStatusCode() == 200);
  }

  private SendMessageRequest sendMessageRequest(String payload) {
    return new SendMessageRequest(ContainerTestUtils.queueUrl(SQS_CONTAINER, QUEUE), payload);
  }

  private void assertUnreferencedPath(HousekeepingPath actual, String expectedPath) {
    assertHousekeepingEntity(actual, expectedPath);
    assertMetrics();
  }

  public void assertHousekeepingEntity(HousekeepingPath actual, String expectedPath) {
    assertThat(actual.getPath()).isEqualTo(expectedPath);
    assertThat(actual.getDatabaseName()).isEqualTo(DATABASE_NAME_VALUE);
    assertThat(actual.getTableName()).isEqualTo(TABLE_NAME_VALUE);
    assertThat(actual.getHousekeepingStatus()).isEqualTo(SCHEDULED);
    assertThat(actual.getCreationTimestamp()).isAfterOrEqualTo(CREATION_TIMESTAMP_VALUE);
    assertThat(actual.getModifiedTimestamp()).isAfterOrEqualTo(CREATION_TIMESTAMP_VALUE);
    assertThat(actual.getCleanupTimestamp()).isEqualTo(actual.getCreationTimestamp().plus(actual.getCleanupDelay()));
    assertThat(actual.getCleanupDelay()).isEqualTo(java.time.Duration.parse(SHORT_CLEANUP_DELAY_VALUE));
    assertThat(actual.getCleanupAttempts()).isEqualTo(CLEANUP_ATTEMPTS_VALUE);
    assertThat(actual.getClientId()).isEqualTo(CLIENT_ID_VALUE);
    assertThat(actual.getLifecycleType()).isEqualTo(UNREFERENCED.toString());
  }

  public void assertMetrics() {
    Set<MeterRegistry> meterRegistry = ((CompositeMeterRegistry) BeekeeperSchedulerApiary.meterRegistry())
        .getRegistries();
    assertThat(meterRegistry).hasSize(2);
    meterRegistry.forEach(registry -> {
      List<Meter> meters = registry.getMeters();
      assertThat(meters).extracting("id", Meter.Id.class).extracting("name").contains(SCHEDULED_ORPHANED_METRIC);
    });
  }
}
