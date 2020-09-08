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
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;
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

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.integration.model.AddPartitionSqsMessage;
import com.expediagroup.beekeeper.integration.model.AlterPartitionSqsMessage;
import com.expediagroup.beekeeper.integration.model.AlterTableSqsMessage;
import com.expediagroup.beekeeper.integration.model.CreateTableSqsMessage;
import com.expediagroup.beekeeper.integration.utils.ContainerTestUtils;
import com.expediagroup.beekeeper.scheduler.apiary.BeekeeperSchedulerApiary;

@Testcontainers
public class BeekeeperExpiredMetadataSchedulerApiaryIntegrationTest extends BeekeeperIntegrationTestBase {

  private static final int TIMEOUT = 30;
  private static final String APIARY_QUEUE_URL_PROPERTY = "properties.apiary.queue-url";

  private static final String QUEUE = "apiary-receiver-queue";
  private static final String SCHEDULED_EXPIRED_METRIC = "metadata-scheduled";
  private static final String HEALTHCHECK_URI = "http://localhost:8080/actuator/health";
  private static final String PROMETHEUS_URI = "http://localhost:8080/actuator/prometheus";

  private static final String PARTITION_KEYS = "{ \"event_date\": \"date\", \"event_hour\": \"smallint\"}";
  private static final String PARTITION_A_VALUES = "[ \"2020-01-01\", \"0\" ]";
  private static final String PARTITION_B_VALUES = "[ \"2020-01-01\", \"1\" ]";
  private static final String PARTITION_A_NAME = "event_date=2020-01-01/event_hour=0";
  private static final String PARTITION_B_NAME = "event_date=2020-01-01/event_hour=1";
  private static final String LOCATION_A = "s3://location-a";
  private static final String LOCATION_B = "s3://location-b";

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
  public void expiredMetadataCreateTableEvent() throws SQLException, IOException, URISyntaxException {
    CreateTableSqsMessage createTableSqsMessage = new CreateTableSqsMessage(LOCATION_A, true);
    amazonSQS.sendMessage(sendMessageRequest(createTableSqsMessage.getFormattedString()));

    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> getExpiredMetadataRowCount() == 1);

    List<HousekeepingMetadata> expiredMetadata = getExpiredMetadata();
    assertExpiredMetadata(expiredMetadata.get(0), LOCATION_A, null);
  }

  @Test
  public void expiredMetadataAlterTableEvent() throws SQLException, IOException, URISyntaxException {
    insertExpiredMetadata(LOCATION_A + "-old", null);

    AlterTableSqsMessage alterTableSqsMessage = new AlterTableSqsMessage(LOCATION_A, true);
    amazonSQS.sendMessage(sendMessageRequest(alterTableSqsMessage.getFormattedString()));

    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> getUpdatedExpiredMetadataRowCount() == 1);

    List<HousekeepingMetadata> expiredMetadata = getExpiredMetadata();
    assertExpiredMetadata(expiredMetadata.get(0), LOCATION_A, null);
  }

  @Test
  public void expiredMetadataAddPartitionEvent() throws SQLException, IOException, URISyntaxException {
    AddPartitionSqsMessage addPartitionSqsMessage = new AddPartitionSqsMessage(LOCATION_A, PARTITION_KEYS,
        PARTITION_A_VALUES, true);
    amazonSQS.sendMessage(sendMessageRequest(addPartitionSqsMessage.getFormattedString()));

    insertExpiredMetadata("s3://location", null);

    await().atMost(60, TimeUnit.SECONDS).until(() -> getExpiredMetadataRowCount() == 2);

    List<HousekeepingMetadata> expiredMetadata = getExpiredMetadata();
    assertExpiredMetadata(expiredMetadata.get(1), LOCATION_A, PARTITION_A_NAME);
  }

  @Test
  public void expiredMetadataMultipleAddPartitionEvents() throws SQLException, IOException, URISyntaxException {
    AddPartitionSqsMessage addPartitionSqsMessage = new AddPartitionSqsMessage(LOCATION_A, PARTITION_KEYS,
        PARTITION_A_VALUES, true);
    AddPartitionSqsMessage addPartitionSqsMessage2 = new AddPartitionSqsMessage(LOCATION_B, PARTITION_KEYS,
        PARTITION_B_VALUES, true);
    amazonSQS.sendMessage(sendMessageRequest(addPartitionSqsMessage.getFormattedString()));
    amazonSQS.sendMessage(sendMessageRequest(addPartitionSqsMessage2.getFormattedString()));

    insertExpiredMetadata("s3://location", null);

    await().atMost(60, TimeUnit.SECONDS).until(() -> getExpiredMetadataRowCount() == 3);

    List<HousekeepingMetadata> expiredMetadata = getExpiredMetadata();
    assertExpiredMetadata(expiredMetadata.get(1), LOCATION_A, PARTITION_A_NAME);
    assertExpiredMetadata(expiredMetadata.get(2), LOCATION_B, PARTITION_B_NAME);
  }

  @Test
  public void expiredMetadataAlterPartitionTableEvent() throws SQLException, IOException, URISyntaxException {
    insertExpiredMetadata(LOCATION_A + "-old", PARTITION_A_NAME);

    AlterPartitionSqsMessage alterPartitionSqsMessage = new AlterPartitionSqsMessage(LOCATION_A, PARTITION_KEYS,
        PARTITION_A_VALUES, true);
    amazonSQS.sendMessage(sendMessageRequest(alterPartitionSqsMessage.getFormattedString()));

    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> getUpdatedExpiredMetadataRowCount() == 1);

    List<HousekeepingMetadata> expiredMetadata = getExpiredMetadata();
    assertExpiredMetadata(expiredMetadata.get(0), LOCATION_A, PARTITION_A_NAME);
  }

  @Test
  public void expiredMetadataMultipleAlterPartitionTableEvents() throws SQLException, IOException, URISyntaxException {
    insertExpiredMetadata(LOCATION_A + "-old", PARTITION_A_NAME);
    insertExpiredMetadata(LOCATION_B + "-old", PARTITION_B_NAME);

    AlterPartitionSqsMessage alterPartitionSqsMessage = new AlterPartitionSqsMessage(LOCATION_A, PARTITION_KEYS,
        PARTITION_A_VALUES, true);
    AlterPartitionSqsMessage alterPartitionSqsMessage2 = new AlterPartitionSqsMessage(LOCATION_B, PARTITION_KEYS,
        PARTITION_B_VALUES, true);
    amazonSQS.sendMessage(sendMessageRequest(alterPartitionSqsMessage.getFormattedString()));
    amazonSQS.sendMessage(sendMessageRequest(alterPartitionSqsMessage2.getFormattedString()));

    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> getUpdatedExpiredMetadataRowCount() == 2);

    List<HousekeepingMetadata> expiredMetadata = getExpiredMetadata();
    assertExpiredMetadata(expiredMetadata.get(0), LOCATION_A, PARTITION_A_NAME);
    assertExpiredMetadata(expiredMetadata.get(1), LOCATION_B, PARTITION_B_NAME);
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

  private void assertExpiredMetadata(HousekeepingMetadata actual, String expectedPath, String partitionName) {
    assertHousekeepingMetadata(actual, expectedPath, partitionName);
    assertMetrics();
  }

  public void assertHousekeepingMetadata(HousekeepingMetadata actual, String expectedPath,
      String expectedPartitionName) {
    assertThat(actual.getPath()).isEqualTo(expectedPath);
    assertThat(actual.getDatabaseName()).isEqualTo(DATABASE_NAME_VALUE);
    assertThat(actual.getTableName()).isEqualTo(TABLE_NAME_VALUE);
    assertThat(actual.getPartitionName()).isEqualTo(expectedPartitionName);
    assertThat(actual.getHousekeepingStatus()).isEqualTo(SCHEDULED);
    assertThat(actual.getCreationTimestamp()).isAfterOrEqualTo(CREATION_TIMESTAMP_VALUE.withNano(0));
    assertThat(actual.getModifiedTimestamp()).isAfterOrEqualTo(CREATION_TIMESTAMP_VALUE.withNano(0));
    assertThat(actual.getCleanupTimestamp()).isEqualTo(actual.getCreationTimestamp().plus(actual.getCleanupDelay()));
    assertThat(actual.getCleanupDelay()).isEqualTo(java.time.Duration.parse(SHORT_CLEANUP_DELAY_VALUE));
    assertThat(actual.getCleanupAttempts()).isEqualTo(CLEANUP_ATTEMPTS_VALUE);
    assertThat(actual.getClientId()).isEqualTo(CLIENT_ID_VALUE);
    assertThat(actual.getLifecycleType()).isEqualTo(EXPIRED.toString());
  }

  public void assertMetrics() {
    Set<MeterRegistry> meterRegistry = ((CompositeMeterRegistry) BeekeeperSchedulerApiary.meterRegistry())
        .getRegistries();
    assertThat(meterRegistry).hasSize(2);
    meterRegistry.forEach(registry -> {
      List<Meter> meters = registry.getMeters();
      assertThat(meters).extracting("id", Meter.Id.class).extracting("name").contains(SCHEDULED_EXPIRED_METRIC);
    });
  }
}
