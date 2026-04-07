/**
 * Copyright (C) 2019-2026 Expedia, Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.expediagroup.beekeeper.integration;

import static java.util.Collections.emptyMap;

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
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
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

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.PeriodDuration;
import com.expediagroup.beekeeper.core.model.history.BeekeeperHistory;
import com.expediagroup.beekeeper.integration.model.AlterPartitionSqsMessage;
import com.expediagroup.beekeeper.integration.model.AlterTableSqsMessage;
import com.expediagroup.beekeeper.integration.model.DropPartitionSqsMessage;
import com.expediagroup.beekeeper.integration.model.DropTableSqsMessage;
import com.expediagroup.beekeeper.integration.utils.ContainerTestUtils;
import com.expediagroup.beekeeper.scheduler.apiary.BeekeeperSchedulerApiary;

import com.hotels.beeju.extensions.ThriftHiveMetaStoreJUnitExtension;

@Testcontainers
public class BeekeeperUnreferencedPathSchedulerApiaryIntegrationTest extends BeekeeperIntegrationTestBase {

  protected static final int TIMEOUT = 30;
  protected static final String APIARY_QUEUE_URL_PROPERTY = "properties.apiary.queue-url";
  protected static final String METASTORE_URI_PROPERTY = "properties.metastore-uri";

  protected static final String QUEUE = "apiary-receiver-queue";
  protected static final String SCHEDULED_ORPHANED_METRIC = "paths-scheduled";
  protected static final String HEALTHCHECK_URI = "http://localhost:8080/actuator/health";
  protected static final String PROMETHEUS_URI = "http://localhost:8080/actuator/prometheus";

  @Container
  protected static final LocalStackContainer SQS_CONTAINER = ContainerTestUtils.awsContainer(SQS);
  protected static AmazonSQS amazonSQS;
  protected static String ACTUAL_QUEUE_URL;

  @RegisterExtension
  protected ThriftHiveMetaStoreJUnitExtension thriftHiveMetaStore = new ThriftHiveMetaStoreJUnitExtension(
      DATABASE_NAME_VALUE, emptyMap());

  protected static final String SQS_ENDPOINT_PROPERTY = "properties.sqs.endpoint";
  protected static final String SQS_REGION_PROPERTY = "properties.sqs.region";

  @BeforeAll
  public static void init() {
    amazonSQS = ContainerTestUtils.sqsClient(SQS_CONTAINER, AWS_REGION);
    ACTUAL_QUEUE_URL = ContainerTestUtils.queueUrl(SQS_CONTAINER, QUEUE);
    amazonSQS.createQueue(QUEUE);
    System.setProperty(APIARY_QUEUE_URL_PROPERTY, ACTUAL_QUEUE_URL);
    System.setProperty(SQS_ENDPOINT_PROPERTY, ContainerTestUtils.awsServiceEndpoint(SQS_CONTAINER, SQS));
    System.setProperty(SQS_REGION_PROPERTY, AWS_REGION);
  }

  @AfterAll
  public static void teardown() {
    System.clearProperty(APIARY_QUEUE_URL_PROPERTY);
    System.clearProperty(SQS_ENDPOINT_PROPERTY);
    System.clearProperty(SQS_REGION_PROPERTY);

    amazonSQS.shutdown();
  }

  @BeforeEach
  public void setup() {
    System.setProperty(METASTORE_URI_PROPERTY, thriftHiveMetaStore.getThriftConnectionUri());

    amazonSQS.purgeQueue(new PurgeQueueRequest(ACTUAL_QUEUE_URL));
    executorService.execute(() -> BeekeeperSchedulerApiary.main(new String[] {}));
    await().atMost(Duration.ofMinutes(1)).until(BeekeeperSchedulerApiary::isRunning);
  }

  @AfterEach
  public void stop() throws InterruptedException {
    BeekeeperSchedulerApiary.stop();
    executorService.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test
  public void unreferencedAlterTableEvent() throws SQLException, IOException, URISyntaxException {
    AlterTableSqsMessage alterTableSqsMessage = new AlterTableSqsMessage("s3://bucket/tableLocation",
        "s3://bucket/oldTableLocation", true, true);
    amazonSQS.sendMessage(sendMessageRequest(alterTableSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> getUnreferencedPathsRowCount() == 1);

    List<HousekeepingPath> unreferencedPaths = getUnreferencedPaths();
    assertUnreferencedPath(unreferencedPaths.get(0), "s3://bucket/oldTableLocation");
  }

  @Test
  public void unreferencedMultipleAlterTableEvents() throws SQLException, IOException, URISyntaxException {
    AlterTableSqsMessage alterTableSqsMessage = new AlterTableSqsMessage("s3://bucket/tableLocation",
        "s3://bucket/oldTableLocation", true, true);
    amazonSQS.sendMessage(sendMessageRequest(alterTableSqsMessage.getFormattedString()));
    alterTableSqsMessage.setTableLocation("s3://bucket/tableLocation2");
    alterTableSqsMessage.setOldTableLocation("s3://bucket/tableLocation");
    amazonSQS.sendMessage(sendMessageRequest(alterTableSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> getUnreferencedPathsRowCount() == 2);

    List<HousekeepingPath> unreferencedPaths = getUnreferencedPaths();
    assertUnreferencedPath(unreferencedPaths.get(0), "s3://bucket/oldTableLocation");
    assertUnreferencedPath(unreferencedPaths.get(1), "s3://bucket/tableLocation");
  }

  @Test
  public void unreferencedAlterPartitionEvent() throws SQLException, IOException, URISyntaxException {
    AlterPartitionSqsMessage alterPartitionSqsMessage = new AlterPartitionSqsMessage(
        "s3://bucket/table/expiredTableLocation", "s3://bucket/table/partitionLocation",
        "s3://bucket/table/unreferencedPartitionLocation", true, true);
    amazonSQS.sendMessage(sendMessageRequest(alterPartitionSqsMessage.getFormattedString()));
    alterPartitionSqsMessage.setTableLocation("s3://bucket/table/expiredTableLocation2");
    alterPartitionSqsMessage.setPartitionLocation("s3://bucket/table/partitionLocation2");
    alterPartitionSqsMessage.setOldPartitionLocation("s3://bucket/table/partitionLocation");
    amazonSQS.sendMessage(sendMessageRequest(alterPartitionSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> getUnreferencedPathsRowCount() == 2);

    List<HousekeepingPath> unreferencedPaths = getUnreferencedPaths();
    assertUnreferencedPath(unreferencedPaths.get(0), "s3://bucket/table/partitionLocation");
    assertUnreferencedPath(unreferencedPaths.get(1), "s3://bucket/table/unreferencedPartitionLocation");
  }

  @Test
  public void unreferencedMultipleAlterPartitionEvent() throws IOException, SQLException, URISyntaxException {
    List
        .of(new AlterPartitionSqsMessage("s3://bucket/table/expiredTableLocation",
                "s3://bucket/table/partitionLocation", "s3://bucket/table/unreferencedPartitionLocation", true, true),
            new AlterPartitionSqsMessage("s3://bucket/table/expiredTableLocation2",
                "s3://bucket/table/partitionLocation2", "s3://bucket/table/partitionLocation", true, true))
        .forEach(msg -> amazonSQS.sendMessage(sendMessageRequest(msg.getFormattedString())));

    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> getUnreferencedPathsRowCount() == 2);

    List<HousekeepingPath> unreferencedPaths = getUnreferencedPaths();
    assertUnreferencedPath(unreferencedPaths.get(0), "s3://bucket/table/partitionLocation");
    assertUnreferencedPath(unreferencedPaths.get(1), "s3://bucket/table/unreferencedPartitionLocation");
  }

  @Test
  public void unreferencedDropPartitionEvent() throws SQLException, IOException, URISyntaxException {
    DropPartitionSqsMessage dropPartitionSqsMessage = new DropPartitionSqsMessage("s3://bucket/table/partitionLocation",
        true, true);
    amazonSQS.sendMessage(sendMessageRequest(dropPartitionSqsMessage.getFormattedString()));
    dropPartitionSqsMessage.setPartitionLocation("s3://bucket/table/partitionLocation2");
    amazonSQS.sendMessage(sendMessageRequest(dropPartitionSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> getUnreferencedPathsRowCount() == 2);

    List<HousekeepingPath> unreferencedPaths = getUnreferencedPaths();
    assertUnreferencedPath(unreferencedPaths.get(0), "s3://bucket/table/partitionLocation");
    assertUnreferencedPath(unreferencedPaths.get(1), "s3://bucket/table/partitionLocation2");
  }

  @Test
  public void unreferencedDropTableEvent() throws SQLException, IOException, URISyntaxException {
    DropTableSqsMessage dropTableSqsMessage = new DropTableSqsMessage("s3://bucket/tableLocation", true, true);
    amazonSQS.sendMessage(sendMessageRequest(dropTableSqsMessage.getFormattedString()));
    dropTableSqsMessage.setTableLocation("s3://bucket/tableLocation2");
    amazonSQS.sendMessage(sendMessageRequest(dropTableSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> getUnreferencedPathsRowCount() == 2);

    List<HousekeepingPath> unreferencedPaths = getUnreferencedPaths();
    assertUnreferencedPath(unreferencedPaths.get(0), "s3://bucket/tableLocation");
    assertUnreferencedPath(unreferencedPaths.get(1), "s3://bucket/tableLocation2");
  }

  @Test
  public void testEventAddedToHistoryTable() throws IOException, URISyntaxException, SQLException {
    AlterTableSqsMessage alterTableSqsMessage = new AlterTableSqsMessage("s3://bucket/tableLocation",
        "s3://bucket/oldTableLocation", true, true);
    amazonSQS.sendMessage(sendMessageRequest(alterTableSqsMessage.getFormattedString()));

    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> getBeekeeperHistoryRowCount(UNREFERENCED) == 1);

    List<BeekeeperHistory> beekeeperHistory = getBeekeeperHistory(UNREFERENCED);
    BeekeeperHistory history = beekeeperHistory.get(0);
    assertThat(history.getDatabaseName()).isEqualTo(DATABASE_NAME_VALUE);
    assertThat(history.getTableName()).isEqualTo(TABLE_NAME_VALUE);
    assertThat(history.getLifecycleType()).isEqualTo(UNREFERENCED.toString());
    assertThat(history.getHousekeepingStatus()).isEqualTo(SCHEDULED.name());
  }

  @Test
  public void healthCheck() {
    CloseableHttpClient client = HttpClientBuilder.create().build();
    HttpGet request = new HttpGet(HEALTHCHECK_URI);
    await()
        .atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> client.execute(request).getStatusLine().getStatusCode() == 200);
  }

  @Test
  public void prometheus() {
    CloseableHttpClient client = HttpClientBuilder.create().build();
    HttpGet request = new HttpGet(PROMETHEUS_URI);
    await().atMost(30, TimeUnit.SECONDS).until(() -> client.execute(request).getStatusLine().getStatusCode() == 200);
  }

  protected SendMessageRequest sendMessageRequest(String payload) {
    return new SendMessageRequest(ACTUAL_QUEUE_URL, payload);
  }

  protected void assertUnreferencedPath(HousekeepingPath actual, String expectedPath) {
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
    assertThat(actual.getCleanupDelay()).isEqualTo(PeriodDuration.parse(SHORT_CLEANUP_DELAY_VALUE));
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
