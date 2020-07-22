package com.expediagroup.beekeeper.integration;

import static java.time.ZoneOffset.UTC;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpCoreContext;
import org.awaitility.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;

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
import com.expediagroup.beekeeper.scheduler.apiary.BeekeeperSchedulerApiary;

public class NewBeekeeperSchedulerApiaryIntegrationTest extends BeekeeperIntegrationTestBase {

  private static final String CLEANUP_DELAY_UNREFERENCED = "P7D";
  private static final int CLEANUP_ATTEMPTS = 0;
  private static final String CLIENT_ID = "apiary-metastore-event";
  private static final LocalDateTime CREATION_TIMESTAMP = LocalDateTime.now(UTC).minus(1L, ChronoUnit.MINUTES);
  private static final String DATABASE = "some_db";
  private static final String TABLE = "some_table";
  private static final String HEALTHCHECK_URI = "http://localhost:8080/actuator/health";
  private static final String PROMETHEUS_URI = "http://localhost:8080/actuator/prometheus";

  private static final String QUEUE = "apiary-receiver-queue";
  private static final String SCHEDULED_EXPIRATION_METRIC = "paths-scheduled-expiration";
  private static final String SCHEDULED_ORPHANED_METRIC = "paths-scheduled";

  private static AmazonSQS amazonSQS;
  private static LocalStackContainer sqsContainer;

  public NewBeekeeperSchedulerApiaryIntegrationTest() throws SQLException {
    System.out.println(Clock.systemUTC().instant().toString() + " SQL done");
    sqsContainer = ContainerTestUtils.awsContainer(SQS);
    sqsContainer.start();
    System.out.println(Clock.systemUTC().instant().toString() + " SQS done");

    String queueUrl = ContainerTestUtils.queueUrl(sqsContainer, QUEUE);
    System.setProperty("properties.apiary.queue-url", queueUrl);

    amazonSQS = ContainerTestUtils.sqsClient(sqsContainer, AWS_REGION);
    amazonSQS.createQueue(QUEUE);
    System.out.println(Clock.systemUTC().instant().toString() + " DONE");
  }

  @BeforeEach
  public void beforeEach() throws SQLException {
    amazonSQS.purgeQueue(new PurgeQueueRequest(ContainerTestUtils.queueUrl(sqsContainer, QUEUE)));
    dropTables();
    executorService.execute(() -> BeekeeperSchedulerApiary.main(new String[] {}));
    await().atMost(Duration.ONE_MINUTE).until(BeekeeperSchedulerApiary::isRunning);
  }

  @AfterEach
  public void afterEach() throws InterruptedException {
    BeekeeperSchedulerApiary.stop();
    executorService.awaitTermination(5, TimeUnit.SECONDS);
  }

  @AfterAll
  public static void teardown() throws SQLException {
    closeMySQLConnectionAndDatabase();
  }

  @Test
  void unreferencedAlterTableEvent() throws SQLException, IOException {
    AlterTableSqsMessage alterTableSqsMessage = new AlterTableSqsMessage("s3://tableLocation", "s3://oldTableLocation",
        true, true);
    amazonSQS.sendMessage(sendMessageRequest(alterTableSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> getUnreferencedHousekeepingPathsRowCount() == 1);
    List<HousekeepingPath> unreferencedPaths = getUnreferencedHousekeepingPaths();
    assertUnreferencedPathFields(unreferencedPaths.get(0));
    assertThat(unreferencedPaths.get(0).getPath()).isEqualTo("s3://oldTableLocation");
  }

  @Test
  void unreferencedMultipleAlterTableEvents() throws SQLException, IOException {
    AlterTableSqsMessage alterTableSqsMessage = new AlterTableSqsMessage("s3://tableLocation", "s3://oldTableLocation",
        true, true);
    amazonSQS.sendMessage(sendMessageRequest(alterTableSqsMessage.getFormattedString()));
    alterTableSqsMessage.setTableLocation("s3://tableLocation2");
    alterTableSqsMessage.setOldTableLocation("s3://tableLocation");
    amazonSQS.sendMessage(sendMessageRequest(alterTableSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> getUnreferencedHousekeepingPathsRowCount() == 2);
    List<HousekeepingPath> unreferencedPaths = getUnreferencedHousekeepingPaths();

    assertUnreferencedPathFields(unreferencedPaths.get(0));
    assertUnreferencedPathFields(unreferencedPaths.get(1));
    assertThat(Set.of(unreferencedPaths.get(0).getPath(), unreferencedPaths.get(1).getPath()))
        .isEqualTo(Set.of("s3://oldTableLocation", "s3://tableLocation"));
  }

  @Test
  void unreferencedAlterPartitionEvent() throws SQLException, IOException {
    AlterPartitionSqsMessage alterPartitionSqsMessage = new AlterPartitionSqsMessage("s3://expiredTableLocation",
        "s3://partitionLocation", "s3://unreferencedPartitionLocation", true, true);
    amazonSQS.sendMessage(sendMessageRequest(alterPartitionSqsMessage.getFormattedString()));
    AlterPartitionSqsMessage alterPartitionSqsMessage2 = new AlterPartitionSqsMessage("s3://expiredTableLocation2",
        "s3://partitionLocation2", "s3://partitionLocation", true, true);
    amazonSQS.sendMessage(sendMessageRequest(alterPartitionSqsMessage2.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> getUnreferencedHousekeepingPathsRowCount() == 2);
    List<HousekeepingPath> unreferencedPaths = getUnreferencedHousekeepingPaths();

    assertUnreferencedPathFields(unreferencedPaths.get(0));
    assertUnreferencedPathFields(unreferencedPaths.get(1));
    assertThat(Set.of(unreferencedPaths.get(0).getPath(), unreferencedPaths.get(1).getPath()))
        .isEqualTo(Set.of("s3://unreferencedPartitionLocation", "s3://partitionLocation"));
  }

  @Test
  void unreferencedMultipleAlterPartitionEvent() throws IOException, SQLException {
    List
        .of(new AlterPartitionSqsMessage("s3://expiredTableLocation", "s3://partitionLocation",
            "s3://unreferencedPartitionLocation", true, true),
            new AlterPartitionSqsMessage("s3://expiredTableLocation2", "s3://partitionLocation2",
                "s3://partitionLocation", true, true))
        .forEach(msg -> amazonSQS.sendMessage(sendMessageRequest(msg.getFormattedString())));

    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> getUnreferencedHousekeepingPathsRowCount() == 2);
    List<HousekeepingPath> unreferencedPaths = getUnreferencedHousekeepingPaths();

    unreferencedPaths.forEach(this::assertUnreferencedPathFields);
    Set<String> unreferencedPathSet = unreferencedPaths
        .stream()
        .map(HousekeepingPath::getPath)
        .collect(Collectors.toSet());
    assertThat(unreferencedPathSet).isEqualTo(Set.of("s3://unreferencedPartitionLocation", "s3://partitionLocation"));
  }

  @Test
  void unreferencedDropPartitionEvent() throws SQLException, IOException {
    DropPartitionSqsMessage dropPartitionSqsMessage = new DropPartitionSqsMessage("s3://partitionLocation", true, true);
    amazonSQS.sendMessage(sendMessageRequest(dropPartitionSqsMessage.getFormattedString()));
    dropPartitionSqsMessage.setPartitionLocation("s3://partitionLocation2");
    amazonSQS.sendMessage(sendMessageRequest(dropPartitionSqsMessage.getFormattedString()));
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> getUnreferencedHousekeepingPathsRowCount() == 2);
    List<HousekeepingPath> unreferencedPaths = getUnreferencedHousekeepingPaths();

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
    await().atMost(TIMEOUT, TimeUnit.SECONDS).until(() -> getUnreferencedHousekeepingPathsRowCount() == 2);
    List<HousekeepingPath> unreferencedPaths = getUnreferencedHousekeepingPaths();

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

  private SendMessageRequest sendMessageRequest(String payload) {
    return new SendMessageRequest(ContainerTestUtils.queueUrl(sqsContainer, QUEUE), payload);
  }

  private void assertUnreferencedPathFields(HousekeepingPath savedPath) {
    assertThat(savedPath.getLifecycleType()).isEqualTo(UNREFERENCED.toString());
    assertThat(savedPath.getCleanupDelay()).isEqualTo(java.time.Duration.parse(CLEANUP_DELAY_UNREFERENCED));
    assertPathFields(savedPath);
    assertMetrics(false);
  }

  private void assertPathFields(HousekeepingPath savedPath) {
    assertThat(savedPath.getDatabaseName()).isEqualTo(DATABASE);
    assertThat(savedPath.getTableName()).isEqualTo(TABLE);
    assertThat(savedPath.getHousekeepingStatus()).isEqualTo(SCHEDULED);
    assertThat(savedPath.getCreationTimestamp()).isAfterOrEqualTo(CREATION_TIMESTAMP);
    assertThat(savedPath.getModifiedTimestamp()).isAfterOrEqualTo(CREATION_TIMESTAMP);

    assertThat(timestampWithinRangeInclusive(savedPath.getCleanupTimestamp(),
        savedPath.getCreationTimestamp().plus(savedPath.getCleanupDelay()).minusSeconds(5),
        savedPath.getCreationTimestamp().plus(savedPath.getCleanupDelay()).plusSeconds(5))).isTrue();

    assertThat(savedPath.getCleanupAttempts()).isEqualTo(CLEANUP_ATTEMPTS);
    assertThat(savedPath.getClientId()).isEqualTo(CLIENT_ID);
  }

  private void assertMetrics(boolean isExpired) {
    String pathMetric = isExpired ? SCHEDULED_EXPIRATION_METRIC : SCHEDULED_ORPHANED_METRIC;
    Set<MeterRegistry> meterRegistry = ((CompositeMeterRegistry) BeekeeperSchedulerApiary.meterRegistry())
        .getRegistries();
    assertThat(meterRegistry).hasSize(2);
    meterRegistry.forEach(registry -> {
      List<Meter> meters = registry.getMeters();
      assertThat(meters).extracting("id", Meter.Id.class).extracting("name").contains(pathMetric);
    });
  }

  private boolean timestampWithinRangeInclusive(
      LocalDateTime timestamp,
      LocalDateTime lowerBound,
      LocalDateTime upperBound) {
    return !timestamp.isBefore(lowerBound) && !timestamp.isAfter(upperBound);
  }
}
