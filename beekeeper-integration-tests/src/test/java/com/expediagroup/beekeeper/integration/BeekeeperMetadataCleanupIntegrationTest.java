/**
 * Copyright (C) 2019-2024 Expedia, Inc.
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

import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.SECRET_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import static com.expediagroup.beekeeper.cleanup.monitoring.DeletedMetadataReporter.METRIC_NAME;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DELETED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DISABLED;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SKIPPED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.AWS_REGION;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.DATABASE_NAME_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.LONG_CLEANUP_DELAY_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.SHORT_CLEANUP_DELAY_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.TABLE_NAME_VALUE;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.thrift.TException;
import org.awaitility.Duration;
import org.junit.Rule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.google.common.collect.ImmutableMap;

import com.expediagroup.beekeeper.cleanup.monitoring.BytesDeletedReporter;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.core.model.history.BeekeeperHistory;
import com.expediagroup.beekeeper.integration.utils.ContainerTestUtils;
import com.expediagroup.beekeeper.integration.utils.HiveTestUtils;
import com.expediagroup.beekeeper.metadata.cleanup.BeekeeperMetadataCleanup;

import com.hotels.beeju.extensions.ThriftHiveMetaStoreJUnitExtension;

@Testcontainers
public class BeekeeperMetadataCleanupIntegrationTest extends BeekeeperIntegrationTestBase {

  protected static final int TIMEOUT = 15;
  protected static final String SCHEDULER_DELAY_MS = "5000";
  protected static final String HEALTHCHECK_URI = "http://localhost:9008/actuator/health";
  protected static final String PROMETHEUS_URI = "http://localhost:9008/actuator/prometheus";

  protected static final String SPRING_PROFILES_ACTIVE_PROPERTY = "spring.profiles.active";
  protected static final String SCHEDULER_DELAY_MS_PROPERTY = "properties.scheduler-delay-ms";
  protected static final String DRY_RUN_ENABLED_PROPERTY = "properties.dry-run-enabled";
  protected static final String AWS_S3_ENDPOINT_PROPERTY = "aws.s3.endpoint";
  protected static final String METASTORE_URI_PROPERTY = "properties.metastore-uri";
  protected static final String AWS_DISABLE_GET_VALIDATION_PROPERTY = "com.amazonaws.services.s3.disableGetObjectMD5Validation";
  protected static final String AWS_DISABLE_PUT_VALIDATION_PROPERTY = "com.amazonaws.services.s3.disablePutObjectMD5Validation";

  protected static final String S3_ACCESS_KEY = "access";
  protected static final String S3_SECRET_KEY = "secret";

  protected static final String BUCKET = "test-path-bucket";
  protected static final String TABLE_DATA = "1\tadam\tlondon\n2\tsusan\tglasgow\n";
  protected static final String PARTITIONED_TABLE_NAME = TABLE_NAME_VALUE + "_partitioned";
  protected static final String UNPARTITIONED_TABLE_NAME = TABLE_NAME_VALUE + "_unpartitioned";

  protected static final String PARTITION_NAME = "event_date=2020-01-01/event_hour=0/event_type=A";
  protected static final List<String> PARTITION_VALUES = List.of("2020-01-01", "0", "A");

  protected static final String ROOT_PATH = "s3a://" + BUCKET + "/" + DATABASE_NAME_VALUE + "/";

  protected static final String PARTITIONED_TABLE_PATH = ROOT_PATH + PARTITIONED_TABLE_NAME + "/id1";
  protected static final String PARTITION_ROOT_PATH = ROOT_PATH + "some_location/id1";
  protected static final String PARTITION_PATH = PARTITION_ROOT_PATH + "/" + PARTITION_NAME + "/file1";
  protected static final String PARTITIONED_TABLE_OBJECT_KEY = DATABASE_NAME_VALUE
      + "/"
      + PARTITIONED_TABLE_NAME
      + "/id1";

  protected static final String PARTITIONED_OBJECT_KEY = DATABASE_NAME_VALUE
      + "/some_location/id1/"
      + PARTITION_NAME
      + "/file1";

  protected static final String UNPARTITIONED_TABLE_PATH = ROOT_PATH + UNPARTITIONED_TABLE_NAME + "/id1";
  protected static final String UNPARTITIONED_OBJECT_KEY = DATABASE_NAME_VALUE
      + "/"
      + UNPARTITIONED_TABLE_NAME
      + "/id1/file1";

  @Rule
  public static final LocalStackContainer S3_CONTAINER = ContainerTestUtils.awsContainer(S3);
  static {
    S3_CONTAINER.start();
  }

  protected static AmazonS3 amazonS3;
  protected static final String S3_ENDPOINT = ContainerTestUtils.awsServiceEndpoint(S3_CONTAINER, S3);
  protected final ExecutorService executorService = Executors.newFixedThreadPool(1);

  private static Map<String, String> metastoreProperties = ImmutableMap
      .<String, String>builder()
      .put(ENDPOINT, S3_ENDPOINT)
      .put(ACCESS_KEY, S3_ACCESS_KEY)
      .put(SECRET_KEY, S3_SECRET_KEY)
      .build();

  @RegisterExtension
  public ThriftHiveMetaStoreJUnitExtension thriftHiveMetaStore = new ThriftHiveMetaStoreJUnitExtension(
      DATABASE_NAME_VALUE, metastoreProperties);

  protected HiveTestUtils hiveTestUtils;
  protected HiveMetaStoreClient metastoreClient;

  @BeforeAll
  public static void init() {
    System.setProperty(SPRING_PROFILES_ACTIVE_PROPERTY, "test");
    System.setProperty(SCHEDULER_DELAY_MS_PROPERTY, SCHEDULER_DELAY_MS);
    System.setProperty(DRY_RUN_ENABLED_PROPERTY, "false");
    System.setProperty(AWS_S3_ENDPOINT_PROPERTY, S3_ENDPOINT);
    System.setProperty(AWS_DISABLE_GET_VALIDATION_PROPERTY, "true");
    System.setProperty(AWS_DISABLE_PUT_VALIDATION_PROPERTY, "true");

    amazonS3 = ContainerTestUtils.s3Client(S3_CONTAINER, AWS_REGION);
    amazonS3.createBucket(new CreateBucketRequest(BUCKET, AWS_REGION));
  }

  @AfterAll
  public static void teardown() {
    amazonS3.shutdown();
    S3_CONTAINER.stop();

    System.clearProperty(SPRING_PROFILES_ACTIVE_PROPERTY);
    System.clearProperty(SCHEDULER_DELAY_MS_PROPERTY);
    System.clearProperty(DRY_RUN_ENABLED_PROPERTY);
    System.clearProperty(AWS_S3_ENDPOINT_PROPERTY);
    System.clearProperty(AWS_DISABLE_GET_VALIDATION_PROPERTY);
    System.clearProperty(AWS_DISABLE_PUT_VALIDATION_PROPERTY);
    System.clearProperty(METASTORE_URI_PROPERTY);
  }

  @BeforeEach
  public void setup() {
    System.setProperty(METASTORE_URI_PROPERTY, thriftHiveMetaStore.getThriftConnectionUri());
    metastoreClient = thriftHiveMetaStore.client();
    hiveTestUtils = new HiveTestUtils(metastoreClient);

    amazonS3
        .listObjectsV2(BUCKET)
        .getObjectSummaries()
        .forEach(object -> amazonS3.deleteObject(BUCKET, object.getKey()));
    executorService.execute(() -> BeekeeperMetadataCleanup.main(new String[] {}));
    await().atMost(Duration.ONE_MINUTE).until(BeekeeperMetadataCleanup::isRunning);
  }

  @AfterEach
  public void stop() throws InterruptedException {
    BeekeeperMetadataCleanup.stop();
    executorService.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test
  public void cleanupUnpartitionedTable() throws TException, SQLException {
    hiveTestUtils.createTableWithProperties(UNPARTITIONED_TABLE_PATH, TABLE_NAME_VALUE, false, createBeeKeeperDeletionProperties(), true);
    amazonS3.putObject(BUCKET, UNPARTITIONED_OBJECT_KEY, TABLE_DATA);

    insertExpiredMetadata(UNPARTITIONED_TABLE_PATH, null);
    await()
        .atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getExpiredMetadata().get(0).getHousekeepingStatus() == DELETED);

    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, UNPARTITIONED_OBJECT_KEY)).isFalse();
  }

  @Test
  public void cleanupPartitionedTable() throws Exception {
    Table table = hiveTestUtils.createTableWithProperties(PARTITIONED_TABLE_PATH, TABLE_NAME_VALUE, true, createBeeKeeperDeletionProperties(), true);
    hiveTestUtils.addPartitionsToTable(PARTITION_ROOT_PATH, table, PARTITION_VALUES);

    amazonS3.putObject(BUCKET, PARTITIONED_TABLE_OBJECT_KEY, "");
    amazonS3.putObject(BUCKET, PARTITIONED_OBJECT_KEY, TABLE_DATA);

    insertExpiredMetadata(PARTITIONED_TABLE_PATH, null);
    insertExpiredMetadata(PARTITION_PATH, PARTITION_NAME);
    await()
        .atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getExpiredMetadata().get(1).getHousekeepingStatus() == DELETED);

    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_TABLE_OBJECT_KEY)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_OBJECT_KEY)).isFalse();
  }

  @Test
  public void shouldSkipCleanupForIcebergTable() throws Exception {
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("table_type", "ICEBERG");

    hiveTestUtils.createTableWithProperties(
        PARTITIONED_TABLE_PATH, TABLE_NAME_VALUE, true, tableProperties, true);
    amazonS3.putObject(BUCKET, PARTITIONED_TABLE_OBJECT_KEY, TABLE_DATA);

    insertExpiredMetadata(PARTITIONED_TABLE_PATH, null);

    await()
        .atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getExpiredMetadata().get(0).getHousekeepingStatus() == SKIPPED);

    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_TABLE_OBJECT_KEY)).isTrue();
  }

  @Test
  public void cleanupPartitionButNotTable() throws Exception {
    Table table = hiveTestUtils.createTable(PARTITIONED_TABLE_PATH, TABLE_NAME_VALUE, true);

    hiveTestUtils.addPartitionsToTable(PARTITION_ROOT_PATH, table, PARTITION_VALUES);
    hiveTestUtils
        .addPartitionsToTable(PARTITION_ROOT_PATH, table, List.of("2020-01-01", "1", "B"));

    String partition2Name = "event_date=2020-01-01/event_hour=1/event_type=B";
    String partition2Path = PARTITION_ROOT_PATH + "/" + partition2Name + "/file1";
    String partition2ObjectKey = DATABASE_NAME_VALUE + "/some_location/id1/" + partition2Name + "/file1";

    amazonS3.putObject(BUCKET, PARTITIONED_TABLE_OBJECT_KEY, "");
    amazonS3.putObject(BUCKET, PARTITIONED_OBJECT_KEY, TABLE_DATA);
    amazonS3.putObject(BUCKET, partition2ObjectKey, TABLE_DATA);

    insertExpiredMetadata(PARTITIONED_TABLE_PATH, null);
    insertExpiredMetadata(PARTITION_PATH, PARTITION_NAME);
    insertExpiredMetadata(TABLE_NAME_VALUE, partition2Path, partition2Name, LONG_CLEANUP_DELAY_VALUE);

    await()
        .atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getExpiredMetadata().get(0).getHousekeepingStatus() == DELETED);

    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE)).isTrue();
    List<Partition> partitions = metastoreClient.listPartitions(DATABASE_NAME_VALUE, TABLE_NAME_VALUE, (short) 1);
    assertEquals(partitions.size(), 1);
    assertEquals(partitions.get(0).getValues(), List.of("2020-01-01", "1", "B"));
    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_TABLE_OBJECT_KEY)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_OBJECT_KEY)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, partition2ObjectKey)).isTrue();
  }

  @Test
  public void cleanupPartitionedTableWithNoPartitions() throws TException, SQLException {
    hiveTestUtils.createTableWithProperties(PARTITIONED_TABLE_PATH, TABLE_NAME_VALUE, true, createBeeKeeperDeletionProperties(), true);

    amazonS3.putObject(BUCKET, PARTITIONED_TABLE_OBJECT_KEY, TABLE_DATA);
    insertExpiredMetadata(PARTITIONED_TABLE_PATH, null);
    await()
        .atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getExpiredMetadata().get(0).getHousekeepingStatus() == DELETED);

    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_TABLE_OBJECT_KEY)).isFalse();
  }

  @Test
  public void disableTableWhichWasDropped() throws SQLException {
    amazonS3.putObject(BUCKET, PARTITIONED_TABLE_OBJECT_KEY, TABLE_DATA);
    insertExpiredMetadata(PARTITIONED_TABLE_PATH, null);
    await()
        .atMost(5, TimeUnit.MINUTES)
        .until(() -> getExpiredMetadata().get(0).getHousekeepingStatus() == DISABLED);

    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_TABLE_OBJECT_KEY)).isTrue();
  }

  @Test
  public void disableTableWithNoPartitions() throws TException, SQLException {
    hiveTestUtils.createTable(PARTITIONED_TABLE_PATH, TABLE_NAME_VALUE, true, false);

    amazonS3.putObject(BUCKET, PARTITIONED_TABLE_OBJECT_KEY, TABLE_DATA);
    insertExpiredMetadata(PARTITIONED_TABLE_PATH, null);
    await()
        .atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getExpiredMetadata().get(0).getHousekeepingStatus() == DISABLED);

    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_TABLE_OBJECT_KEY)).isTrue();
  }

  @Test
  public void disablePartitionedTable() throws Exception {
    Table table = hiveTestUtils.createTable(PARTITIONED_TABLE_PATH, TABLE_NAME_VALUE, true, false);
    hiveTestUtils.addPartitionsToTable(PARTITION_ROOT_PATH, table, PARTITION_VALUES);

    amazonS3.putObject(BUCKET, PARTITIONED_TABLE_OBJECT_KEY, "");
    amazonS3.putObject(BUCKET, PARTITIONED_OBJECT_KEY, TABLE_DATA);

    insertExpiredMetadata(PARTITIONED_TABLE_PATH, null);
    insertExpiredMetadata(PARTITION_PATH, PARTITION_NAME);
    await()
        .atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getExpiredMetadata().get(0).getHousekeepingStatus() == DISABLED);

    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_TABLE_OBJECT_KEY)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_OBJECT_KEY)).isTrue();
  }

  @Test
  public void cleanupMultipleTablesOfMixedType() throws Exception {
    hiveTestUtils.createTableWithProperties(UNPARTITIONED_TABLE_PATH, UNPARTITIONED_TABLE_NAME, false, createBeeKeeperDeletionProperties(), true);

    Table partitionedTable = hiveTestUtils
        .createTableWithProperties(PARTITIONED_TABLE_PATH, PARTITIONED_TABLE_NAME, true, createBeeKeeperDeletionProperties(), true);
    hiveTestUtils
        .addPartitionsToTable(PARTITION_ROOT_PATH, partitionedTable, PARTITION_VALUES);

    amazonS3.putObject(BUCKET, UNPARTITIONED_OBJECT_KEY, TABLE_DATA);
    amazonS3.putObject(BUCKET, PARTITIONED_TABLE_OBJECT_KEY, TABLE_DATA);
    amazonS3.putObject(BUCKET, PARTITIONED_OBJECT_KEY, TABLE_DATA);

    insertExpiredMetadata(UNPARTITIONED_TABLE_NAME, UNPARTITIONED_TABLE_PATH, null, SHORT_CLEANUP_DELAY_VALUE);
    insertExpiredMetadata(PARTITIONED_TABLE_NAME, PARTITIONED_TABLE_PATH, null, SHORT_CLEANUP_DELAY_VALUE);
    insertExpiredMetadata(PARTITIONED_TABLE_NAME, PARTITION_PATH, PARTITION_NAME, SHORT_CLEANUP_DELAY_VALUE);
    await()
        .atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getExpiredMetadata().get(1).getHousekeepingStatus() == DELETED);

    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, UNPARTITIONED_TABLE_NAME)).isFalse();
    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, PARTITIONED_TABLE_NAME)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, UNPARTITIONED_OBJECT_KEY)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_TABLE_OBJECT_KEY)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_OBJECT_KEY)).isFalse();
  }

  @Test
  public void onlyCleanupLocationWhenTableExists() throws SQLException {
    amazonS3.putObject(BUCKET, PARTITIONED_TABLE_OBJECT_KEY, "");
    insertExpiredMetadata(PARTITIONED_TABLE_PATH, null);
    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_TABLE_OBJECT_KEY)).isTrue();
  }

  @Test
  public void onlyCleanupLocationWhenPartitionExists() throws TException, SQLException {
    hiveTestUtils.createTableWithProperties(PARTITIONED_TABLE_PATH, TABLE_NAME_VALUE, true, createBeeKeeperDeletionProperties() ,true);

    amazonS3.putObject(BUCKET, PARTITIONED_TABLE_OBJECT_KEY, "");
    amazonS3.putObject(BUCKET, PARTITIONED_OBJECT_KEY, TABLE_DATA);

    insertExpiredMetadata(PARTITIONED_TABLE_PATH, null);
    insertExpiredMetadata(PARTITION_PATH, PARTITION_NAME);
    await()
        .atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getExpiredMetadata().get(1).getHousekeepingStatus() == DELETED);

    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_TABLE_OBJECT_KEY)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_OBJECT_KEY)).isTrue();
  }

  @Test
  public void testEventAddedToHistoryTable() throws TException, SQLException {
    hiveTestUtils.createTableWithProperties(UNPARTITIONED_TABLE_PATH, TABLE_NAME_VALUE, false, createBeeKeeperDeletionProperties(), true);
    amazonS3.putObject(BUCKET, UNPARTITIONED_OBJECT_KEY, TABLE_DATA);

    insertExpiredMetadata(UNPARTITIONED_TABLE_PATH, null);
    await()
        .atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getBeekeeperHistoryRowCount(LifecycleEventType.EXPIRED) == 1);

    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, UNPARTITIONED_OBJECT_KEY)).isFalse();

    List<BeekeeperHistory> beekeeperHistory = getBeekeeperHistory(EXPIRED);
    BeekeeperHistory history = beekeeperHistory.get(0);
    assertThat(history.getDatabaseName()).isEqualTo(DATABASE_NAME_VALUE);
    assertThat(history.getTableName()).isEqualTo(TABLE_NAME_VALUE);
    assertThat(history.getLifecycleType()).isEqualTo(EXPIRED.toString());
    assertThat(history.getHousekeepingStatus()).isEqualTo(DELETED.name());
  }

  @Test
  public void tableNotDeletedWhenDeletionPropertyIsFalse() throws TException, SQLException {
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("beekeeper.expired.data.table.deletion.enabled", "false");

    hiveTestUtils.createTableWithProperties(UNPARTITIONED_TABLE_PATH, TABLE_NAME_VALUE, false, tableProperties, true);
    amazonS3.putObject(BUCKET, UNPARTITIONED_OBJECT_KEY, TABLE_DATA);

    insertExpiredMetadata(UNPARTITIONED_TABLE_PATH, null);
    await()
        .atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getExpiredMetadata().get(0).getHousekeepingStatus() == SKIPPED);

    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, UNPARTITIONED_OBJECT_KEY)).isTrue();
  }

  @Test
  public void tableNotDeletedWhenDeletionPropertyNotSet() throws TException, SQLException {
    hiveTestUtils.createTable(UNPARTITIONED_TABLE_PATH, TABLE_NAME_VALUE, false);
    amazonS3.putObject(BUCKET, UNPARTITIONED_OBJECT_KEY, TABLE_DATA);

    insertExpiredMetadata(UNPARTITIONED_TABLE_PATH, null);
    await()
        .atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getExpiredMetadata().get(0).getHousekeepingStatus() == SKIPPED);

    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, UNPARTITIONED_OBJECT_KEY)).isTrue();
  }

  @Test
  public void metrics() throws Exception {
    Table table = hiveTestUtils.createTableWithProperties(PARTITIONED_TABLE_PATH, TABLE_NAME_VALUE, true, createBeeKeeperDeletionProperties(), true);
    hiveTestUtils.addPartitionsToTable(PARTITION_ROOT_PATH, table, PARTITION_VALUES);

    amazonS3.putObject(BUCKET, PARTITIONED_TABLE_OBJECT_KEY, "");
    amazonS3.putObject(BUCKET, PARTITIONED_OBJECT_KEY, TABLE_DATA);

    insertExpiredMetadata(PARTITIONED_TABLE_PATH, null);
    insertExpiredMetadata(PARTITION_PATH, PARTITION_NAME);
    await()
        .atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getExpiredMetadata().get(1).getHousekeepingStatus() == DELETED);

    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_TABLE_OBJECT_KEY)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_OBJECT_KEY)).isFalse();
    assertMetrics();
  }

  protected void assertMetrics() {
    Set<MeterRegistry> meterRegistry = ((CompositeMeterRegistry) BeekeeperMetadataCleanup.meterRegistry()).getRegistries();
    assertThat(meterRegistry).hasSize(2);
    meterRegistry.forEach(registry -> {
      List<Meter> meters = registry.getMeters();
      assertThat(meters).extracting("id", Meter.Id.class).extracting("name")
          .contains("metadata-cleanup-job", "hive-table-deleted", "hive-partition-deleted", "hive-table-" + METRIC_NAME,
              "hive-partition-" + METRIC_NAME, "s3-paths-deleted", "s3-" + BytesDeletedReporter.METRIC_NAME);
    });
  }

  private Map<String, String> createBeeKeeperDeletionProperties() {
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("beekeeper.expired.data.table.deletion.enabled", "true");
    return tableProperties;
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
