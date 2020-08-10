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

import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.SECRET_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import static com.expediagroup.beekeeper.cleanup.monitoring.DeletedMetadataReporter.METRIC_NAME;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DELETED;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.AWS_REGION;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.DATABASE_NAME_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.LONG_CLEANUP_DELAY_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.SHORT_CLEANUP_DELAY_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.TABLE_NAME_VALUE;

import java.sql.SQLException;
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

import com.expediagroup.beekeeper.cleanup.monitoring.DeletedMetadataReporter;
import com.expediagroup.beekeeper.integration.utils.ContainerTestUtils;
import com.expediagroup.beekeeper.integration.utils.HiveTestUtils;
import com.expediagroup.beekeeper.metadata.cleanup.BeekeeperMetadataCleanup;
import com.expediagroup.beekeeper.path.cleanup.BeekeeperPathCleanup;

import com.hotels.beeju.extensions.ThriftHiveMetaStoreJUnitExtension;

@Testcontainers
public class BeekeeperMetadataCleanupIntegrationTest extends BeekeeperIntegrationTestBase {

  private static final int TIMEOUT = 30;
  private static final String SCHEDULER_DELAY_MS = "5000";
  private static final String HEALTHCHECK_URI = "http://localhost:8008/actuator/health";
  private static final String PROMETHEUS_URI = "http://localhost:8008/actuator/prometheus";

  private static final String S3_ACCESS_KEY = "access";
  private static final String S3_SECRET_KEY = "secret";

  private static final String BUCKET = "test-path-bucket";
  private static final String TABLE_DATA = "1\tadam\tlondon\n2\tsusan\tglasgow\n";
  private static final String PARTITIONED_TABLE_NAME = TABLE_NAME_VALUE + "_partitioned";
  private static final String UNPARTITIONED_TABLE_NAME = TABLE_NAME_VALUE + "_unpartitioned";

  private static final String PARTITION_NAME = "event_date=2020-01-01/event_hour=0/event_type=A";
  private static final List<String> PARTITION_VALUES = List.of("2020-01-01", "0", "A");

  private static final String ROOT_PATH = "s3a://" + BUCKET + "/" + DATABASE_NAME_VALUE + "/";

  private static final String PARTITIONED_TABLE_PATH = ROOT_PATH + PARTITIONED_TABLE_NAME + "/id1";
  private static final String PARTITION_ROOT_PATH = ROOT_PATH + "some_location/id1";
  private static final String PARTITION_PATH = PARTITION_ROOT_PATH + "/" + PARTITION_NAME + "/file1";
  private static final String PARTITIONED_TABLE_OBJECT_KEY = DATABASE_NAME_VALUE
      + "/"
      + PARTITIONED_TABLE_NAME
      + "/id1";

  private static final String PARTITIONED_OBJECT_KEY = DATABASE_NAME_VALUE
      + "/some_location/id1/"
      + PARTITION_NAME
      + "/file1";

  private static final String UNPARTITIONED_TABLE_PATH = ROOT_PATH + UNPARTITIONED_TABLE_NAME + "/id1";
  private static final String UNPARTITIONED_OBJECT_KEY = DATABASE_NAME_VALUE
      + "/"
      + UNPARTITIONED_TABLE_NAME
      + "/id1/file1";

  @Rule
  public static final LocalStackContainer S3_CONTAINER = ContainerTestUtils.awsContainer(S3);
  static {
    S3_CONTAINER.start();
  }

  private static AmazonS3 amazonS3;
  private static final String S3_ENDPOINT = ContainerTestUtils.awsServiceEndpoint(S3_CONTAINER, S3);
  private final ExecutorService executorService = Executors.newFixedThreadPool(1);

  private static Map<String, String> metastoreProperties = ImmutableMap
      .<String, String>builder()
      .put(ENDPOINT, S3_ENDPOINT)
      .put(ACCESS_KEY, S3_ACCESS_KEY)
      .put(SECRET_KEY, S3_SECRET_KEY)
      .build();

  @RegisterExtension
  public ThriftHiveMetaStoreJUnitExtension thriftHiveMetaStore = new ThriftHiveMetaStoreJUnitExtension(
      DATABASE_NAME_VALUE, metastoreProperties);

  private HiveTestUtils hiveTestUtils = new HiveTestUtils();
  private HiveMetaStoreClient metastoreClient;

  @BeforeAll
  public static void init() {
    System.setProperty("spring.profiles.active", "test");
    System.setProperty("properties.scheduler-delay-ms", SCHEDULER_DELAY_MS);
    System.setProperty("properties.dry-run-enabled", "false");
    System.setProperty("aws.s3.endpoint", S3_ENDPOINT);
    System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");
    System.setProperty("com.amazonaws.services.s3.disablePutObjectMD5Validation", "true");

    amazonS3 = ContainerTestUtils.s3Client(S3_CONTAINER, AWS_REGION);
    amazonS3.createBucket(new CreateBucketRequest(BUCKET, AWS_REGION));
  }

  @AfterAll
  public static void teardown() {
    amazonS3.shutdown();
    S3_CONTAINER.stop();
  }

  @BeforeEach
  public void setup() {
    metastoreClient = thriftHiveMetaStore.client();
    System.setProperty("properties.metastore-uri", thriftHiveMetaStore.getThriftConnectionUri());

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
    hiveTestUtils.createTable(metastoreClient, UNPARTITIONED_TABLE_PATH, TABLE_NAME_VALUE, false);
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
    Table table = hiveTestUtils.createTable(metastoreClient, PARTITIONED_TABLE_PATH, TABLE_NAME_VALUE, true);

    hiveTestUtils.addPartitionsToTable(metastoreClient, PARTITION_ROOT_PATH, table, PARTITION_VALUES, TABLE_DATA);

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
  public void cleanupPartitionButNotTable() throws Exception {
    Table table = hiveTestUtils.createTable(metastoreClient, PARTITIONED_TABLE_PATH, TABLE_NAME_VALUE, true);

    hiveTestUtils.addPartitionsToTable(metastoreClient, PARTITION_ROOT_PATH, table, PARTITION_VALUES, TABLE_DATA);
    hiveTestUtils
        .addPartitionsToTable(metastoreClient, PARTITION_ROOT_PATH, table, List.of("2020-01-01", "1", "B"), TABLE_DATA);

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
    hiveTestUtils.createTable(metastoreClient, PARTITIONED_TABLE_PATH, TABLE_NAME_VALUE, true);

    amazonS3.putObject(BUCKET, PARTITIONED_TABLE_OBJECT_KEY, TABLE_DATA);
    insertExpiredMetadata(PARTITIONED_TABLE_PATH, null);
    await()
        .atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getExpiredMetadata().get(0).getHousekeepingStatus() == DELETED);

    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_TABLE_OBJECT_KEY)).isFalse();
  }

  @Test
  public void cleanupMultipleTablesOfMixedType() throws Exception {
    hiveTestUtils.createTable(metastoreClient, UNPARTITIONED_TABLE_PATH, UNPARTITIONED_TABLE_NAME, false);

    Table partitionedTable = hiveTestUtils
        .createTable(metastoreClient, PARTITIONED_TABLE_PATH, PARTITIONED_TABLE_NAME, true);
    hiveTestUtils
        .addPartitionsToTable(metastoreClient, PARTITION_ROOT_PATH, partitionedTable, PARTITION_VALUES, TABLE_DATA);

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

    await()
        .atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getExpiredMetadata().get(0).getHousekeepingStatus() == DELETED);

    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_TABLE_OBJECT_KEY)).isTrue();
  }

  @Test
  public void onlyCleanupLocationWhenPartitionExists() throws TException, SQLException {
    Table table = hiveTestUtils.createTable(metastoreClient, PARTITIONED_TABLE_PATH, TABLE_NAME_VALUE, true);

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
  public void metrics() throws SQLException, TException {
    Table table = hiveTestUtils.createTable(metastoreClient, UNPARTITIONED_TABLE_PATH, TABLE_NAME_VALUE, false);
    amazonS3.putObject(BUCKET, UNPARTITIONED_OBJECT_KEY, "");

    insertExpiredMetadata(UNPARTITIONED_TABLE_PATH, null);
    await().atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getExpiredMetadata().get(0).getHousekeepingStatus() == DELETED);

    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, UNPARTITIONED_OBJECT_KEY)).isFalse();
    assertMetrics();
  }

  private void assertMetrics() {
    Set<MeterRegistry> meterRegistry = ((CompositeMeterRegistry) BeekeeperMetadataCleanup.meterRegistry()).getRegistries();
    assertThat(meterRegistry).hasSize(2);
    meterRegistry.forEach(registry -> {
      List<Meter> meters = registry.getMeters();
      assertThat(meters).extracting("id", Meter.Id.class).extracting("name")
          .contains("cleanup-job", "hive-tables-deleted", "hive-table-" + METRIC_NAME);
    });
  }

  // TODO
  // fix test
  @Test
  public void healthCheck() {
    CloseableHttpClient client = HttpClientBuilder.create().build();
    HttpGet request = new HttpGet(HEALTHCHECK_URI);
    await().atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> client.execute(request).getStatusLine().getStatusCode() == 200);
  }

  // TODO
  // fix test
  @Test
  public void prometheus() {
    CloseableHttpClient client = HttpClientBuilder.create().build();
    HttpGet request = new HttpGet(PROMETHEUS_URI);
    await().atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> client.execute(request).getStatusLine().getStatusCode() == 200);
  }


  // TODO
  // use 'log lines' like in the dry run path cleanup - when you cant wait for something to say 'deleted'
  // do this in dry run also

}
