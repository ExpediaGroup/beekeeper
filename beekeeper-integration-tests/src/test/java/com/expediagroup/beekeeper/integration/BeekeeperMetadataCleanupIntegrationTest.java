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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DELETED;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.AWS_REGION;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.DATABASE_NAME_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.TABLE_NAME_VALUE;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.google.common.collect.ImmutableMap;

import com.expediagroup.beekeeper.integration.utils.ContainerTestUtils;
import com.expediagroup.beekeeper.integration.utils.HiveTestUtils;
import com.expediagroup.beekeeper.metadata.cleanup.BeekeeperMetadataCleanup;

import com.hotels.beeju.extensions.ThriftHiveMetaStoreJUnitExtension;

@Testcontainers
public class BeekeeperMetadataCleanupIntegrationTest extends BeekeeperIntegrationTestBase {

  private static final int TIMEOUT = 30;

  private static final String BUCKET = "test-path-bucket";
  private static final String DB_AND_TABLE_PREFIX = DATABASE_NAME_VALUE + "/" + TABLE_NAME_VALUE;

  public static final String PART_00000 = "part-00000";
  private static final String PARTITIONS = "event_date/event_hour/event_type";

  private static final String PARTITIONED_OBJECT_KEY_ROOT = DB_AND_TABLE_PREFIX
      + "/id1/"
      + PARTITIONS
      + "/"
      + PART_00000;

  private static final String PARTITION_NAME = "event_date=2020-01-01/event_hour=0/event_type=A";
  private static final String TABLE_DATA = "1\tadam\tlondon\n2\tsusan\tglasgow\n";

  private static final String PARTITIONED_OBJECT_KEY1 = DB_AND_TABLE_PREFIX
      + "/id1/"
      + PARTITION_NAME
      + "/"
      + PART_00000;
  // private static final String OBJECT_KEY2 = DB_AND_TABLE_PREFIX + "/id1/" + PARTITIONS + "/file2";

  // not sure if this is needed
  private static final String PARTITIONED_OBJECT_KEY_SENTINEL = DB_AND_TABLE_PREFIX + "/id1/partition1_$folder$";

  private static final String PARTITIONED_TABLE_LOCATION = "s3a://" + BUCKET + "/" + DB_AND_TABLE_PREFIX;
  private static final String PARTITION_LOCATION = "s3a://" + BUCKET + "/" + PARTITIONED_OBJECT_KEY1;

  private static final String UNPARTITIONED_OBJECT_KEY1 = DB_AND_TABLE_PREFIX + "/id1";
  private static final String UNPARTITIONED_OBJECT_KEY_SENTINEL = DB_AND_TABLE_PREFIX + "/id1_$folder$";
  private static final String UNPARTITIONED_OBJECT_KEY_ROOT = DB_AND_TABLE_PREFIX + "/id1";
  private static final String UNPARTITIONED_ABSOLUTE_PATH = "s3a://" + BUCKET + "/" + UNPARTITIONED_OBJECT_KEY_ROOT;

  private static final String PARTITION_OBJECT_KEY_OTHER = DB_AND_TABLE_PREFIX + "/id1/partition10/file1";
  private static final String PARTITION_OBJECT_KEY_OTHER_SENTINEL = DB_AND_TABLE_PREFIX + "/id1/partition10_$folder$";
  // private static final String UNPARTITION_OBJECT_KEY_OTHER = DB_AND_TABLE_PREFIX + "/id1/partition10/file1";
  // private static final String UNPARTITION_OBJECT_KEY_OTHER_SENTINEL = DB_AND_TABLE_PREFIX +
  // "/id1/partition10_$folder$";

  private static final String S3_ACCESS_KEY = "access";
  private static final String S3_SECRET_KEY = "secret";
  private static final String CONTENT = "Content";
  private static final String SCHEDULER_DELAY_MS = "5000";

  private static AmazonS3 amazonS3;

  // @Container
  @Rule
  public static final LocalStackContainer S3_CONTAINER = ContainerTestUtils.awsContainer(S3);
  static {
    S3_CONTAINER.start();
  }

  private static final String endpoint = ContainerTestUtils.awsServiceEndpoint(S3_CONTAINER, S3);
  private final ExecutorService executorService = Executors.newFixedThreadPool(1);

  // TODO
  // use the variables for the names - dont hard code
  private static Map<String, String> metastoreProperties = ImmutableMap
      .<String, String>builder()
      .put("fs.s3a.endpoint", endpoint)
      .put("fs.s3a.access.key", S3_ACCESS_KEY)
      .put("fs.s3a.secret.key", S3_SECRET_KEY)
      .build();

  @RegisterExtension
  public static ThriftHiveMetaStoreJUnitExtension thriftHiveMetaStore = new ThriftHiveMetaStoreJUnitExtension(
      DATABASE_NAME_VALUE, metastoreProperties);

  // public static @Rule ThriftHiveMetaStoreJUnitRule thriftHiveMetaStore = new ThriftHiveMetaStoreJUnitRule(
  // DATABASE_NAME_VALUE,
  // metastoreProperties);

  private HiveTestUtils hiveTestUtils = new HiveTestUtils();

  private HiveMetaStoreClient metastoreClient;

  @BeforeAll
  public static void init() {
    System.setProperty("spring.profiles.active", "test");
    System.setProperty("properties.scheduler-delay-ms", SCHEDULER_DELAY_MS);
    System.setProperty("properties.dry-run-enabled", "false");
    System.setProperty("aws.s3.endpoint", endpoint);
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
    System.out.println("***** client: " + metastoreClient + " uri: " + thriftHiveMetaStore.getThriftConnectionUri());
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
    System.out.println("**** Inside the after each method. Stopping beekeeper and terminating executor service.");

    BeekeeperMetadataCleanup.stop();
    executorService.awaitTermination(5, TimeUnit.SECONDS);
  }

  // TODO
  // fix rejected execution exception
  // // whats going on?
  // // how can it be fixed?
  // // looks like the container gets shut down - but which one? does it matter that our execute service is shut down?
  // // i think the order is our after each, then the thrift version - would this have any effect?????

  // add in auto-start container stuff
  // // will need to figure out how to wait for it to be set up before the thrift metastore is made

  @Test
  public void cleanupUnpartitionedTable() throws TException, SQLException {
    // create table
    hiveTestUtils.createUnpartitionedTable(metastoreClient, UNPARTITIONED_ABSOLUTE_PATH);

    // add to s3 bucket
    amazonS3.putObject(BUCKET, UNPARTITIONED_OBJECT_KEY1, TABLE_DATA);
    amazonS3.putObject(BUCKET, UNPARTITIONED_OBJECT_KEY_SENTINEL, "");

    insertExpiredMetadata(UNPARTITIONED_ABSOLUTE_PATH, null);
    await()
        .atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getExpiredMetadata().get(0).getHousekeepingStatus() == DELETED);

    // check table is dropped
    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE)).isFalse();
    // check the correct s3 file is removed
    assertThat(amazonS3.doesObjectExist(BUCKET, UNPARTITIONED_OBJECT_KEY1)).isFalse();
    assertThat(amazonS3.doesObjectExist(BUCKET, UNPARTITIONED_OBJECT_KEY_SENTINEL)).isTrue();
  }

  @Test
  public void cleanupPartitionedTable() throws Exception {
    // create table
    Table table = hiveTestUtils.createPartitionedTable(metastoreClient, PARTITION_LOCATION);

    // "event_date=2020-01-01/event_hour=0/event_type=A
    hiveTestUtils
        .addPartitionsToTable(metastoreClient, PARTITIONED_TABLE_LOCATION, table, List.of("2020-01-01", "0", "A"),
            TABLE_DATA);

    // add to s3 bucket
    amazonS3.putObject(BUCKET, PARTITIONED_OBJECT_KEY1, TABLE_DATA);

    insertExpiredMetadata(PARTITIONED_TABLE_LOCATION, null);
    insertExpiredMetadata(PARTITION_LOCATION, PARTITION_NAME);
    // wait for run
    await()
        .atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getExpiredMetadata().get(0).getHousekeepingStatus() == DELETED);

    // check table is dropped
    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE)).isFalse();
    // check the correct s3 file is removed
    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_OBJECT_KEY1)).isFalse();
  }

  // clean up table only if its expired
  // dont clean up partition table with existing partitions

  @Test
  public void cleanupPartitionButNotTable() throws Exception {
    Table table = hiveTestUtils.createPartitionedTable(metastoreClient, PARTITION_LOCATION);

    hiveTestUtils
        .addPartitionsToTable(metastoreClient, PARTITIONED_TABLE_LOCATION, table, List.of("2020-01-01", "0", "A"),
            TABLE_DATA);
    hiveTestUtils
        .addPartitionsToTable(metastoreClient, PARTITIONED_TABLE_LOCATION, table, List.of("2020-01-01", "1", "B"),
            TABLE_DATA);

    System.out.println(metastoreClient.listPartitions(DATABASE_NAME_VALUE, TABLE_NAME_VALUE, (short) 1));
    String partition2Name = "event_date=2020-01-01/event_hour=1/event_type=B";
    String partition2Location = partition2Name + "/" + PART_00000;

    // add to s3 bucket
    amazonS3.putObject(BUCKET, PARTITIONED_OBJECT_KEY1, TABLE_DATA);
    amazonS3.putObject(BUCKET, DB_AND_TABLE_PREFIX + partition2Location, TABLE_DATA);

    insertExpiredMetadata(PARTITIONED_TABLE_LOCATION, null);
    insertExpiredMetadata(PARTITION_LOCATION, PARTITION_NAME);
    insertNonExpiredMetadata(PARTITIONED_TABLE_LOCATION + "/" + partition2Location, partition2Name);

    await()
        .atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getExpiredMetadata().get(2).getHousekeepingStatus() == DELETED);

    // check table is not dropped
    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE)).isTrue();

    List<Partition> partitions = metastoreClient.listPartitions(DATABASE_NAME_VALUE, TABLE_NAME_VALUE, (short) 1);
    List<String> partitionValues = partitions.get(0).getValues();

    // check that the second partition still exists
    assertEquals(partitions.size(), 1);
    assertEquals(partitionValues, List.of("2020-01-01", "1", "B"));

    // check the correct s3 file is removed
    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_OBJECT_KEY1)).isFalse();
  }

  @Test
  public void cleanupPartitionedTableWithNoPartitions() throws Exception {
    hiveTestUtils.createPartitionedTable(metastoreClient, PARTITION_LOCATION);

    amazonS3.putObject(BUCKET, PARTITIONED_OBJECT_KEY1, TABLE_DATA);
    insertExpiredMetadata(PARTITIONED_TABLE_LOCATION, null);
    await()
        .atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getExpiredMetadata().get(0).getHousekeepingStatus() == DELETED);

    // check table is dropped
    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE)).isFalse();
    // check the correct s3 file is removed
    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_OBJECT_KEY1)).isFalse();
  }

  // @Test
  // public void test() throws TException {
  // String path = "s3a://" + BUCKET + "/" + PARTITIONED_OBJECT_KEY1;
  //
  // HiveMetaStoreClient client = thriftHiveMetaStore.client();
  // String uri = thriftHiveMetaStore.getThriftConnectionUri();
  // System.out.println("TESTING - " + client + " AND URI = " + uri);
  //
  // hiveTestUtils.createUnpartitionedTable(client, path);
  //
  // boolean result = client.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE);
  // System.out.println("************** RESULT: " + result);
  // }

  // TODO
  // add some metadata to the HousekeepingMetadata table

  // setup
  // need some kind of hive object can test with

  // utils
  // need to have some hive test utils
  // will have a hive metastore
  // like circus train
  // // create unpartitioned table
  // // create partitioned table

  // will also need to make use of the amazon s3 stuff
  // set up some paths
  // make sure theyre deleted

  // use the integration test base
  // insertExpiredMetadata

  // TODO - tests
  // test the cleanup for partitioned tables
  // test the cleanup for unpartitioned tables
  // // isnt deleted if there are existing partitions
  // // is deleted when there are no existing partitions
  // test the cleanup for a partition on a table
  // check that paths are still deleted if the table doesnt exist
  // check that paths are still deleted if the partition doesnt exist

}
