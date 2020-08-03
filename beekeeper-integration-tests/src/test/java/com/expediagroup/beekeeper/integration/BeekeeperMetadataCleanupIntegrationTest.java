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
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DELETED;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.AWS_REGION;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.DATABASE_NAME_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.TABLE_NAME_VALUE;

import java.net.URI;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.google.common.collect.ImmutableMap;

import com.expediagroup.beekeeper.cleanup.BeekeeperCleanup;
import com.expediagroup.beekeeper.integration.utils.ContainerTestUtils;
import com.expediagroup.beekeeper.integration.utils.HiveTestUtils;
import com.expediagroup.beekeeper.metadata.cleanup.BeekeeperMetadataCleanup;

import com.hotels.beeju.extensions.ThriftHiveMetaStoreJUnitExtension;

public class BeekeeperMetadataCleanupIntegrationTest extends BeekeeperIntegrationTestBase {

  private static final int TIMEOUT = 30;

  private static final String BUCKET = "test-path-bucket";
  private static final String DB_AND_TABLE_PREFIX = DATABASE_NAME_VALUE + "/" + TABLE_NAME_VALUE;
  private static final String OBJECT_KEY_ROOT = DB_AND_TABLE_PREFIX + "/id1/partition1";
  private static final String OBJECT_KEY1 = DB_AND_TABLE_PREFIX + "/id1/partition1/file1";
  private static final String OBJECT_KEY2 = DB_AND_TABLE_PREFIX + "/id1/partition1/file2";
  private static final String OBJECT_KEY_SENTINEL = DB_AND_TABLE_PREFIX + "/id1/partition1_$folder$";
  private static final String ABSOLUTE_PATH = "s3://" + BUCKET + "/" + OBJECT_KEY_ROOT;
  private static final String PARTITION_NAME = "event_date=2020-01-01/event_hour=0/event_type=A";

  private static final String S3_ACCESS_KEY = "access";
  private static final String S3_SECRET_KEY = "secret";
  private static final String CONTENT = "Content";
  private static final String SCHEDULER_DELAY_MS = "5000";

  private static AmazonS3 amazonS3;

  @Rule
  public static LocalStackContainer s3Container = ContainerTestUtils.awsContainer(S3);
  static {
    s3Container.start();
  }

  private static final String endpoint = ContainerTestUtils.awsServiceEndpoint(s3Container, S3);
  private final ExecutorService executorService = Executors.newFixedThreadPool(1);

  private static Map<String, String> metastoreProperties = ImmutableMap
      .<String, String>builder()
      .put("fs.s3a.endpoint", endpoint)
      .put("fs.s3a.access.key", S3_ACCESS_KEY)
      .put("fs.s3a.secret.key", S3_SECRET_KEY)
      .build();


  private static String metastoreUri;
  @RegisterExtension
  public static ThriftHiveMetaStoreJUnitExtension thriftHiveMetaStore = new ThriftHiveMetaStoreJUnitExtension(
      DATABASE_NAME_VALUE, metastoreProperties);
  static {
    metastoreUri = thriftHiveMetaStore.getThriftConnectionUri();
    System.out.println("*** CHECKING INSIDE STATIC METHOD - " + metastoreUri);
  }

  private HiveTestUtils hiveTestUtils = new HiveTestUtils();

  @BeforeAll
  public static void init() {
    System.setProperty("spring.profiles.active", "test");
    System.setProperty("properties.scheduler-delay-ms", SCHEDULER_DELAY_MS);
    System.setProperty("properties.dry-run-enabled", "false");

    String uri = thriftHiveMetaStore.getThriftConnectionUri();
    System.out.println("******* TESTING URI = " + uri);

    System.out.println("Thrift metastore: " + thriftHiveMetaStore + ". And client: " + thriftHiveMetaStore.client());
    // gives '******* TESTING URI = thrift://localhost:0'
    // System.setProperty("properties.metastore-uri", metastoreUri);
    System.setProperty("aws.s3.endpoint", endpoint);
    System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");
    System.setProperty("com.amazonaws.services.s3.disablePutObjectMD5Validation", "true");

    amazonS3 = ContainerTestUtils.s3Client(s3Container, AWS_REGION);
    amazonS3.createBucket(new CreateBucketRequest(BUCKET, AWS_REGION));
  }

  @AfterAll
  public static void teardown() {
    amazonS3.shutdown();
    s3Container.stop();
  }

  @BeforeEach
  public void setup() {
    // String uri = thriftHiveMetaStore.getThriftConnectionUri();
    // System.out.println("*** TESTING URI INSIDE SETUP 1: " + uri);
    // // gives ' ******* inside common beans. URI - thrift://localhost:52646'
    // System.setProperty("properties.metastore-uri", thriftHiveMetaStore.getThriftConnectionUri());

    amazonS3
        .listObjectsV2(BUCKET)
        .getObjectSummaries()
        .forEach(object -> amazonS3.deleteObject(BUCKET, object.getKey()));
    executorService.execute(() -> BeekeeperMetadataCleanup.main(new String[] {}));
    await().atMost(Duration.ONE_MINUTE).until(BeekeeperMetadataCleanup::isRunning);

    System.out.println("*** TESTING URI INSIDE SETUP 2: " + thriftHiveMetaStore.getThriftConnectionUri());
  }

  @AfterEach
  public void stop() throws InterruptedException {
    String uri = thriftHiveMetaStore.getThriftConnectionUri();
    System.out.println("*** TESTING URI INSIDE after each: " + uri);

    BeekeeperCleanup.stop();
    executorService.awaitTermination(5, TimeUnit.SECONDS);
  }

  // Tests
  // add to s3 bucket
  // create table
  // schedule expired

  // wait for run

  // check table is dropped
  // check s3 files removed

  @Test
  public void cleanupUnpartitionedTable() throws TException, SQLException {
    // add to s3 bucket
    amazonS3.putObject(BUCKET, OBJECT_KEY1, CONTENT);

    String path = "s3a://" + BUCKET + "/" + OBJECT_KEY1;
    // create table
    HiveMetaStoreClient metastoreClient = thriftHiveMetaStore.client();
    hiveTestUtils.createUnpartitionedTable(metastoreClient, path);
    // schedule expired
    insertExpiredMetadata(path, null);
    // wait for run
    await()
        .atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getExpiredMetadata().get(0).getHousekeepingStatus() == DELETED);


    // check table is dropped
    assertThat(amazonS3.doesObjectExist(BUCKET, OBJECT_KEY1)).isFalse();
    // check s3 files removed
    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE)).isFalse();

  }


  @Test
  public void test() throws TException {
    String path = "s3a://" + BUCKET + "/" + OBJECT_KEY1;

    HiveMetaStoreClient client = thriftHiveMetaStore.client();
    String uri = thriftHiveMetaStore.getThriftConnectionUri();
    System.out.println("TESTING - " + client + " AND URI = " + uri);

    hiveTestUtils.createUnpartitionedTable(client, path);

    boolean result = client.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE);
    System.out.println("************** RESULT: " + result);
  }

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

  @Test
  public void cleanupUnpartitionedMetadata() throws SQLException, TException {
    HiveMetaStoreClient client = thriftHiveMetaStore.client();
    String uri = thriftHiveMetaStore.getThriftConnectionUri();
    System.out.println("TESTING - " + client + " AND URI = " + uri);
    // add the table to hive
    // add the data to S3

    // hiveTestUtils.createUnpartitionedTable(path);
    // insertExpiredMetadata(path, PARTITION_NAME);
    
    // TODO
    // add a table to this client
    // check it exists
    // oh wait - not sure this is going to work, since the code itself makes its own hive metastore and client stuff ..
    // how can this be mocked?????
    // use a temp metastore uri or something?
    // maybe this is why we use system prooperties so they can be overriden at this step??

    // can get a uri from the hive object
    // set this as the property in the init, like with the aws stuff and dry run



    // clients coming out as null
    // how can i fix this ?
    // need to set some env variables????
    // hiveMetastoreClient = thriftHive.client();
    // hiveMetastoreClient.createTable(table);


    // assert that the data is deleted
    // assert that the table is dropped

    // HiveMetaStoreClient client = hive.client();
    // Table table2 = new Table();
    // table2.setDbName("foo_db");
    // table2.setTableName("bar_table");

    // hive.client().getDatabase("foo_db");
    // hive.client().createTable(table2);

    // boolean result = hive.client().tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE);
    // System.out.println("************** RESULT: " + result);


    // assertEquals(1, client.listPartitions("foo_db", "bar_table", (short) 100));
  }

  public URI toUri(String baseLocation, String database, String table) {
    return URI.create(String.format("%s/%s/%s", baseLocation, database, table)).normalize();
  }

}
