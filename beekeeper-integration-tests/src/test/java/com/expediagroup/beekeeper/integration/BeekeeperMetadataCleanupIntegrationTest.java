package com.expediagroup.beekeeper.integration;

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import static com.expediagroup.beekeeper.integration.CommonTestVariables.AWS_REGION;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.DATABASE_NAME_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.TABLE_NAME_VALUE;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.thrift.TException;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.localstack.LocalStackContainer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.google.common.collect.ImmutableMap;

import com.expediagroup.beekeeper.integration.utils.ContainerTestUtils;
import com.expediagroup.beekeeper.integration.utils.HiveTestUtils;

import com.hotels.beeju.extensions.ThriftHiveMetaStoreJUnitExtension;



public class BeekeeperMetadataCleanupIntegrationTest extends BeekeeperIntegrationTestBase {

  private static final String BUCKET = "test-path-bucket";
  private static final String DB_AND_TABLE_PREFIX = DATABASE_NAME_VALUE + "/" + TABLE_NAME_VALUE;
  private static final String OBJECT_KEY1 = DB_AND_TABLE_PREFIX + "/id1/partition1/file1";
  private static final String PARTITION_NAME = "event_date=2020-01-01/event_hour=0/event_type=A";

  private static final String S3_ACCESS_KEY = "access";
  private static final String S3_SECRET_KEY = "secret";
  private final int s3ProxyPort = getAvailablePort();

  private static AmazonS3 amazonS3;

  @Rule
  public static LocalStackContainer s3Container = ContainerTestUtils.awsContainer(S3);

  private static final String endpoint = ContainerTestUtils.awsServiceEndpoint(s3Container, S3);

  private static final String SCHEDULER_DELAY_MS = "5000";

  private final ExecutorService executorService = Executors.newFixedThreadPool(1);

  private static Map<String, String> metastoreProperties = ImmutableMap
      .<String, String>builder()
      .put("fs.s3a.endpoint", endpoint)
      .put("fs.s3a.access.key", S3_ACCESS_KEY)
      .put("fs.s3a.secret.key", S3_SECRET_KEY)
      .build();


  // Hive
  @RegisterExtension
  public static ThriftHiveMetaStoreJUnitExtension thriftHive = new ThriftHiveMetaStoreJUnitExtension(
      DATABASE_NAME_VALUE,
      metastoreProperties);

  private HiveTestUtils hiveTestUtils = new HiveTestUtils();

  @BeforeAll
  public static void init() {
    System.out.println("test");
    System.setProperty("spring.profiles.active", "test");
    System.setProperty("properties.scheduler-delay-ms", SCHEDULER_DELAY_MS);
    System.setProperty("properties.dry-run-enabled", "false");
    // ****
    System.setProperty("properties.metastore-uri", thriftHive.getThriftConnectionUri());
    System.setProperty("aws.s3.endpoint", endpoint);
    System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");
    System.setProperty("com.amazonaws.services.s3.disablePutObjectMD5Validation", "true");

    amazonS3 = ContainerTestUtils.s3Client(s3Container, AWS_REGION);
    amazonS3.createBucket(new CreateBucketRequest(BUCKET, AWS_REGION));
  }


  @Test
  public void test() throws TException {

    String path = "s3a://" + BUCKET + "/" + OBJECT_KEY1;

    String uri = thriftHive.getThriftConnectionUri();
    // System.out.println("*************** URI: " + uri);
    // HiveConf conf = new HiveConf();
    // conf.set("hive.metastore.uris", uri);
    // HiveMetaStoreClient client2 = new HiveMetaStoreClient(conf);
    // System.out.println("Checking a client can be made: " + client2);

    HiveMetaStoreClient client = thriftHive.client();
    System.out.println("TESTING - " + client);

    hiveTestUtils.createUnpartitionedTable(client, path);

    boolean result = client.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE);
    System.out.println("************** RESULT: " + result);
  }

  public int getAvailablePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new RuntimeException("Unable to find an available port", e);
    }
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
