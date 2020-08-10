package com.expediagroup.beekeeper.integration;

import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.SECRET_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import static com.expediagroup.beekeeper.cleanup.monitoring.DeletedMetadataReporter.DRY_RUN_METRIC_NAME;
import static com.expediagroup.beekeeper.cleanup.monitoring.DeletedMetadataReporter.METRIC_NAME;
import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.DELETED;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.AWS_REGION;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.DATABASE_NAME_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.TABLE_NAME_VALUE;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
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

import com.hotels.beeju.extensions.ThriftHiveMetaStoreJUnitExtension;

public class BeekeeperDryRunMetadataCleanupIntegrationTest extends BeekeeperIntegrationTestBase  {

  private static final int TIMEOUT = 30;
  private static final String SCHEDULER_DELAY_MS = "5000";

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
    System.setProperty("properties.dry-run-enabled", "true");
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
  public void dryRunCleanupUnpartitionedTable() throws TException, SQLException {
    hiveTestUtils.createTable(metastoreClient, UNPARTITIONED_TABLE_PATH, TABLE_NAME_VALUE, false);
    amazonS3.putObject(BUCKET, UNPARTITIONED_OBJECT_KEY, TABLE_DATA);

    insertExpiredMetadata(UNPARTITIONED_TABLE_PATH, null);
    await()
        .atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getExpiredMetadata().get(0).getHousekeepingStatus() == DELETED);

    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, UNPARTITIONED_OBJECT_KEY)).isTrue();
  }

  @Test
  public void dryRunCleanupPartitionedTable() throws Exception {
    Table table = hiveTestUtils.createTable(metastoreClient, PARTITIONED_TABLE_PATH, TABLE_NAME_VALUE, true);

    hiveTestUtils.addPartitionsToTable(metastoreClient, PARTITION_ROOT_PATH, table, PARTITION_VALUES, TABLE_DATA);

    amazonS3.putObject(BUCKET, PARTITIONED_TABLE_OBJECT_KEY, "");
    amazonS3.putObject(BUCKET, PARTITIONED_OBJECT_KEY, TABLE_DATA);

    insertExpiredMetadata(PARTITIONED_TABLE_PATH, null);
    insertExpiredMetadata(PARTITION_PATH, PARTITION_NAME);
    await()
        .atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getExpiredMetadata().get(1).getHousekeepingStatus() == DELETED);

    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_TABLE_OBJECT_KEY)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, PARTITIONED_OBJECT_KEY)).isTrue();
  }

  @Test
  public void dryRunMetrics() throws SQLException, TException {
    Table table = hiveTestUtils.createTable(metastoreClient, UNPARTITIONED_TABLE_PATH, TABLE_NAME_VALUE, false);
    amazonS3.putObject(BUCKET, UNPARTITIONED_OBJECT_KEY, "");

    insertExpiredMetadata(UNPARTITIONED_TABLE_PATH, null);
    await().atMost(TIMEOUT, TimeUnit.SECONDS)
        .until(() -> getExpiredMetadata().get(0).getHousekeepingStatus() == DELETED);

    assertThat(metastoreClient.tableExists(DATABASE_NAME_VALUE, TABLE_NAME_VALUE)).isTrue();
    assertThat(amazonS3.doesObjectExist(BUCKET, UNPARTITIONED_OBJECT_KEY)).isTrue();
    assertMetrics();
  }
  private void assertMetrics() {
    Set<MeterRegistry> meterRegistry = ((CompositeMeterRegistry) BeekeeperMetadataCleanup.meterRegistry()).getRegistries();
    assertThat(meterRegistry).hasSize(2);
    meterRegistry.forEach(registry -> {
      List<Meter> meters = registry.getMeters();
      assertThat(meters).extracting("id", Meter.Id.class).extracting("name")
          .contains("cleanup-job", "hive-tables-deleted", "hive-table-" + DRY_RUN_METRIC_NAME);
    });
  }


}
