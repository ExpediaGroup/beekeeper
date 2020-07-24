package com.expediagroup.beekeeper.integration;

import static java.lang.String.format;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.AWS_REGION;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.CLEANUP_ATTEMPTS_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.CLEANUP_ATTEMPTS_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.CLEANUP_DELAY_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.CLEANUP_DELAY_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.CLEANUP_TIMESTAMP_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.CLIENT_ID_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.CREATION_TIMESTAMP_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.CREATION_TIMESTAMP_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.DATABASE_NAME_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.DATABASE_NAME_VALUE;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.HOUSEKEEPING_STATUS_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.ID_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.LIFECYCLE_TYPE_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.MODIFIED_TIMESTAMP_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.PARTITION_NAME_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.PATH_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.TABLE_NAME_FIELD;
import static com.expediagroup.beekeeper.integration.CommonTestVariables.TABLE_NAME_VALUE;
import static com.expediagroup.beekeeper.integration.utils.ResultSetToHousekeepingEntityMapper.mapToHousekeepingMetadata;
import static com.expediagroup.beekeeper.integration.utils.ResultSetToHousekeepingEntityMapper.mapToHousekeepingPath;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.MySQLContainer;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;
import com.expediagroup.beekeeper.core.model.LifecycleEventType;
import com.expediagroup.beekeeper.integration.utils.ContainerTestUtils;
import com.expediagroup.beekeeper.integration.utils.MySqlTestUtils;

public abstract class BeekeeperIntegrationTestBase {

  // AWS VARIABLES
  private static final String AWS_ACCESS_KEY_ID = "access-key";
  private static final String AWS_SECRET_KEY = "secret-key";

  // BEEKEEPER DB VARIABLES
  private static final String BEEKEEPER_DB_NAME = "beekeeper";
  private static final String BEEKEEPER_FLYWAY_TABLE = "flyway_schema_history";
  private static final String BEEKEEPER_HOUSEKEEPING_PATH_TABLE_NAME = "housekeeping_path";
  private static final String BEEKEEPER_HOUSEKEEPING_METADATA_TABLE_NAME = "housekeeping_metadata";

  // FIELDS TO INSERT INTO BEEKEEPER TABLES
  private Long id = 1L;
  private static final String HOUSEKEEPING_PATH_FIELDS = String.join(",", ID_FIELD, PATH_FIELD, DATABASE_NAME_FIELD,
      TABLE_NAME_FIELD, HOUSEKEEPING_STATUS_FIELD, CREATION_TIMESTAMP_FIELD, MODIFIED_TIMESTAMP_FIELD,
      CLEANUP_TIMESTAMP_FIELD, CLEANUP_DELAY_FIELD, CLEANUP_ATTEMPTS_FIELD, CLIENT_ID_FIELD, LIFECYCLE_TYPE_FIELD);
  private static final String HOUSEKEEPING_METADATA_FIELDS = String.join(",", ID_FIELD, PATH_FIELD, DATABASE_NAME_FIELD,
      TABLE_NAME_FIELD, PARTITION_NAME_FIELD, HOUSEKEEPING_STATUS_FIELD, CREATION_TIMESTAMP_FIELD,
      MODIFIED_TIMESTAMP_FIELD, CLEANUP_TIMESTAMP_FIELD, CLEANUP_DELAY_FIELD, CLEANUP_ATTEMPTS_FIELD, CLIENT_ID_FIELD,
      LIFECYCLE_TYPE_FIELD);
  private static final String LIFE_CYCLE_FILTER = "WHERE " + LIFECYCLE_TYPE_FIELD + " = '%s' ORDER BY " + PATH_FIELD;
  private static final String LIFE_CYCLE_AND_UPDATE_FILTER = "WHERE " + LIFECYCLE_TYPE_FIELD + " = '%s'"
      + " AND " + MODIFIED_TIMESTAMP_FIELD + " > " + CREATION_TIMESTAMP_FIELD
      + " ORDER BY " + PATH_FIELD;

  // MySQL DB CONTAINER AND UTILS
  private static MySQLContainer mySQLContainer;
  private static MySqlTestUtils mySQLTestUtils;

  protected final ExecutorService executorService = Executors.newFixedThreadPool(1);

  @BeforeAll
  protected static void initMySQLContainer() throws SQLException {
    mySQLContainer = ContainerTestUtils.mySqlContainer();
    mySQLContainer.start();

    String jdbcUrl = mySQLContainer.getJdbcUrl() + "?useSSL=false";
    String username = mySQLContainer.getUsername();
    String password = mySQLContainer.getPassword();

    System.setProperty("spring.datasource.url", jdbcUrl);
    System.setProperty("spring.datasource.username", username);
    System.setProperty("spring.datasource.password", password);
    System.setProperty("aws.region", AWS_REGION);
    System.setProperty("aws.accessKeyId", AWS_ACCESS_KEY_ID);
    System.setProperty("aws.secretKey", AWS_SECRET_KEY);

    mySQLTestUtils = new MySqlTestUtils(jdbcUrl, username, password);
  }

  @AfterAll
  protected static void destroyMySQLContainer() throws SQLException {
    mySQLTestUtils.close();
    mySQLContainer.stop();
  }

  @BeforeEach
  public void dropMySQLTables() throws SQLException {
    mySQLTestUtils.dropTable(BEEKEEPER_DB_NAME, BEEKEEPER_FLYWAY_TABLE);
    mySQLTestUtils.dropTable(BEEKEEPER_DB_NAME, BEEKEEPER_HOUSEKEEPING_PATH_TABLE_NAME);
    mySQLTestUtils.dropTable(BEEKEEPER_DB_NAME, BEEKEEPER_HOUSEKEEPING_METADATA_TABLE_NAME);
  }

  protected void insertUnreferencedPath(String path) throws SQLException {
    HousekeepingPath housekeepingPath = createHousekeepingPath(path, UNREFERENCED);
    housekeepingPath.setCleanupTimestamp(housekeepingPath.getCleanupTimestamp().minus(Duration.ofDays(1)));
    String values = Stream.of(housekeepingPath.getId().toString(), housekeepingPath.getPath(),
        housekeepingPath.getDatabaseName(),
        housekeepingPath.getTableName(), housekeepingPath.getHousekeepingStatus().toString(),
        housekeepingPath.getCreationTimestamp().toString(), housekeepingPath.getModifiedTimestamp().toString(),
        housekeepingPath.getCleanupTimestamp().toString(), housekeepingPath.getCleanupDelay().toString(),
        String.valueOf(housekeepingPath.getCleanupAttempts()), housekeepingPath.getClientId(),
        housekeepingPath.getLifecycleType())
        .map(s -> s == null ? null : "\"" + s + "\"")
        .collect(Collectors.joining(", "));

    mySQLTestUtils
        .insertToTable(BEEKEEPER_DB_NAME, BEEKEEPER_HOUSEKEEPING_PATH_TABLE_NAME, HOUSEKEEPING_PATH_FIELDS, values);
  }

  protected void insertExpiredMetadata(String path, String partitionName)
      throws SQLException {
    HousekeepingMetadata metadata = createHousekeepingMetadata(path, partitionName, EXPIRED);
    String values = Stream.of(metadata.getId().toString(), metadata.getPath(), metadata.getDatabaseName(),
        metadata.getTableName(), metadata.getPartitionName(), metadata.getHousekeepingStatus().toString(),
        metadata.getCreationTimestamp().toString(), metadata.getModifiedTimestamp().toString(),
        metadata.getCleanupTimestamp().toString(), metadata.getCleanupDelay().toString(),
        String.valueOf(metadata.getCleanupAttempts()), metadata.getClientId(), metadata.getLifecycleType())
        .map(s -> s == null ? null : "\"" + s + "\"")
        .collect(Collectors.joining(", "));

    mySQLTestUtils
        .insertToTable(BEEKEEPER_DB_NAME, BEEKEEPER_HOUSEKEEPING_METADATA_TABLE_NAME, HOUSEKEEPING_METADATA_FIELDS,
            values);
  }

  protected int getUnreferencedPathsRowCount() throws SQLException {
    return mySQLTestUtils
        .getTableRowCount(BEEKEEPER_DB_NAME, BEEKEEPER_HOUSEKEEPING_PATH_TABLE_NAME,
            format(LIFE_CYCLE_FILTER, UNREFERENCED));
  }

  protected int getExpiredMetadataRowCount() throws SQLException {
    return mySQLTestUtils
        .getTableRowCount(BEEKEEPER_DB_NAME, BEEKEEPER_HOUSEKEEPING_METADATA_TABLE_NAME,
            format(LIFE_CYCLE_FILTER, EXPIRED));
  }

  protected int getUpdatedExpiredMetadataRowCount() throws SQLException {
    return mySQLTestUtils
        .getTableRowCount(BEEKEEPER_DB_NAME, BEEKEEPER_HOUSEKEEPING_METADATA_TABLE_NAME,
            format(LIFE_CYCLE_AND_UPDATE_FILTER, EXPIRED));
  }

  protected List<HousekeepingPath> getUnreferencedPaths() throws SQLException {
    List<HousekeepingPath> paths = new ArrayList<>();
    ResultSet resultSet = mySQLTestUtils
        .getTableRows(BEEKEEPER_DB_NAME, BEEKEEPER_HOUSEKEEPING_PATH_TABLE_NAME,
            format(LIFE_CYCLE_FILTER, UNREFERENCED));

    while (resultSet.next()) {
      paths.add(mapToHousekeepingPath(resultSet));
    }

    return paths;
  }

  protected List<HousekeepingMetadata> getExpiredMetadata() throws SQLException {
    List<HousekeepingMetadata> metadata = new ArrayList<>();
    ResultSet resultSet = mySQLTestUtils
        .getTableRows(BEEKEEPER_DB_NAME, BEEKEEPER_HOUSEKEEPING_METADATA_TABLE_NAME,
            format(LIFE_CYCLE_FILTER, EXPIRED));

    while (resultSet.next()) {
      metadata.add(mapToHousekeepingMetadata(resultSet));
    }

    return metadata;
  }

  private HousekeepingPath createHousekeepingPath(String path, LifecycleEventType lifecycleEventType) {
    return new HousekeepingPath.Builder()
        .id(id++)
        .path(path)
        .databaseName(DATABASE_NAME_VALUE)
        .tableName(TABLE_NAME_FIELD)
        .housekeepingStatus(SCHEDULED)
        .creationTimestamp(CREATION_TIMESTAMP_VALUE)
        .modifiedTimestamp(CREATION_TIMESTAMP_VALUE)
        .cleanupDelay(Duration.parse(CLEANUP_DELAY_VALUE))
        .cleanupAttempts(CLEANUP_ATTEMPTS_VALUE)
        .lifecycleType(lifecycleEventType.toString())
        .clientId(CLIENT_ID_FIELD)
        .build();
  }

  private HousekeepingMetadata createHousekeepingMetadata(String path, String partitionName,
      LifecycleEventType lifecycleEventType) {
    return new HousekeepingMetadata.Builder()
        .id(id++)
        .path(path)
        .databaseName(DATABASE_NAME_VALUE)
        .tableName(TABLE_NAME_VALUE)
        .partitionName(partitionName)
        .housekeepingStatus(SCHEDULED)
        .creationTimestamp(CREATION_TIMESTAMP_VALUE)
        .modifiedTimestamp(CREATION_TIMESTAMP_VALUE)
        .cleanupDelay(Duration.parse(CLEANUP_DELAY_VALUE))
        .cleanupAttempts(CLEANUP_ATTEMPTS_VALUE)
        .lifecycleType(lifecycleEventType.toString())
        .clientId(CLIENT_ID_FIELD)
        .build();
  }
}
