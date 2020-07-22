package com.expediagroup.beekeeper.integration;

import static java.lang.String.format;

import static com.expediagroup.beekeeper.core.model.LifecycleEventType.EXPIRED;
import static com.expediagroup.beekeeper.core.model.LifecycleEventType.UNREFERENCED;
import static com.expediagroup.beekeeper.integration.ResultSetToHousekeepingEntityMapper.CLEANUP_ATTEMPTS;
import static com.expediagroup.beekeeper.integration.ResultSetToHousekeepingEntityMapper.CLEANUP_DELAY;
import static com.expediagroup.beekeeper.integration.ResultSetToHousekeepingEntityMapper.CLEANUP_TIMESTAMP;
import static com.expediagroup.beekeeper.integration.ResultSetToHousekeepingEntityMapper.CLIENT_ID;
import static com.expediagroup.beekeeper.integration.ResultSetToHousekeepingEntityMapper.CREATION_TIMESTAMP;
import static com.expediagroup.beekeeper.integration.ResultSetToHousekeepingEntityMapper.DATABASE_NAME;
import static com.expediagroup.beekeeper.integration.ResultSetToHousekeepingEntityMapper.HOUSEKEEPING_STATUS;
import static com.expediagroup.beekeeper.integration.ResultSetToHousekeepingEntityMapper.ID;
import static com.expediagroup.beekeeper.integration.ResultSetToHousekeepingEntityMapper.LIFECYCLE_TYPE;
import static com.expediagroup.beekeeper.integration.ResultSetToHousekeepingEntityMapper.MODIFIED_TIMESTAMP;
import static com.expediagroup.beekeeper.integration.ResultSetToHousekeepingEntityMapper.PARTITION_NAME;
import static com.expediagroup.beekeeper.integration.ResultSetToHousekeepingEntityMapper.PATH;
import static com.expediagroup.beekeeper.integration.ResultSetToHousekeepingEntityMapper.TABLE_NAME;
import static com.expediagroup.beekeeper.integration.ResultSetToHousekeepingEntityMapper.mapToHousekeepingMetadata;
import static com.expediagroup.beekeeper.integration.ResultSetToHousekeepingEntityMapper.mapToHousekeepingPath;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.persistence.Table;

import org.testcontainers.containers.MySQLContainer;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;

public abstract class BeekeeperIntegrationTestBase {

  protected static final int TIMEOUT = 5;

  // protected static final String AWS_BUCKET = "test-path-bucket";
  protected static final String AWS_REGION = "us-west-2";
  protected static final String AWS_ACCESS_KEY_ID = "access-key";
  protected static final String AWS_SECRET_KEY = "secret-key";

  // BEEKEEPER DB VARIABLES
  private static final String BEEKEEPER_DB_NAME = "beekeeper";
  private static final String BEEKEEPER_FLYWAY_TABLE = "flyway_schema_history";
  private static final String BEEKEEPER_HOUSEKEEPING_PATH_TABLE_NAME = HousekeepingPath.class
      .getAnnotation(Table.class)
      .name();
  private static final String BEEKEEPER_HOUSEKEEPING_METADATA_TABLE_NAME = HousekeepingMetadata.class
      .getAnnotation(Table.class)
      .name();

  // FIELDS TO INSERT INTO BEEKEEPER TABLES
  private static final String HOUSEKEEPING_PATH_FIELDS = String
      .join(",", ID, PATH, DATABASE_NAME, TABLE_NAME, HOUSEKEEPING_STATUS, CREATION_TIMESTAMP, MODIFIED_TIMESTAMP,
          CLEANUP_TIMESTAMP, CLEANUP_DELAY, CLEANUP_ATTEMPTS, CLIENT_ID, LIFECYCLE_TYPE);
  private static final String HOUSEKEEPING_METADATA_FIELDS = String
      .join(",", ID, PATH, DATABASE_NAME, TABLE_NAME, PARTITION_NAME, HOUSEKEEPING_STATUS, CREATION_TIMESTAMP,
          MODIFIED_TIMESTAMP, CLEANUP_TIMESTAMP, CLEANUP_DELAY, CLEANUP_ATTEMPTS, CLIENT_ID, LIFECYCLE_TYPE);
  private static final String LIFE_CYCLE_AND_ORDER_FILTER = "WHERE lifecycle_type = '%s' ORDER BY path";

  // MySQL Database Container and Utils
  private static MySQLContainer mySQLContainer;
  private static MySqlTestUtils mySQLTestUtils;

  protected final ExecutorService executorService = Executors.newFixedThreadPool(1);

  public BeekeeperIntegrationTestBase() throws SQLException {
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

  protected static void closeMySQLConnectionAndDatabase() throws SQLException {
    mySQLTestUtils.close();
    mySQLContainer.stop();
  }

  protected static void dropTables() throws SQLException {
    mySQLTestUtils.dropTable(BEEKEEPER_DB_NAME, BEEKEEPER_FLYWAY_TABLE);
    mySQLTestUtils.dropTable(BEEKEEPER_DB_NAME, BEEKEEPER_HOUSEKEEPING_PATH_TABLE_NAME);
    mySQLTestUtils.dropTable(BEEKEEPER_DB_NAME, BEEKEEPER_HOUSEKEEPING_METADATA_TABLE_NAME);
  }

  protected static void insertHousekeepingPath(HousekeepingPath path) throws SQLException {
    String values = String
        .join(",", path.getId().toString(), path.getPath(), path.getDatabaseName(), path.getTableName(),
            path.getHousekeepingStatus().toString(), path.getCreationTimestamp().toString(),
            path.getModifiedTimestamp().toString(), path.getCleanupTimestamp().toString(),
            path.getCleanupDelay().toString(), String.valueOf(path.getCleanupAttempts()), path.getClientId(),
            path.getLifecycleType());

    mySQLTestUtils
        .insertToTable(BEEKEEPER_DB_NAME, BEEKEEPER_HOUSEKEEPING_PATH_TABLE_NAME, HOUSEKEEPING_PATH_FIELDS, values);
  }

  protected static void insertHousekeepingMetadata(HousekeepingMetadata metadata) throws SQLException {
    String values = String
        .join(",", metadata.getId().toString(), metadata.getPath(), metadata.getDatabaseName(), metadata.getTableName(),
            metadata.getPartitionName(), metadata.getHousekeepingStatus().toString(),
            metadata.getCreationTimestamp().toString(), metadata.getModifiedTimestamp().toString(),
            metadata.getCleanupTimestamp().toString(), metadata.getCleanupDelay().toString(),
            String.valueOf(metadata.getCleanupAttempts()), metadata.getClientId(), metadata.getLifecycleType());

    mySQLTestUtils
        .insertToTable(BEEKEEPER_DB_NAME, BEEKEEPER_HOUSEKEEPING_METADATA_TABLE_NAME, HOUSEKEEPING_PATH_FIELDS, values);
  }

  protected static int getUnreferencedHousekeepingPathsRowCount() throws SQLException {
    return mySQLTestUtils
        .getTableRowCount(BEEKEEPER_DB_NAME, BEEKEEPER_HOUSEKEEPING_PATH_TABLE_NAME,
            format(LIFE_CYCLE_AND_ORDER_FILTER, UNREFERENCED));
  }

  protected static List<HousekeepingPath> getUnreferencedHousekeepingPaths() throws SQLException {
    List<HousekeepingPath> paths = new ArrayList<>();
    ResultSet resultSet = mySQLTestUtils
        .getTableRows(BEEKEEPER_DB_NAME, BEEKEEPER_HOUSEKEEPING_PATH_TABLE_NAME,
            format(LIFE_CYCLE_AND_ORDER_FILTER, UNREFERENCED));

    while (resultSet.next()) {
      paths.add(mapToHousekeepingPath(resultSet));
    }

    return paths;
  }

  protected static int getExpiredHousekeepingMetadataRowCount() throws SQLException {
    return mySQLTestUtils
        .getTableRowCount(BEEKEEPER_DB_NAME, BEEKEEPER_HOUSEKEEPING_METADATA_TABLE_NAME,
            format(LIFE_CYCLE_AND_ORDER_FILTER, EXPIRED));
  }

  protected static List<HousekeepingMetadata> getExpiredHousekeepingMetadata() throws SQLException {
    List<HousekeepingMetadata> metadata = new ArrayList<>();
    ResultSet resultSet = mySQLTestUtils
        .getTableRows(BEEKEEPER_DB_NAME, BEEKEEPER_HOUSEKEEPING_METADATA_TABLE_NAME,
            format(LIFE_CYCLE_AND_ORDER_FILTER, EXPIRED));

    while (resultSet.next()) {
      metadata.add(mapToHousekeepingMetadata(resultSet));
    }

    return metadata;
  }
}
