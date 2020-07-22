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

import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;

import static com.expediagroup.beekeeper.core.model.HousekeepingStatus.SCHEDULED;
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.expediagroup.beekeeper.core.model.HousekeepingMetadata;
import com.expediagroup.beekeeper.core.model.HousekeepingPath;

class MySqlTestUtils {

  private static final String DROP_TABLE = "DROP TABLE IF EXISTS %s.%s;";
  private static final String SELECT_TABLE_WITH_WHERE = "SELECT * FROM %s.%s where lifecycle_type = '%s' order by path;";
  private static final String SELECT_TABLE = "SELECT * FROM %s.%s %s;";
  private static final String INSERT_TO_TABLE = "INSERT INTO %s.%s (%s) VALUES (%s);";

  private final Connection connection;

  MySqlTestUtils(String jdbcUrl, String username, String password) throws SQLException {
    connection = DriverManager.getConnection(jdbcUrl, username, password);
  }

  void close() throws SQLException {
    connection.close();
  }

  void insertToTable(String database, String table, String fields, String values) throws SQLException {
    connection.createStatement().executeUpdate(format(INSERT_TO_TABLE, database, table, fields, values));
  }

  int getTableRowCount(String database, String table, String additionalFilters) throws SQLException {
    return getTableRowCount(format(SELECT_TABLE, database, table, additionalFilters));
  }

  int getTableRowCount(String database, String table) throws SQLException {
    return getTableRowCount(format(SELECT_TABLE, database, table, ""));
  }

  private int getTableRowCount(String statement) throws SQLException {
    ResultSet resultSet = getTableRows(statement);
    resultSet.last();
    int rowsInTable = resultSet.getRow();
    return rowsInTable;
  }

  ResultSet getTableRows(String database, String table, String additionalFilters) throws SQLException {
    return getTableRows(format(SELECT_TABLE, database, table, additionalFilters));
  }

  ResultSet getTableRows(String database, String table) throws SQLException {
    return getTableRows(format(SELECT_TABLE, database, table, ""));
  }

  private ResultSet getTableRows(String statement) throws SQLException {
    return connection.createStatement().executeQuery(statement);
  }

  void dropTable(String database, String table) throws SQLException {
    connection.createStatement().executeUpdate(format(DROP_TABLE, database, table));
  }

  void insertPath(String beekeeperTableName, String path, String table) throws SQLException {
    String lifecycleType = UNREFERENCED.toString().toLowerCase();

    String fields = String.join(", ", PATH, HOUSEKEEPING_STATUS, CLEANUP_DELAY, CLEANUP_TIMESTAMP, TABLE_NAME,
        LIFECYCLE_TYPE);
    String values = Stream.of(path, SCHEDULED.toString(), "PT1S", Timestamp.valueOf(LocalDateTime.now(UTC)
        .minus(1L, ChronoUnit.DAYS))
        .toString(), table, lifecycleType)
        .map(s -> "\"" + s + "\"")
        .collect(Collectors.joining(", "));

    connection.createStatement()
        .executeUpdate(format(INSERT_TO_TABLE, "beekeeper", beekeeperTableName, fields, values));
  }

  int unreferencedRowsInTable(String table) throws SQLException {
    return rowsInTable("beekeeper", table, UNREFERENCED.toString());
  }

  int unreferencedRowsInTable(String database, String table) throws SQLException {
    return rowsInTable(database, table, UNREFERENCED.toString());
  }

  private int rowsInTable(String database, String table, String cleanupType) throws SQLException {
    ResultSet resultSet = connection.createStatement()
        .executeQuery(format(SELECT_TABLE_WITH_WHERE, database, table, cleanupType));
    resultSet.last();
    int rowsInTable = resultSet.getRow();
    return rowsInTable;
  }

  void dropTable(String table) throws SQLException {
    connection.createStatement().executeUpdate(format(DROP_TABLE, "beekeeper", table));
  }

  List<HousekeepingPath> getUnreferencedHousekeepingPaths(String beekeeperTableName) throws SQLException {
    return getHousekeepingPaths(beekeeperTableName, UNREFERENCED.toString());
  }

  List<HousekeepingPath> getHousekeepingPaths(String table, String cleanupType) throws SQLException {
    return getHousekeepingPaths("beekeeper", table, cleanupType);
  }

  List<HousekeepingPath> getHousekeepingPaths(String database, String table, String lifecycleType)
      throws SQLException {
    ResultSet resultSet = connection.createStatement()
        .executeQuery(format(SELECT_TABLE_WITH_WHERE, database, table, lifecycleType));
    List<HousekeepingPath> paths = new ArrayList<>();

    while (resultSet.next()) {
      paths.add(mapToHousekeepingPath(resultSet));
    }

    return paths;
  }

  List<HousekeepingMetadata> getHousekeepingMetadata(String database, String table, String lifecycleType)
      throws SQLException {
    ResultSet resultSet = connection.createStatement()
        .executeQuery(format(SELECT_TABLE_WITH_WHERE, database, table, lifecycleType));
    List<HousekeepingMetadata> metadata = new ArrayList<>();

    while (resultSet.next()) {
      metadata.add(mapToHousekeepingMetadata(resultSet));
    }

    return metadata;
  }
}
