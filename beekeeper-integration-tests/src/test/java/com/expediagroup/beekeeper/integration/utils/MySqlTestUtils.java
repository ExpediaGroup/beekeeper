/**
 * Copyright (C) 2019-2021 Expedia, Inc.
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
package com.expediagroup.beekeeper.integration.utils;

import static java.lang.String.format;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MySqlTestUtils {

  private static final String DROP_TABLE = "DROP TABLE IF EXISTS %s.%s;";
  private static final String SELECT_TABLE = "SELECT * FROM %s.%s %s;";
  private static final String INSERT_TO_TABLE = "INSERT INTO %s.%s (%s) VALUES (%s);";

  private final Connection connection;

  public MySqlTestUtils(String jdbcUrl, String username, String password) throws SQLException {
    connection = DriverManager.getConnection(jdbcUrl, username, password);
  }

  public void close() throws SQLException {
    connection.close();
  }

  public void insertToTable(String database, String table, String fields, String values) throws SQLException {
    connection.createStatement().executeUpdate(format(INSERT_TO_TABLE, database, table, fields, values));
  }

  public int getTableRowCount(String database, String table) throws SQLException {
    return getTableRowCount(format(SELECT_TABLE, database, table, ""));
  }

  public int getTableRowCount(String database, String table, String additionalFilters) throws SQLException {
    return getTableRowCount(format(SELECT_TABLE, database, table, additionalFilters));
  }

  private int getTableRowCount(String statementString) throws SQLException {
    Statement statement;
    statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
        ResultSet.CONCUR_READ_ONLY);
    ResultSet resultSet = statement.executeQuery(statementString);
    resultSet.last();
    return resultSet.getRow();
  }

  public ResultSet getTableRows(String database, String table, String additionalFilters) throws SQLException {
    return getTableRows(format(SELECT_TABLE, database, table, additionalFilters));
  }

  public ResultSet getTableRows(String database, String table) throws SQLException {
    return getTableRows(format(SELECT_TABLE, database, table, ""));
  }

  private ResultSet getTableRows(String statement) throws SQLException {
    return connection.createStatement().executeQuery(statement);
  }

  public void dropTable(String database, String table) throws SQLException {
    connection.createStatement().executeUpdate(format(DROP_TABLE, database, table));
  }
}
