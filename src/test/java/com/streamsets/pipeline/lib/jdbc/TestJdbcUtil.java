/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.jdbc;

import com.jolbox.bonecp.BoneCPDataSource;
import com.jolbox.bonecp.BoneCPConfig;
import org.junit.After;
import org.junit.Before;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;


public class TestJdbcUtil {

  private static final long MINUS_2HRS_OFFSET = -7200000L;
  private final String username = "";
  private final String password = "";
  private final String database = "test";
  private final String hiveConnectionString = "jdbc:hive2://localhost:10000/" + database;
  private final String schema = "SCHEMA_TEST";
  private final String tableName = "MYAPP";
  private final String emptyTableName = "EMPTY_TABLE";

  private BoneCPPoolConfigBean createConfigBean() {
    BoneCPPoolConfigBean bean = new BoneCPPoolConfigBean();
    bean.connectionString = hiveConnectionString;
    bean.useCredentials = true;
    bean.username = username;
    bean.password = password;

    return bean;
  }

  private Connection connection;

  @Before
  public void setUp() throws SQLException {
    // Create a table in Hive.
    BoneCPConfig config = new BoneCPConfig();
    config.setJdbcUrl(hiveConnectionString);
    config.setUsername(username);
    config.setPassword(password);
    BoneCPDataSource dataSource = new BoneCPDataSource(config);

    connection = dataSource.getConnection();
    try (Statement statement = connection.createStatement()) {
      // Setup table
      statement.executeQuery("CREATE SCHEMA IF NOT EXISTS " + schema + "");
      statement.executeQuery(
          "CREATE TABLE IF NOT EXISTS " + schema + "." + tableName +
              "(P_ID INT, MSG VARCHAR(255))"
      );
      statement.executeQuery(
              "CREATE TABLE IF NOT EXISTS " + schema + "." + emptyTableName +
                      "(P_ID TIMESTAMP)"
      );
    }
  }

  @After
  public void tearDown() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      // Setup table
      statement.executeQuery("DROP TABLE IF EXISTS " + schema + ".MYAPP");
    }
    // Last open connection terminates Hive
    connection.close();
  }



}
