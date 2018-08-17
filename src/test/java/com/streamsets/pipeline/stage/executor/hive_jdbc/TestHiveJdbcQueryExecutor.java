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
package com.streamsets.pipeline.stage.executor.hive_jdbc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;


public class TestHiveJdbcQueryExecutor {

  private final String JDBC_USER = "";
  private final String JDBC_PASSWD = "";
  private final String JDBC_DB = "test";
  private final String JDBC_CONNECTION = "jdbc:hive2://localhost:10000/" + JDBC_DB;

  private Connection connection;


  @Before
  public void setUp() throws Exception {
    connection = DriverManager.getConnection(JDBC_CONNECTION, JDBC_USER, JDBC_PASSWD);

    try(Statement stmt = connection.createStatement()) {
      stmt.executeQuery("CREATE TABLE IF NOT EXISTS origin (id int, name VARCHAR(50))");
    }
  }


  @Test
  public void testSampleQuery() throws Exception{

      try(Statement stmt = connection.createStatement()) {

          try(ResultSet rs=stmt.executeQuery("SELECT * FROM origin")){
              assertFalse(rs.next());
          }

      }

    }

  @After
  public void tearDown() throws Exception {
      if (connection != null) {

          try (Statement stmt = connection.createStatement()) {
              stmt.executeQuery("DROP TABLE IF EXISTS origin");
          }

          connection.close();
      }
  }


}
