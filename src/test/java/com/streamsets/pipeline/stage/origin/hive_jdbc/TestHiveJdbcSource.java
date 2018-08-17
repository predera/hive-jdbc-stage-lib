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
package com.streamsets.pipeline.stage.origin.hive_jdbc;


import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.jdbc.BoneCPPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.UnknownTypeAction;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("Duplicates")
public class TestHiveJdbcSource {
  private static final Logger LOG = LoggerFactory.getLogger(TestHiveJdbcSource.class);
  private static final int BATCH_SIZE = 1000;
  private static final int CLOB_SIZE = 1000;

  private final String username = "";
  private final String password = "";
  private final String database = "test";

  private final String hiveConnectionString = "jdbc:hive2://localhost:10000/" + database;
  private final String query = "SELECT * FROM TEST.TEST_TABLE WHERE P_ID > ${offset} ORDER BY P_ID ASC LIMIT 10";
  private final String queryNonIncremental = "SELECT * FROM TEST.TEST_TABLE LIMIT 10";
  private final String queryUnknownType = "SELECT * FROM TEST.TEST_UNKNOWN_TYPE WHERE P_ID > ${offset} ORDER BY P_ID ASC LIMIT 10";
  private final String initialOffset = "0";
  private final long queryInterval = 0L;

  private Connection connection = null;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws SQLException {
    // Create a table in Hive and put some data in it for querying.
    connection = DriverManager.getConnection(hiveConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      // Setup table
      statement.executeQuery("CREATE DATABASE IF NOT EXISTS TEST");
      statement.executeQuery(
          "CREATE TABLE IF NOT EXISTS TEST.TEST_TABLE " +
              "(p_id INT, first_name VARCHAR(255), last_name VARCHAR(255))"
      );
      statement.executeQuery(
          "CREATE TABLE IF NOT EXISTS TEST.TEST_ARRAY " +
              "(p_id INT, non_scalar ARRAY<string>)"
      );

      statement.executeQuery(
          "CREATE TABLE IF NOT EXISTS TEST.TEST_JDBC_NS_HEADERS " +
              "(p_id INT, dec DECIMAL(2, 1))"
      );
      statement.executeQuery(
          "CREATE TABLE IF NOT EXISTS TEST.TEST_NULL " +
              "(p_id INT, name VARCHAR(255), number int, ts TIMESTAMP)"
      );
      statement.executeQuery(
        "CREATE TABLE IF NOT EXISTS TEST.TEST_TIMES " +
          "(p_id INT, d DATE, t STRING, ts TIMESTAMP)"
      );
      statement.executeQuery(
              "CREATE TABLE IF NOT EXISTS TEST.TEST_UNKNOWN_TYPE " +
              "(p_id INT, geo STRING)"
      );
    }
  }

  @After
  public void tearDown() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      // Setup table
      statement.executeQuery("DROP TABLE IF EXISTS TEST.TEST_TABLE");
      statement.executeQuery("DROP TABLE IF EXISTS TEST.TEST_ARRAY");
      statement.executeQuery("DROP TABLE IF EXISTS TEST.TEST_JDBC_NS_HEADERS");
      statement.executeQuery("DROP TABLE IF EXISTS TEST.TEST_NULL");
      statement.executeQuery("DROP TABLE IF EXISTS TEST.TEST_TIMES");
      statement.executeQuery("DROP TABLE IF EXISTS TEST.TEST_UNKNOWN_TYPE");
    }

    // Last open connection terminates Hive
    connection.close();
  }

  private BoneCPPoolConfigBean createConfigBean(String connectionString, String username, String password) {
    BoneCPPoolConfigBean bean = new BoneCPPoolConfigBean();
    bean.connectionString = connectionString;
    bean.useCredentials = true;
    bean.username = username;
    bean.password = password;

    return bean;
  }

  @Test
  public void testNonIncrementalMode() throws Exception {
    HiveJdbcSource origin = new HiveJdbcSource(
        false,
        queryNonIncremental,
        "",
        "",
        false,
        "",
        1000,
        HiveJdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queryInterval, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(hiveConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE
        );
    SourceRunner runner = new SourceRunner.Builder(HiveJdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 2);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(0, parsedRecords.size());

      assertEquals("", output.getNewOffset());

      // Check that the remaining rows in the initial cursor are read.
      output = runner.runProduce(output.getNewOffset(), 100);
      parsedRecords = output.getRecords().get("lane");
      assertEquals(0, parsedRecords.size());


      // Check that new rows are loaded.
      //runInsertNewRows();
      output = runner.runProduce(output.getNewOffset(), 100);
      parsedRecords = output.getRecords().get("lane");
      assertEquals(0, parsedRecords.size());

      assertEquals("", output.getNewOffset());

      // Check that older rows are loaded.
      //runInsertOldRows();
      output = runner.runProduce(output.getNewOffset(), 100);
      parsedRecords = output.getRecords().get("lane");
      assertEquals(0, parsedRecords.size());
    } finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testDriverNotFound() throws Exception {
    HiveJdbcSource origin = new HiveJdbcSource(
        true,
        query,
        initialOffset,
        "P_ID",
        false,
        "",
        1000,
        HiveJdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queryInterval, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean("some bad connection string", username, password),
        UnknownTypeAction.STOP_PIPELINE
    );

    SourceRunner runner = new SourceRunner.Builder(HiveJdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
  }

  @Test
  public void testBadConnectionString() throws Exception {
    // Want to get the Hive driver, but give it a bad connection string, testing fix for SDC-5025
    BoneCPPoolConfigBean configBean = createConfigBean("some bad connection string", username, password);
    configBean.driverClassName = "org.apache.hive.jdbc.HiveDriver";

    HiveJdbcSource origin = new HiveJdbcSource(
        true,
        query,
        initialOffset,
        "P_ID",
        false,
        "",
        1000,
        HiveJdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queryInterval, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        configBean,
        UnknownTypeAction.STOP_PIPELINE
    );

    SourceRunner runner = new SourceRunner.Builder(HiveJdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
  }

  @Test
  public void testMissingWhereClause() throws Exception {
    String queryMissingWhere = "SELECT * FROM TEST.TEST_TABLE ORDER BY P_ID ASC LIMIT 10";
    HiveJdbcSource origin = new HiveJdbcSource(
        true,
        queryMissingWhere,
        initialOffset,
        "P_ID",
        false,
        "",
        1000,
        HiveJdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queryInterval, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(hiveConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE
    );

    SourceRunner runner = new SourceRunner.Builder(HiveJdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains("Query must include 'WHERE' clause."));
  }

  @Test
  public void testMissingOrderByClause() throws Exception {
    String queryMissingOrderBy = "SELECT * FROM TEST.TEST_TABLE WHERE P_ID > ${offset} LIMIT 10";
    HiveJdbcSource origin = new HiveJdbcSource(
        true,
        queryMissingOrderBy,
        initialOffset,
        "P_ID",
        false,
        "",
        1000,
        HiveJdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queryInterval, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(hiveConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE
    );

    SourceRunner runner = new SourceRunner.Builder(HiveJdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    for (Stage.ConfigIssue issue : issues) {
      LOG.info(issue.toString());
    }
    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains("Query must include 'ORDER BY' clause."));
  }

  @Test
  public void testMissingWhereAndOrderByClause() throws Exception {
    String queryMissingWhereAndOrderBy = "SELECT * FROM TEST.TEST_TABLE";
    HiveJdbcSource origin = new HiveJdbcSource(
        true,
        queryMissingWhereAndOrderBy,
        initialOffset,
        "P_ID",
        false,
        "",
        1000,
        HiveJdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queryInterval, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(hiveConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE
    );

    SourceRunner runner = new SourceRunner.Builder(HiveJdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(2, issues.size());
  }

  @Test
  public void testInvalidQuery() throws Exception {
    String queryInvalid = "SELET * FORM TABLE WHERE P_ID > ${offset} ORDER BY P_ID LIMIT 10";
    HiveJdbcSource origin = new HiveJdbcSource(
        true,
        queryInvalid,
        initialOffset,
        "P_ID",
        false,
        "",
        1000,
        HiveJdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queryInterval, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(hiveConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE
    );

    SourceRunner runner = new SourceRunner.Builder(HiveJdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
  }

  @Test
  public void testMultiLineQuery() throws Exception {
    String queryInvalid = "SELECT * FROM TEST.TEST_TABLE WHERE\nP_ID > ${offset}\nORDER BY P_ID ASC LIMIT 10";
    HiveJdbcSource origin = new HiveJdbcSource(
        true,
        queryInvalid,
        initialOffset,
        "P_ID",
        false,
        "",
        1000,
        HiveJdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queryInterval, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(hiveConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE
      );

    SourceRunner runner = new SourceRunner.Builder(HiveJdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(0, issues.size());
  }

  @Test
  public void testQualifiedOffsetColumnInQuery() throws Exception {
    final String query = "SELECT * FROM TEST.TEST_TABLE T WHERE T.P_ID > ${offset} ORDER BY T.P_ID ASC LIMIT 10";

    HiveJdbcSource origin = new HiveJdbcSource(
        true,
        query,
        "1",
        "P_ID",
        false,
        "FIRST_NAME",
        1,
        HiveJdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queryInterval, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(hiveConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE
    );
    SourceRunner runner = new SourceRunner.Builder(HiveJdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(0, issues.size());
  }

  @Test
  public void testDuplicateColumnLabels() throws Exception {
    final String query = "SELECT * FROM TEST.TEST_TABLE T, TEST.TEST_TABLE TB WHERE T.P_ID > ${offset} " +
        "ORDER BY T.P_ID ASC LIMIT 10";

    HiveJdbcSource origin = new HiveJdbcSource(
        true,
        query,
        "1",
        "P_ID",
        false,
        "FIRST_NAME",
        1,
        HiveJdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queryInterval, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(hiveConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE
    );
    SourceRunner runner = new SourceRunner.Builder(HiveJdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(3, issues.size());
  }

  @Test
  public void testPrefixedOffsetColumn() throws Exception {
    final String query = "SELECT * FROM TEST.TEST_TABLE T WHERE T.P_ID > ${offset} ORDER BY T.P_ID ASC LIMIT 10";

    HiveJdbcSource origin = new HiveJdbcSource(
        true,
        query,
        "1",
        "T.P_ID",
        false,
        "FIRST_NAME",
        1,
        HiveJdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queryInterval, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(hiveConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE
    );
    SourceRunner runner = new SourceRunner.Builder(HiveJdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
  }

  @Test
  public void testEmptyResultSet() throws Exception {
    Statement statement = connection.createStatement();
    statement.execute("TRUNCATE TABLE TEST.TEST_TABLE");

    HiveJdbcSource origin = new HiveJdbcSource(
        true,
        query,
        initialOffset,
        "P_ID",
        false,
        "FIRST_NAME",
        1000,
        HiveJdbcRecordType.LIST_MAP,
        new CommonSourceConfigBean(queryInterval, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(hiveConnectionString, username, password),
        UnknownTypeAction.STOP_PIPELINE
    );
    SourceRunner runner = new SourceRunner.Builder(HiveJdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(0, parsedRecords.size());

      assertEquals(initialOffset, output.getNewOffset());
    } finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testQueryReplaceUpperOffset() throws Exception {
    HiveJdbcSource origin = new HiveJdbcSource(
        true,
        queryUnknownType,
        "0",
        "P_ID",
        false,
        "",
        1000,
        HiveJdbcRecordType.LIST_MAP,
        // Using "0" leads to SDC-6429
        new CommonSourceConfigBean(10, BATCH_SIZE, CLOB_SIZE, CLOB_SIZE),
        false,
        "",
        createConfigBean(hiveConnectionString, username, password),
        UnknownTypeAction.CONVERT_TO_STRING
    );

    final String lastSourceOffset = "10";
    final String query = "${OFFSET}${offset}";

    String result = origin.prepareQuery(query, lastSourceOffset);
    Assert.assertEquals(result, lastSourceOffset+lastSourceOffset);
  }
}
