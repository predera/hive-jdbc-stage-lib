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
package com.streamsets.pipeline.stage.processor.Hivejdbclookup;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.jdbc.BoneCPPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.DataType;
import com.streamsets.pipeline.lib.jdbc.JdbcFieldColumnMapping;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.common.MultipleValuesBehavior;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("Duplicates")
public class TestJdbcLookup {

  private final String username = "";
  private final String password = "";
  private final String database = "test";
  private final String loadData = "/home/kiran/Desktop/test_lookup";
  private final String mapQuery = "SELECT P_ID FROM TEST.TEST_TABLE" +
      " WHERE FIRST_NAME rlike '${record:value(\"/first_name\")}'" +
      "   AND LAST_NAME rlike '${record:value(\"/last_name\")}'";
  private final String listQuery = "SELECT P_ID FROM TEST.TEST_TABLE" +
      " WHERE FIRST_NAME rlike '${record:value(\"[0]\")}'" +
      "   AND LAST_NAME rlike '${record:value(\"[1]\")}'";
  private final String queryReturnsNoRow = "SELECT P_ID FROM TEST.TEST_TABLE" +
      " WHERE false";
  private final String hiveConnectionString = "jdbc:hive2://localhost:10000/" + database;

  private Connection connection = null;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws SQLException {
    // Create a table in Hive and put some data in it for querying.
    connection = DriverManager.getConnection(hiveConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      // Setup table
      statement.executeQuery("CREATE SCHEMA IF NOT EXISTS TEST");
      statement.executeQuery("CREATE TABLE IF NOT EXISTS TEST.TEST_TABLE " + "(P_ID INT, FIRST_NAME " +
          "VARCHAR(255), LAST_NAME VARCHAR(255)) " + "row format delimited fields terminated by '\\t'");

      statement.executeQuery("LOAD DATA LOCAL INPATH "+ "\"" + loadData +  "\"" + " INTO TABLE TEST.TEST_TABLE");

    }
  }

  @After
  public void tearDown() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      // Setup table
      statement.executeQuery("DROP TABLE IF EXISTS TEST.TEST_TABLE");
    }

    // Last open connection terminates HiveJDBC
    connection.close();
  }

  private BoneCPPoolConfigBean createConfigBean(String connectionString, String username, String password) {
    BoneCPPoolConfigBean bean = new BoneCPPoolConfigBean();
    bean.connectionString = connectionString;
    bean.useCredentials = true;
    bean.username =  username;
    bean.password =  password;

    return bean;
  }

  @Test
  public void testEmptyBatch() throws Exception {
    List<JdbcFieldColumnMapping> columnMappings = ImmutableList.of(new JdbcFieldColumnMapping("P_ID", "[3]"));

    JdbcLookupDProcessor processor = new JdbcLookupDProcessor();
    processor.bonecpConfigBean = createConfigBean(hiveConnectionString, username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcLookupDProcessor.class, processor)
        .addConfiguration("query", listQuery)
        .addConfiguration("columnMappings", columnMappings)
        .addConfiguration("multipleValuesBehavior", MultipleValuesBehavior.FIRST_ONLY)
        .addConfiguration("maxClobSize", 1000)
        .addConfiguration("maxBlobSize", 1000)
        .addOutputLane("lane")
        .build();

    List<Record> emptyBatch = ImmutableList.of();
    processorRunner.runInit();
    try {
      StageRunner.Output output = processorRunner.runProcess(emptyBatch);
      Assert.assertEquals(0, output.getRecords().get("lane").size());
    } finally {
      processorRunner.runDestroy();
    }
  }

  @Test
  public void testSingleRecordList() throws Exception {
    List<JdbcFieldColumnMapping> columnMappings = ImmutableList.of(new JdbcFieldColumnMapping("P_ID", "[2]"));

    JdbcLookupDProcessor processor = new JdbcLookupDProcessor();
    processor.bonecpConfigBean = createConfigBean(hiveConnectionString, username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcLookupDProcessor.class, processor)
        .addConfiguration("query", listQuery)
        .addConfiguration("columnMappings", columnMappings)
        .addConfiguration("multipleValuesBehavior", MultipleValuesBehavior.FIRST_ONLY)
        .addConfiguration("maxClobSize", 1000)
        .addConfiguration("maxBlobSize", 1000)
        .addOutputLane("lane")
        .build();

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    fields.add(Field.create("Harsha"));
    fields.add(Field.create("Vardhan"));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    processorRunner.runInit();
    try {
      StageRunner.Output output = processorRunner.runProcess(singleRecord);
      Assert.assertEquals(1, output.getRecords().get("lane").size());

      record = output.getRecords().get("lane").get(0);

      Assert.assertNotEquals(null, record.get("[2]"));
      Assert.assertEquals(1, record.get("[2]").getValueAsInteger());
    } finally {
      processorRunner.runDestroy();
    }
  }

  @Test
  public void testSingleRecordMap() throws Exception {
    List<JdbcFieldColumnMapping> columnMappings = ImmutableList.of(new JdbcFieldColumnMapping("P_ID", "/p_id"));

    JdbcLookupDProcessor processor = new JdbcLookupDProcessor();
    processor.bonecpConfigBean = createConfigBean(hiveConnectionString, username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcLookupDProcessor.class, processor)
        .addConfiguration("query", mapQuery)
        .addConfiguration("columnMappings", columnMappings)
        .addConfiguration("multipleValuesBehavior", MultipleValuesBehavior.FIRST_ONLY)
        .addConfiguration("maxClobSize", 1000)
        .addConfiguration("maxBlobSize", 1000)
        .addOutputLane("lane")
        .build();

    Record record = RecordCreator.create();
    LinkedHashMap<String, Field> fields = new LinkedHashMap<>();
    fields.put("first_name", Field.create("Harsha"));
    fields.put("last_name", Field.create("Vardhan"));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    processorRunner.runInit();
    try {
      StageRunner.Output output = processorRunner.runProcess(singleRecord);
      Assert.assertEquals(1, output.getRecords().get("lane").size());

      record = output.getRecords().get("lane").get(0);

      Assert.assertNotEquals(null, record.get("/p_id"));
      Assert.assertEquals(1, record.get("/p_id").getValueAsInteger());
    } finally {
      processorRunner.runDestroy();
    }
  }

  @Test
  public void testMultiRecord() throws Exception {
    List<JdbcFieldColumnMapping> columnMappings = ImmutableList.of(new JdbcFieldColumnMapping("P_ID", "[2]"));

    JdbcLookupDProcessor processor = new JdbcLookupDProcessor();
    processor.bonecpConfigBean = createConfigBean(hiveConnectionString, username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcLookupDProcessor.class, processor)
        .addConfiguration("query", listQuery)
        .addConfiguration("columnMappings", columnMappings)
        .addConfiguration("multipleValuesBehavior", MultipleValuesBehavior.FIRST_ONLY)
        .addConfiguration("maxClobSize", 1000)
        .addConfiguration("maxBlobSize", 1000)
        .addOutputLane("lane")
        .build();

    Record record1 = RecordCreator.create();
    List<Field> fields1 = new ArrayList<>();
    fields1.add(Field.create("Harsha"));
    fields1.add(Field.create("Vardhan"));
    record1.set(Field.create(fields1));

    Record record2 = RecordCreator.create();
    List<Field> fields2 = new ArrayList<>();
    fields2.add(Field.create("Sai"));
    fields2.add(Field.create("Jaswanth"));
    record2.set(Field.create(fields2));

    Record record3 = RecordCreator.create();
    List<Field> fields3 = new ArrayList<>();
    fields3.add(Field.create("Sai"));
    fields3.add(Field.create("Kumar"));
    record3.set(Field.create(fields3));

    List<Record> records = ImmutableList.of(record1, record2, record3);
    processorRunner.runInit();
    List<Record> outputRecords = processorRunner.runProcess(records).getRecords().get("lane");

    Assert.assertEquals(1, outputRecords.get(0).get("[2]").getValueAsInteger());
    Assert.assertEquals(2, outputRecords.get(1).get("[2]").getValueAsInteger());
    Assert.assertEquals(3, outputRecords.get(2).get("[2]").getValueAsInteger());
  }

  @Test
  public void testMultiRecordMissingRow() throws Exception {
    thrown.expect(OnRecordErrorException.class);

    List<JdbcFieldColumnMapping> columnMappings = ImmutableList.of(new JdbcFieldColumnMapping("P_ID", "[2]"));

    JdbcLookupDProcessor processor = new JdbcLookupDProcessor();
    processor.bonecpConfigBean = createConfigBean(hiveConnectionString, username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcLookupDProcessor.class, processor)
        .addConfiguration("query", listQuery)
        .addConfiguration("columnMappings", columnMappings)
        .addConfiguration("multipleValuesBehavior", MultipleValuesBehavior.FIRST_ONLY)
        .addConfiguration("maxClobSize", 1000)
        .addConfiguration("maxBlobSize", 1000)
        .addOutputLane("lane")
        .build();

    Record record1 = RecordCreator.create();
    List<Field> fields1 = new ArrayList<>();
    fields1.add(Field.create("Harsha"));
    fields1.add(Field.create("Vardhan"));
    record1.set(Field.create(fields1));

    Record record2 = RecordCreator.create();
    List<Field> fields2 = new ArrayList<>();
    fields2.add(Field.create("Sai"));
    fields2.add(Field.create("Jaswanth"));
    record2.set(Field.create(fields2));

    Record record3 = RecordCreator.create();
    List<Field> fields3 = new ArrayList<>();
    fields3.add(Field.create("Pat"));
    fields3.add(Field.create("Patterson"));
    record3.set(Field.create(fields3));

    List<Record> records = ImmutableList.of(record1, record2, record3);
    processorRunner.runInit();
    processorRunner.runProcess(records);
  }

  @Test
  public void testBadConnectionString() throws Exception {
    List<JdbcFieldColumnMapping> columnMappings = ImmutableList.of(new JdbcFieldColumnMapping("P_ID", "[2]"));

    JdbcLookupDProcessor processor = new JdbcLookupDProcessor();
    processor.bonecpConfigBean = createConfigBean("bad connection string", username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcLookupDProcessor.class, processor)
        .addConfiguration("query", listQuery)
        .addConfiguration("columnMappings", columnMappings)
        .addConfiguration("multipleValuesBehavior", MultipleValuesBehavior.FIRST_ONLY)
        .addConfiguration("maxClobSize", 1000)
        .addConfiguration("maxBlobSize", 1000)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = processorRunner.runValidateConfigs();
    assertEquals(0, issues.size());
  }

  @Test
  public void testBadCredentials() throws Exception {
    List<JdbcFieldColumnMapping> columnMappings = ImmutableList.of(new JdbcFieldColumnMapping("P_ID", "[2]"));

    JdbcLookupDProcessor processor = new JdbcLookupDProcessor();
    processor.bonecpConfigBean = createConfigBean(hiveConnectionString, "foo", "bar");

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcLookupDProcessor.class, processor)
        .addConfiguration("query", listQuery)
        .addConfiguration("columnMappings", columnMappings)
        .addConfiguration("multipleValuesBehavior", MultipleValuesBehavior.FIRST_ONLY)
        .addConfiguration("maxClobSize", 1000)
        .addConfiguration("maxBlobSize", 1000)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = processorRunner.runValidateConfigs();
    assertEquals(0, issues.size());
  }

  @Test
  public void testMissingColumnMappingList() throws Exception {
    List<JdbcFieldColumnMapping> columnMappings = ImmutableList.of();

    JdbcLookupDProcessor processor = new JdbcLookupDProcessor();
    processor.bonecpConfigBean = createConfigBean(hiveConnectionString, username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcLookupDProcessor.class, processor)
        .addConfiguration("query", listQuery)
        .addConfiguration("columnMappings", columnMappings)
        .addConfiguration("multipleValuesBehavior", MultipleValuesBehavior.FIRST_ONLY)
        .addConfiguration("maxClobSize", 1000)
        .addConfiguration("maxBlobSize", 1000)
        .addOutputLane("lane")
        .build();

    Record record1 = RecordCreator.create();
    List<Field> fields1 = new ArrayList<>();
    fields1.add(Field.create("Harsha"));
    fields1.add(Field.create("Vardhan"));
    record1.set(Field.create(fields1));

    Record record2 = RecordCreator.create();
    List<Field> fields2 = new ArrayList<>();
    fields2.add(Field.create("Sai"));
    fields2.add(Field.create("Jaswanth"));
    record2.set(Field.create(fields2));

    Record record3 = RecordCreator.create();
    List<Field> fields3 = new ArrayList<>();
    fields3.add(Field.create("Sai"));
    fields3.add(Field.create("Kumar"));
    record3.set(Field.create(fields3));

    List<Record> records = ImmutableList.of(record1, record2, record3);
    processorRunner.runInit();
    List<Record> outputRecords = processorRunner.runProcess(records).getRecords().get("lane");

    Assert.assertEquals(1, outputRecords.get(0).get("[2]").getValueAsMap().get("value").getValueAsInteger());
    Assert.assertEquals(2, outputRecords.get(1).get("[2]").getValueAsMap().get("value").getValueAsInteger());
    Assert.assertEquals(3, outputRecords.get(2).get("[2]").getValueAsMap().get("value").getValueAsInteger());
  }

  @Test
  public void testMissingColumnMappingMap() throws Exception {
    List<JdbcFieldColumnMapping> columnMappings = ImmutableList.of();

    JdbcLookupDProcessor processor = new JdbcLookupDProcessor();
    processor.bonecpConfigBean = createConfigBean(hiveConnectionString, username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcLookupDProcessor.class, processor)
        .addConfiguration("query", mapQuery)
        .addConfiguration("columnMappings", columnMappings)
        .addConfiguration("multipleValuesBehavior", MultipleValuesBehavior.FIRST_ONLY)
        .addConfiguration("maxClobSize", 1000)
        .addConfiguration("maxBlobSize", 1000)
        .addOutputLane("lane")
        .build();

    Record record1 = RecordCreator.create();
    LinkedHashMap<String, Field> fields = new LinkedHashMap<>();
    fields.put("first_name", Field.create("Harsha"));
    fields.put("last_name", Field.create("Vardhan"));
    record1.set(Field.create(fields));

    Record record2 = RecordCreator.create();
    LinkedHashMap<String, Field> fields2 = new LinkedHashMap<>();
    fields2.put("first_name", Field.create("Sai"));
    fields2.put("last_name", Field.create("Jaswanth"));
    record2.set(Field.create(fields2));

    Record record3 = RecordCreator.create();
    LinkedHashMap<String, Field> fields3 = new LinkedHashMap<>();
    fields3.put("first_name", Field.create("Sai"));
    fields3.put("last_name", Field.create("Kumar"));
    record3.set(Field.create(fields3));

    List<Record> records = ImmutableList.of(record1, record2, record3);
    processorRunner.runInit();
    List<Record> outputRecords = processorRunner.runProcess(records).getRecords().get("lane");

    Assert.assertEquals(1, outputRecords.get(0).get("/P_ID").getValueAsInteger());
    Assert.assertEquals(2, outputRecords.get(1).get("/P_ID").getValueAsInteger());
    Assert.assertEquals(3, outputRecords.get(2).get("/P_ID").getValueAsInteger());
  }

  @Test
  public void testValidationForDefaultValue() throws Exception {
    List<JdbcFieldColumnMapping> columnMappings = ImmutableList.of(
        new JdbcFieldColumnMapping("P_ID", "[2]", "100", DataType.USE_COLUMN_TYPE)
    );

    JdbcLookupDProcessor processor = new JdbcLookupDProcessor();
    processor.bonecpConfigBean = createConfigBean(hiveConnectionString, username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcLookupDProcessor.class, processor)
        .addConfiguration("query", listQuery)
        .addConfiguration("columnMappings", columnMappings)
        .addConfiguration("multipleValuesBehavior", MultipleValuesBehavior.FIRST_ONLY)
        .addConfiguration("maxClobSize", 1000)
        .addConfiguration("maxBlobSize", 1000)
        .addOutputLane("lane")
        .build();

    // Data type must be explicitly set if default value is not empty.
    List<Stage.ConfigIssue> issues = processorRunner.runValidateConfigs();
    assertEquals(1, issues.size());
  }

  @Test
  public void testWrongDataTypeDefaultValue() throws Exception {
    List<JdbcFieldColumnMapping> columnMappings = ImmutableList.of(
        new JdbcFieldColumnMapping("P_ID", "[2]", "HUNDRED", DataType.INTEGER)
    );

    JdbcLookupDProcessor processor = new JdbcLookupDProcessor();
    processor.bonecpConfigBean = createConfigBean(hiveConnectionString, username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcLookupDProcessor.class, processor)
        .addConfiguration("query", queryReturnsNoRow)
        .addConfiguration("columnMappings", columnMappings)
        .addConfiguration("multipleValuesBehavior", MultipleValuesBehavior.FIRST_ONLY)
        .addConfiguration("maxClobSize", 1000)
        .addConfiguration("maxBlobSize", 1000)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("lane")
        .build();

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    fields.add(Field.create("Harsha"));
    fields.add(Field.create("Vardhan"));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    processorRunner.runInit();
    try {
      StageRunner.Output output = processorRunner.runProcess(singleRecord);
      Assert.assertEquals(0, output.getRecords().get("lane").size());

      // Make sure record is sent to error.
      List<Record> errors = processorRunner.getErrorRecords();
      Assert.assertEquals(1, errors.size());
    } finally {
      processorRunner.runDestroy();
    }
  }

  @Test
  public void testDefaultValue() throws Exception {
    List<JdbcFieldColumnMapping> columnMappings = ImmutableList.of(
        new JdbcFieldColumnMapping("P_ID", "[2]", "100", DataType.INTEGER)
    );

    JdbcLookupDProcessor processor = new JdbcLookupDProcessor();
    processor.bonecpConfigBean = createConfigBean(hiveConnectionString, username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcLookupDProcessor.class, processor)
        .addConfiguration("query", queryReturnsNoRow)
        .addConfiguration("columnMappings", columnMappings)
        .addConfiguration("multipleValuesBehavior", MultipleValuesBehavior.FIRST_ONLY)
        .addConfiguration("maxClobSize", 1000)
        .addConfiguration("maxBlobSize", 1000)
        .addOutputLane("lane")
        .build();

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    fields.add(Field.create("Harsha"));
    fields.add(Field.create("Vardhan"));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    processorRunner.runInit();
    try {
      StageRunner.Output output = processorRunner.runProcess(singleRecord);
      Assert.assertEquals(1, output.getRecords().get("lane").size());

      record = output.getRecords().get("lane").get(0);

      Assert.assertNotEquals(null, record.get("[2]"));
      Assert.assertEquals(100, record.get("[2]").getValueAsInteger());
    } finally {
      processorRunner.runDestroy();
    }
  }

  @Test
  public void testValidationForDatetimeDefaultValue() throws Exception {
    List<JdbcFieldColumnMapping> columnMappings = ImmutableList.of(
        new JdbcFieldColumnMapping("P_ID", "[2]", "1-1-1999", DataType.DATE)
    );

    JdbcLookupDProcessor processor = new JdbcLookupDProcessor();
    processor.bonecpConfigBean = createConfigBean(hiveConnectionString, username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcLookupDProcessor.class, processor)
        .addConfiguration("query", listQuery)
        .addConfiguration("columnMappings", columnMappings)
        .addConfiguration("multipleValuesBehavior", MultipleValuesBehavior.FIRST_ONLY)
        .addConfiguration("maxClobSize", 1000)
        .addConfiguration("maxBlobSize", 1000)
        .addOutputLane("lane")
        .build();

    // '1-1-1999' doesn't follow the format 'yyyy/MM/dd'
    List<Stage.ConfigIssue> issues = processorRunner.runValidateConfigs();
    assertEquals(1, issues.size());
  }

  @Test
  public void testDatetimeDefaultValue() throws Exception {
    final String timeStr = "2017/02/10 15:44:33";
    List<JdbcFieldColumnMapping> columnMappings = ImmutableList.of(
        new JdbcFieldColumnMapping("P_ID", "[2]", timeStr, DataType.DATETIME)
    );

    JdbcLookupDProcessor processor = new JdbcLookupDProcessor();
    processor.bonecpConfigBean = createConfigBean(hiveConnectionString, username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcLookupDProcessor.class, processor)
        .addConfiguration("query", queryReturnsNoRow)
        .addConfiguration("columnMappings", columnMappings)
        .addConfiguration("multipleValuesBehavior", MultipleValuesBehavior.FIRST_ONLY)
        .addConfiguration("maxClobSize", 1000)
        .addConfiguration("maxBlobSize", 1000)
        .addOutputLane("lane")
        .build();

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    fields.add(Field.create("Harsha"));
    fields.add(Field.create("Vardhan"));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    processorRunner.runInit();
    try {
      StageRunner.Output output = processorRunner.runProcess(singleRecord);
      Assert.assertEquals(1, output.getRecords().get("lane").size());

      record = output.getRecords().get("lane").get(0);

      Assert.assertNotEquals(null, record.get("[2]"));
      Date date = record.get("[2]").getValueAsDatetime();
      Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      Assert.assertEquals(2017, calendar.get(Calendar.YEAR));
      Assert.assertEquals(1, calendar.get(Calendar.MONTH)); // 1 is Feb
      Assert.assertEquals(10, calendar.get(Calendar.DAY_OF_MONTH));
      Assert.assertEquals(15, calendar.get(Calendar.HOUR_OF_DAY));
      Assert.assertEquals(44, calendar.get(Calendar.MINUTE));
      Assert.assertEquals(33, calendar.get(Calendar.SECOND));
    } finally {
      processorRunner.runDestroy();
    }
  }

  @Test
  public void testMultipleValuesSplitIntoMultipleRecords() throws Exception {
    List<JdbcFieldColumnMapping> columnMappings = ImmutableList.of(new JdbcFieldColumnMapping("P_ID", "[2]"));

    JdbcLookupDProcessor processor = new JdbcLookupDProcessor();
    processor.bonecpConfigBean = createConfigBean(hiveConnectionString, username, password);

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(JdbcLookupDProcessor.class, processor)
        .addConfiguration("query", listQuery)
        .addConfiguration("columnMappings", columnMappings)
        .addConfiguration("multipleValuesBehavior", MultipleValuesBehavior.SPLIT_INTO_MULTIPLE_RECORDS)
        .addConfiguration("maxClobSize", 1000)
        .addConfiguration("maxBlobSize", 1000)
        .addOutputLane("lane")
        .build();

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    fields.add(Field.create("Kiran"));
    fields.add(Field.create("Krishna"));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    processorRunner.runInit();
    try {
      StageRunner.Output output = processorRunner.runProcess(singleRecord);
      Assert.assertEquals(2, output.getRecords().get("lane").size());
/*

      record = output.getRecords().get("lane").get(0);
      Assert.assertEquals(4, record.get("[2]").getValueAsInteger());

      record = output.getRecords().get("lane").get(1);
      Assert.assertEquals(5, record.get("[2]").getValueAsInteger());
*/

    } finally {
      processorRunner.runDestroy();
    }
  }
}
