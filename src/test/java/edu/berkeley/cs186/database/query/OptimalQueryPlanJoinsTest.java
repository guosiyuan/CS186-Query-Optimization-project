package edu.berkeley.cs186.database.query;

import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.StudentTest;
import edu.berkeley.cs186.database.StudentTestP2;
import edu.berkeley.cs186.database.datatypes.BoolDataType;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.datatypes.FloatDataType;
import edu.berkeley.cs186.database.datatypes.IntDataType;
import edu.berkeley.cs186.database.datatypes.StringDataType;
import edu.berkeley.cs186.database.table.MarkerRecord;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.StringHistogram;

import static org.junit.Assert.*;

public class OptimalQueryPlanJoinsTest {
  private Database database;
  private Random random = new Random();
  private String alphabet = StringHistogram.alphaNumeric;
  private String defaulTableName = "testAllTypes";
  private int defaultNumRecords = 1000;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("db");
    this.database = new Database(tempDir.getAbsolutePath());
    this.database.deleteAllTables();
    this.database.createTable(TestUtils.createSchemaWithAllTypes(), this.defaulTableName);
    Database.Transaction transaction = this.database.beginTransaction();

    // by default, create 100 records
    for (int i = 0; i < this.defaultNumRecords; i++) {
      // generate a random record
      IntDataType intValue = new IntDataType(i);
      FloatDataType floatValue = new FloatDataType(this.random.nextFloat());
      BoolDataType boolValue = new BoolDataType(this.random.nextBoolean());
      String stringValue = "";

      for (int j = 0 ; j < 5; j++) {
        int randomIndex = Math.abs(this.random.nextInt() % alphabet.length());
        stringValue += alphabet.substring(randomIndex, randomIndex + 1);
      }

      List<DataType> values = new ArrayList<DataType>();
      values.add(boolValue);
      values.add(intValue);
      values.add(new StringDataType(stringValue, 5));
      values.add(floatValue);

      transaction.addRecord("testAllTypes", values);
    }

    transaction.end();
  }

  @Test(timeout=5000)
  public void testSimpleJoinIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.defaulTableName, "t2");
    QueryPlan queryPlan = transaction.query("t1");

    queryPlan.join("t2", "t1.int", "t2.int");
    Iterator<Record> outputIterator = queryPlan.executeOptimal();

    int count = 0;

    while (outputIterator.hasNext()) {
      count++;

      Record record = outputIterator.next();
      List<DataType> recordValues = record.getValues();
      assertEquals(recordValues.get(0), recordValues.get(4));
      assertEquals(recordValues.get(1), recordValues.get(5));
      assertEquals(recordValues.get(2), recordValues.get(6));
      assertEquals(recordValues.get(3), recordValues.get(7));
    }

    assertTrue(count == this.defaultNumRecords);

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: BNLJ\n" +
                  "leftColumn: t2.int\n" +
                  "rightColumn: t1.int\n" +
                  "\t(left)\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: t2\n" +
                  "\n" +
                  "\t(right)\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: t1";
    String tree2 = "type: BNLJ\n" +
                  "leftColumn: t1.int\n" +
                  "rightColumn: t2.int\n" +
                  "\t(left)\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: t1\n" +
                  "\n" +
                  "\t(right)\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: t2";
    assertTrue(finalOperator.toString().equals(tree) || finalOperator.toString().equals(tree2));

    transaction.end();
  }

  @Test(timeout=5000)
  public void testSelectJoinIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.defaulTableName, "t2");

    QueryPlan queryPlan = transaction.query("t1");

    queryPlan.join("t2", "t1.string", "t2.string");
    List<String> columnNames = new ArrayList<String>();
    columnNames.add("t1.int");
    columnNames.add("t2.string");
    queryPlan.select(columnNames);

    Iterator<Record> recordIterator = queryPlan.executeOptimal();

    int count = 0;
    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataType> values = record.getValues();

      assertEquals(2, values.size());

      assertTrue(values.get(0) instanceof IntDataType);
      assertTrue(values.get(1) instanceof StringDataType);

      count++;
    }

    // We test `>=` instead of `==` since strings are generated
    // randomly and there's a small chance of duplicates.
    assertTrue(count >= 1000);

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: SELECT\n" +
                  "columns: [t1.int, t2.string]\n" +
                  "\ttype: BNLJ\n" +
                  "\tleftColumn: t2.string\n" +
                  "\trightColumn: t1.string\n" +
                  "\t\t(left)\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: t2\n" +
                  "\t\n" +
                  "\t\t(right)\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: t1";
    String tree2 = "type: SELECT\n" +
                  "columns: [t1.int, t2.string]\n" +
                  "\ttype: BNLJ\n" +
                  "\tleftColumn: t1.string\n" +
                  "\trightColumn: t2.string\n" +
                  "\t\t(left)\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: t1\n" +
                  "\t\n" +
                  "\t\t(right)\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: t2";
    assertTrue(finalOperator.toString().equals(tree) || finalOperator.toString().equals(tree2));

    transaction.end();
  }

  @Test(timeout=5000)
  public void testWhereJoinIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.defaulTableName, "t2");

    QueryPlan queryPlan = transaction.query("t1");

    queryPlan.join("t2", "t1.string", "t2.string");
    queryPlan.where("t1.bool", QueryPlan.PredicateOperator.NOT_EQUALS, new BoolDataType(false));

    Iterator<Record> recordIterator = queryPlan.executeOptimal();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataType> values = record.getValues();

      assertEquals(values.get(0), values.get(4));
      assertEquals(values.get(1), values.get(5));
      assertEquals(values.get(2), values.get(6));
      assertEquals(values.get(3), values.get(7));

      assertTrue(values.get(0).getBool());
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: BNLJ\n" +
                  "leftColumn: t1.string\n" +
                  "rightColumn: t2.string\n" +
                  "\t(left)\n" +
                  "\ttype: WHERE\n" +
                  "\tcolumn: t1.bool\n" +
                  "\tpredicate: NOT_EQUALS\n" +
                  "\tvalue: false\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: t1\n" +
                  "\n" +
                  "\t(right)\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: t2";
    assertEquals(tree, finalOperator.toString());

    transaction.end();
  }

  @Test(timeout=5000)
  public void testSelectWhereJoinIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.defaulTableName, "t2");

    QueryPlan queryPlan = transaction.query("t1");

    queryPlan.join("t2", "t1.string", "t2.string");
    queryPlan.where("t1.bool", QueryPlan.PredicateOperator.NOT_EQUALS, new BoolDataType(false));

    List<String> columnNames = new ArrayList<String>();
    columnNames.add("t1.bool");
    columnNames.add("t2.int");
    queryPlan.select(columnNames);

    Iterator<Record> recordIterator = queryPlan.executeOptimal();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataType> values = record.getValues();

      assertEquals(2, values.size());

      assertTrue(values.get(0) instanceof BoolDataType);
      assertTrue(values.get(1) instanceof IntDataType);

      assertTrue(values.get(0).getBool());
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: SELECT\n" +
                  "columns: [t1.bool, t2.int]\n" +
                  "\ttype: BNLJ\n" +
                  "\tleftColumn: t1.string\n" +
                  "\trightColumn: t2.string\n" +
                  "\t\t(left)\n" +
                  "\t\ttype: WHERE\n" +
                  "\t\tcolumn: t1.bool\n" +
                  "\t\tpredicate: NOT_EQUALS\n" +
                  "\t\tvalue: false\n" +
                  "\t\t\ttype: SEQSCAN\n" +
                  "\t\t\ttable: t1\n" +
                  "\t\n" +
                  "\t\t(right)\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: t2";
    assertEquals(tree, finalOperator.toString());

    transaction.end();
  }

  @Test(timeout=60000)
  public void testSelectWhereGroupByJoinIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.defaulTableName, "t2");

    QueryPlan queryPlan = transaction.query("t1");

    queryPlan.join("t2", "t1.int", "t2.int");
    queryPlan.groupBy("t1.int");
    queryPlan.where("t2.int", QueryPlan.PredicateOperator.GREATER_THAN, new IntDataType(400));
    List<String> columnNames = new ArrayList<String>();
    columnNames.add("t1.float");
    columnNames.add("t2.int");
    queryPlan.select(columnNames);

    Iterator<Record> recordIterator = queryPlan.executeOptimal();

    boolean first = true;
    int prevValue = 0;
    MarkerRecord markerRecord = MarkerRecord.getMarker();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();

      if (first) {
        prevValue = record.getValues().get(1).getInt();
        assertTrue(record.getValues().get(1).getInt() > 400);
        assertEquals(2, record.getValues().size());
        first = false;
      } else if (record == markerRecord) {
        first = true;
      } else {
        assertEquals(prevValue, record.getValues().get(1).getInt());
        assertTrue(record.getValues().get(1).getInt() > 400);
        assertEquals(2, record.getValues().size());
      }
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: SELECT\n" +
                  "columns: [t1.float, t2.int]\n" +
                  "\ttype: GROUPBY\n" +
                  "\tcolumn: t1.int\n" +
                  "\t\ttype: BNLJ\n" +
                  "\t\tleftColumn: t2.int\n" +
                  "\t\trightColumn: t1.int\n" +
                  "\t\t\t(left)\n" +
                  "\t\t\ttype: WHERE\n" +
                  "\t\t\tcolumn: t2.int\n" +
                  "\t\t\tpredicate: GREATER_THAN\n" +
                  "\t\t\tvalue: 400\n" +
                  "\t\t\t\ttype: SEQSCAN\n" +
                  "\t\t\t\ttable: t2\n" +
                  "\t\t\n" +
                  "\t\t\t(right)\n" +
                  "\t\t\ttype: SEQSCAN\n" +
                  "\t\t\ttable: t1";
    assertEquals(tree, finalOperator.toString());

    transaction.end();
  }

  @Test(timeout=1000)
  public void testEmptyJoinResultIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    List<String> otherSchemaNames = new ArrayList<String>();
    otherSchemaNames.add("otherInt");
    List<DataType> otherSchemaTypes = new ArrayList<DataType>();
    otherSchemaTypes.add(new IntDataType());
    Schema otherSchema = new Schema(otherSchemaNames, otherSchemaTypes);

    transaction.createTempTable(otherSchema, "TestOtherTableForEmptyJoin");

    queryPlan.join("TestOtherTableForEmptyJoin",
                   this.defaulTableName + ".int",
                   "TestOtherTableForEmptyJoin.otherInt");

    Iterator<Record> outputIterator = queryPlan.executeOptimal();

    int count = 0;
    while (outputIterator.hasNext()) {
      outputIterator.next();
      count++;
    }

    assertEquals(0, count);

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: SNLJ\n" +
                  "leftColumn: TestOtherTableForEmptyJoin.otherInt\n" +
                  "rightColumn: testAllTypes.int\n" +
                  "\t(left)\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: TestOtherTableForEmptyJoin\n" +
                  "\n" +
                  "\t(right)\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: testAllTypes";
    assertEquals(tree, finalOperator.toString());

    transaction.end();
  }

  @Test(timeout=5000)
  public void testQueryAsWithJoinIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.defaulTableName, "t2");

    QueryPlan queryPlan = transaction.query("t1");
    queryPlan.join("t2", "t1.int", "t2.int");

    List<String> columnNames = new ArrayList<String>();
    columnNames.add("t1.int");
    columnNames.add("t2.int");
    queryPlan.select(columnNames);

    Iterator<Record> recordIterator = queryPlan.executeOptimal();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataType> values = record.getValues();

      assertEquals(2, values.size());
      assertEquals(values.get(0), values.get(1));
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: SELECT\n" +
                  "columns: [t1.int, t2.int]\n" +
                  "\ttype: BNLJ\n" +
                  "\tleftColumn: t2.int\n" +
                  "\trightColumn: t1.int\n" +
                  "\t\t(left)\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: t2\n" +
                  "\t\n" +
                  "\t\t(right)\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: t1";
    String tree2 = "type: SELECT\n" +
                   "columns: [t1.int, t2.int]\n" +
                   "\ttype: BNLJ\n" +
                   "\tleftColumn: t1.int\n" +
                   "\trightColumn: t2.int\n" +
                   "\t\t(left)\n" +
                   "\t\ttype: SEQSCAN\n" +
                   "\t\ttable: t1\n" +
                   "\t\n" +
                   "\t\t(right)\n" +
                   "\t\ttype: SEQSCAN\n" +
                   "\t\ttable: t2";
    assertTrue(finalOperator.toString().equals(tree) || finalOperator.toString().equals(tree2));

    transaction.end();
  }

  @Test(timeout=10000)
  public void testGraceHashJoinIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    Schema testSchema = TestUtils.createSchemaWithAllTypes();
    this.database.createTable(testSchema, "TempTableLeft");
    this.database.createTable(testSchema, "TempTableRight");
    int numEntries = transaction.getNumEntriesPerPage("TempTableLeft");

    // add 15 pages worth of records to left table
    for (int i = 0; i < 15 * numEntries; i++) {
      transaction.addRecord("TempTableLeft", TestUtils.createRecordWithAllTypes().getValues());
    }

    // add 24 pages worth of records to right table
    for (int i = 0; i < 24 * numEntries; i++) {
      transaction.addRecord("TempTableRight", TestUtils.createRecordWithAllTypes().getValues());
    }

    transaction.queryAs("TempTableLeft", "t1");
    transaction.queryAs("TempTableRight", "t2");

    QueryPlan queryPlan = transaction.query("t1");
    queryPlan.join("t2", "t1.int", "t2.int");

    Iterator<Record> outputIterator = queryPlan.executeOptimal();

    int count = 0;
    while (outputIterator.hasNext()) {
      outputIterator.next();
      count++;
    }

    assertEquals(15 * numEntries * 24 * numEntries, count);

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: GRACEHASH\n" +
                  "leftColumn: t1.int\n" +
                  "rightColumn: t2.int\n" +
                  "\t(left)\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: t1\n" +
                  "\n" +
                  "\t(right)\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: t2";
    String tree2 = "type: GRACEHASH\n" +
                   "leftColumn: t2.int\n" +
                   "rightColumn: t1.int\n" +
                   "\t(left)\n" +
                   "\ttype: SEQSCAN\n" +
                   "\ttable: t2\n" +
                   "\n" +
                   "\t(right)\n" +
                   "\ttype: SEQSCAN\n" +
                   "\ttable: t1";
    assertTrue(finalOperator.toString().equals(tree) || finalOperator.toString().equals(tree2));

    transaction.end();
  }

  @Test(timeout=40000)
  public void testBNLJoinIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    Schema testSchema = TestUtils.createSchemaWithAllTypes();
    this.database.createTable(testSchema, "TempTableLeft");
    this.database.createTable(testSchema, "TempTableRight");
    int numEntries = transaction.getNumEntriesPerPage("TempTableLeft");

    // add 9 pages worth of records to left table
    for (int i = 0; i < 9 * numEntries; i++) {
      transaction.addRecord("TempTableLeft", TestUtils.createRecordWithAllTypes().getValues());
    }

    // add 24 pages worth of records to right table
    for (int i = 0; i < 24 * numEntries; i++) {
      transaction.addRecord("TempTableRight", TestUtils.createRecordWithAllTypes().getValues());
    }

    transaction.queryAs("TempTableLeft", "t1");
    transaction.queryAs("TempTableRight", "t2");

    QueryPlan queryPlan = transaction.query("t1");
    queryPlan.join("t2", "t1.int", "t2.int");

    Iterator<Record> outputIterator = queryPlan.executeOptimal();

    int count = 0;
    while (outputIterator.hasNext()) {
      outputIterator.next();
      count++;
    }

    assertEquals(9 * numEntries * 24 * numEntries, count);

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: BNLJ\n" +
                  "leftColumn: t1.int\n" +
                  "rightColumn: t2.int\n" +
                  "\t(left)\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: t1\n" +
                  "\n" +
                  "\t(right)\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: t2";
    assertEquals(tree, finalOperator.toString());

    transaction.end();
  }

  @Test(timeout=10000)
  public void testGraceHashJoinSmallBufferIterator() throws DatabaseException, QueryPlanException, IOException {
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath(), 4);
    Database.Transaction transaction = d.beginTransaction();
    Schema testSchema = TestUtils.createSchemaWithAllTypes();
    d.createTable(testSchema, "TempTableLeft");
    d.createTable(testSchema, "TempTableRight");
    int numEntries = transaction.getNumEntriesPerPage("TempTableLeft");

    // add 10 pages worth of records to left table
    for (int i = 0; i < 10 * numEntries; i++) {
      transaction.addRecord("TempTableLeft", TestUtils.createRecordWithAllTypes().getValues());
    }

    // add 16 pages worth of records to right table
    for (int i = 0; i < 16 * numEntries; i++) {
      transaction.addRecord("TempTableRight", TestUtils.createRecordWithAllTypes().getValues());
    }

    transaction.queryAs("TempTableLeft", "t1");
    transaction.queryAs("TempTableRight", "t2");

    QueryPlan queryPlan = transaction.query("t1");
    queryPlan.join("t2", "t1.int", "t2.int");

    Iterator<Record> outputIterator = queryPlan.executeOptimal();

    int count = 0;
    while (outputIterator.hasNext()) {
      outputIterator.next();
      count++;
    }

    assertEquals(10 * numEntries * 16 * numEntries, count);

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: GRACEHASH\n" +
        "leftColumn: t1.int\n" +
        "rightColumn: t2.int\n" +
        "\t(left)\n" +
        "\ttype: SEQSCAN\n" +
        "\ttable: t1\n" +
        "\n" +
        "\t(right)\n" +
        "\ttype: SEQSCAN\n" +
        "\ttable: t2";
    String tree2 = "type: GRACEHASH\n" +
        "leftColumn: t2.int\n" +
        "rightColumn: t1.int\n" +
        "\t(left)\n" +
        "\ttype: SEQSCAN\n" +
        "\ttable: t2\n" +
        "\n" +
        "\t(right)\n" +
        "\ttype: SEQSCAN\n" +
        "\ttable: t1";
    assertTrue(finalOperator.toString().equals(tree) || finalOperator.toString().equals(tree2));

    transaction.end();
  }

  @Test(timeout=20000)
  public void testBNLJoinSmallBufferIterator() throws DatabaseException, QueryPlanException, IOException {
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath(), 4);
    Database.Transaction transaction = d.beginTransaction();
    Schema testSchema = TestUtils.createSchemaWithAllTypes();
    d.createTable(testSchema, "TempTableLeft");
    d.createTable(testSchema, "TempTableRight");
    int numEntries = transaction.getNumEntriesPerPage("TempTableLeft");

    // add 6 pages worth of records to left table
    for (int i = 0; i < 6 * numEntries; i++) {
      transaction.addRecord("TempTableLeft", TestUtils.createRecordWithAllTypes().getValues());
    }

    // add 16 pages worth of records to right table
    for (int i = 0; i < 16 * numEntries; i++) {
      transaction.addRecord("TempTableRight", TestUtils.createRecordWithAllTypes().getValues());
    }

    transaction.queryAs("TempTableLeft", "t1");
    transaction.queryAs("TempTableRight", "t2");

    QueryPlan queryPlan = transaction.query("t1");
    queryPlan.join("t2", "t1.int", "t2.int");

    Iterator<Record> outputIterator = queryPlan.executeOptimal();

    int count = 0;
    while (outputIterator.hasNext()) {
      outputIterator.next();
      count++;
    }

    assertEquals(6 * numEntries * 16 * numEntries, count);

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: BNLJ\n" +
        "leftColumn: t1.int\n" +
        "rightColumn: t2.int\n" +
        "\t(left)\n" +
        "\ttype: SEQSCAN\n" +
        "\ttable: t1\n" +
        "\n" +
        "\t(right)\n" +
        "\ttype: SEQSCAN\n" +
        "\ttable: t2";
    assertEquals(tree, finalOperator.toString());

    transaction.end();
  }
}
