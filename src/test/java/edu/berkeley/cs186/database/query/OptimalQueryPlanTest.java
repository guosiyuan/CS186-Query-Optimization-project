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

public class OptimalQueryPlanTest {
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

  /**
   * Test sample, do not modify.
   */
  @Test
  @Category(StudentTestP2.class)
  public void testSample() {
    assertEquals(true, true); // Do not actually write a test like this!
  }

  @Test(timeout=1000)
  public void testSimpleSelectIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    List<String> columnNames = new ArrayList<String>();
    columnNames.add("int");
    columnNames.add("string");

    queryPlan.select(columnNames);
    Iterator<Record> outputIterator = queryPlan.executeOptimal();

    int count = 0;
    while (outputIterator.hasNext()) {
      Record record = outputIterator.next();
      assertTrue(record.getValues().get(0) instanceof IntDataType);
      assertTrue(record.getValues().get(1) instanceof StringDataType);

      count++;
    }

    assertEquals(this.defaultNumRecords, count);

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: SELECT\n" +
                  "columns: [int, string]\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: testAllTypes";
    assertEquals(tree, finalOperator.toString());

    transaction.end();
  }

  @Test(timeout=1000)
  public void testSimpleWhereIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    queryPlan.where("int", QueryPlan.PredicateOperator.GREATER_THAN_EQUALS, new IntDataType(0));

    Iterator<Record> outputIterator = queryPlan.executeOptimal();

    while (outputIterator.hasNext()) {
      Record record = outputIterator.next();

      assertTrue(record.getValues().get(1).getInt() >= 0);
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: WHERE\n" +
                  "column: testAllTypes.int\n" +
                  "predicate: GREATER_THAN_EQUALS\n" +
                  "value: 0\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: testAllTypes";
    assertEquals(tree, finalOperator.toString());

    transaction.end();
  }

  @Test(timeout=60000)
  public void testSimpleGroupByIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);
    MarkerRecord markerRecord = MarkerRecord.getMarker();

    queryPlan.groupBy("int");
    Iterator<Record> outputIterator = queryPlan.executeOptimal();

    boolean first = true;
    int prevValue = 0;
    while (outputIterator.hasNext()) {
      Record record = outputIterator.next();

      if (first) {
        prevValue = record.getValues().get(1).getInt();
        first = false;
      } else if (record == markerRecord) {
        first = true;
      } else {
        assertEquals(prevValue, record.getValues().get(1).getInt());
      }
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: GROUPBY\n" +
                  "column: testAllTypes.int\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: testAllTypes";
    assertEquals(tree, finalOperator.toString());

    transaction.end();
  }

  @Test(timeout=1000)
  public void testSelectWhereIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    queryPlan.where("int", QueryPlan.PredicateOperator.GREATER_THAN_EQUALS, new IntDataType(0));

    List<String> columnNames = new ArrayList<String>();
    columnNames.add("bool");
    columnNames.add("int");
    queryPlan.select(columnNames);

    Iterator<Record> recordIterator = queryPlan.executeOptimal();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataType> values = record.getValues();

      assertEquals(2, values.size());
      assertTrue(values.get(0) instanceof BoolDataType);
      assertTrue(values.get(1) instanceof IntDataType);

      assertTrue(values.get(1).getInt() >= 0);
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: SELECT\n" +
                  "columns: [bool, int]\n" +
                  "\ttype: WHERE\n" +
                  "\tcolumn: testAllTypes.int\n" +
                  "\tpredicate: GREATER_THAN_EQUALS\n" +
                  "\tvalue: 0\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: testAllTypes";
    assertEquals(tree, finalOperator.toString());

    transaction.end();
  }

  @Test(timeout=60000)
  public void testSelectGroupByIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    queryPlan.groupBy("int");
    List<String> columnNames = new ArrayList<String>();
    columnNames.add("int");
    queryPlan.select(columnNames);

    Iterator<Record> recordIterator = queryPlan.executeOptimal();

    boolean first = true;
    int prevValue = 0;
    MarkerRecord markerRecord = MarkerRecord.getMarker();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();

      if (first) {
        prevValue = record.getValues().get(0).getInt();
        assertEquals(1, record.getValues().size());
        first = false;
      } else if (record == markerRecord) {
        first = true;
      } else {
        assertEquals(prevValue, record.getValues().get(0).getInt());
        assertEquals(1, record.getValues().size());
      }
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: SELECT\n" +
                  "columns: [int]\n" +
                  "\ttype: GROUPBY\n" +
                  "\tcolumn: testAllTypes.int\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: testAllTypes";
    assertEquals(tree, finalOperator.toString());

    transaction.end();
  }

  @Test(timeout=60000)
  public void testWhereGroupByIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    queryPlan.groupBy("int");
    queryPlan.where("int", QueryPlan.PredicateOperator.GREATER_THAN, new IntDataType(10));

    Iterator<Record> recordIterator = queryPlan.executeOptimal();

    boolean first = true;
    int prevValue = 0;
    MarkerRecord markerRecord = MarkerRecord.getMarker();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();

      if (first) {
        prevValue = record.getValues().get(1).getInt();
        assertTrue(record.getValues().get(1).getInt() > 10);
        first = false;
      } else if (record == markerRecord) {
        first = true;
      } else {
        assertEquals(prevValue, record.getValues().get(1).getInt());
        assertTrue(record.getValues().get(1).getInt() > 10);
      }
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: GROUPBY\n" +
                  "column: testAllTypes.int\n" +
                  "\ttype: WHERE\n" +
                  "\tcolumn: testAllTypes.int\n" +
                  "\tpredicate: GREATER_THAN\n" +
                  "\tvalue: 10\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: testAllTypes";
    assertEquals(tree, finalOperator.toString());

    transaction.end();
  }

  @Test(timeout=60000)
  public void testSelectWhereGroupByIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    queryPlan.groupBy("int");
    queryPlan.where("int", QueryPlan.PredicateOperator.GREATER_THAN, new IntDataType(10));
    List<String> columnNames = new ArrayList<String>();
    columnNames.add("float");
    columnNames.add("int");
    queryPlan.select(columnNames);

    Iterator<Record> recordIterator = queryPlan.executeOptimal();

    boolean first = true;
    int prevValue = 0;
    MarkerRecord markerRecord = MarkerRecord.getMarker();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();

      if (first) {
        prevValue = record.getValues().get(1).getInt();
        assertTrue(record.getValues().get(1).getInt() > 10);
        assertEquals(2, record.getValues().size());
        first = false;
      } else if (record == markerRecord) {
        first = true;
      } else {
        assertEquals(prevValue, record.getValues().get(1).getInt());
        assertTrue(record.getValues().get(1).getInt() > 10);
        assertEquals(2, record.getValues().size());
      }
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: SELECT\n" +
                  "columns: [float, int]\n" +
                  "\ttype: GROUPBY\n" +
                  "\tcolumn: testAllTypes.int\n" +
                  "\t\ttype: WHERE\n" +
                  "\t\tcolumn: testAllTypes.int\n" +
                  "\t\tpredicate: GREATER_THAN\n" +
                  "\t\tvalue: 10\n" +
                  "\t\t\ttype: SEQSCAN\n" +
                  "\t\t\ttable: testAllTypes";
    assertEquals(tree, finalOperator.toString());

    transaction.end();
  }

  @Test(timeout=1000)
  public void testEmptyWhereResultIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    queryPlan.where("int", QueryPlan.PredicateOperator.GREATER_THAN_EQUALS,
        new IntDataType(Integer.MAX_VALUE));

    Iterator<Record> outputIterator = queryPlan.executeOptimal();

    int count = 0;
    while (outputIterator.hasNext()) {
      outputIterator.next();
      count++;
    }

    assertEquals(0, count);

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: WHERE\n" +
                  "column: testAllTypes.int\n" +
                  "predicate: GREATER_THAN_EQUALS\n" +
                  "value: 2147483647\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: testAllTypes";
    assertEquals(tree, finalOperator.toString());

    transaction.end();
  }

  @Test(timeout=5000)
  public void testIndexEqualityLookupIterator() throws DatabaseException, QueryPlanException {
    List<String> intTableNames = new ArrayList<String>() ;
    intTableNames.add("int");

    List<DataType> intTableTypes = new ArrayList<DataType>();
    intTableTypes.add(new IntDataType());

    this.database.createTableWithIndices(new Schema(intTableNames, intTableTypes), "tempIntTable", intTableNames);

    Database.Transaction transaction = this.database.beginTransaction();

    Record record = null;
    for (int i = 0; i < 5000; i++) {
      List<DataType> values = new ArrayList<DataType>();
      values.add(new IntDataType(i));

      transaction.addRecord("tempIntTable", values);

      if (i == 500) {
        record = new Record(values);
      }
    }

    QueryPlan queryPlan = transaction.query("tempIntTable");
    queryPlan.where("int",
                    QueryPlan.PredicateOperator.EQUALS,
                    new IntDataType(500));
    Iterator<Record> result = queryPlan.executeOptimal();

    assertEquals(record, result.next());

    assertFalse(result.hasNext());

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: INDEXSCAN\n" +
                  "table: tempIntTable\n" +
                  "column: int\n" +
                  "operator: EQUALS\n" +
                  "value: 500";
    assertEquals(tree, finalOperator.toString());
  }

  @Test(timeout=1000)
  public void testIndexRangeLookupIterator() throws DatabaseException, QueryPlanException {
    List<String> intTableNames = new ArrayList<String>() ;
    intTableNames.add("int");

    List<DataType> intTableTypes = new ArrayList<DataType>();
    intTableTypes.add(new IntDataType());

    this.database.createTableWithIndices(new Schema(intTableNames, intTableTypes), "tempIntTable", intTableNames);

    Database.Transaction transaction = this.database.beginTransaction();

    for (int i = 0; i < 1000; i++) {
      List<DataType> values = new ArrayList<DataType>();
      values.add(new IntDataType(i));

      transaction.addRecord("tempIntTable", values);
    }

    QueryPlan queryPlan = transaction.query("tempIntTable");
    queryPlan.where("int",
                    QueryPlan.PredicateOperator.GREATER_THAN_EQUALS,
                    new IntDataType(900));
    Iterator<Record> result = queryPlan.executeOptimal();

    int count = 0;
    while (result.hasNext()) {
      IntDataType nextInt = (IntDataType) result.next().getValues().get(0);
      assertEquals(count + 900, nextInt.getInt());

      count++;
    }

    assertEquals(100, count);

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    // Note that the optimzer does NOT choose the index scan operator.
    // Since indexes in our system are unclustered, the index scan
    // operator is actually more expensive for most range queries.
    String tree = "type: WHERE\n" +
                  "column: tempIntTable.int\n" +
                  "predicate: GREATER_THAN_EQUALS\n" +
                  "value: 900\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: tempIntTable";
    assertEquals(tree, finalOperator.toString());
  }

  @Test(timeout=60000)
  public void testSelectGroupByWithAggregatesIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    queryPlan.groupBy("int");
    queryPlan.count();
    queryPlan.average("int");
    queryPlan.sum("int");

    List<String> columnNames = new ArrayList<String>();
    columnNames.add("int");
    queryPlan.select(columnNames);

    Iterator<Record> recordIterator = queryPlan.executeOptimal();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataType> values = record.getValues();

      assertEquals(4, values.size());
      int value = values.get(0).getInt();
      int count = values.get(1).getInt();
      int sum = values.get(2).getInt();
      float average = values.get(3).getFloat();

      assertEquals(value * count, sum);

      // this is a high threshold to account for integer to float conversion inaccuracies
      assertEquals(sum / count, average, 10);
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: SELECT\n" +
                  "columns: [int, countAgg, sumAgg, averageAgg]\n" +
                  "\ttype: GROUPBY\n" +
                  "\tcolumn: testAllTypes.int\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: testAllTypes";
    assertEquals(finalOperator.toString(), tree);

    transaction.end();
  }

  @Test(timeout=500, expected = QueryPlanException.class)
  public void testSelectColumnNotInGroupByIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    queryPlan.groupBy("int");
    List<String> columns = new ArrayList<String>();
    columns.add("string");
    queryPlan.select(columns);

    queryPlan.executeOptimal();

    transaction.end();
  }
}
