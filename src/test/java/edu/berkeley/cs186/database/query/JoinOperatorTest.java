package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.io.Page;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.StudentTest;
import edu.berkeley.cs186.database.StudentTestP2;
import edu.berkeley.cs186.database.datatypes.BoolDataType;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.datatypes.FloatDataType;
import edu.berkeley.cs186.database.datatypes.IntDataType;
import edu.berkeley.cs186.database.datatypes.StringDataType;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import org.junit.rules.TemporaryFolder;


import javax.management.Query;

import static org.junit.Assert.*;

public class JoinOperatorTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test(timeout=5000)
  public void testOperatorSchema() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new SNLJOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    List<String> expectedSchemaNames = new ArrayList<String>();
    expectedSchemaNames.add("bool");
    expectedSchemaNames.add("int");
    expectedSchemaNames.add("string");
    expectedSchemaNames.add("float");
    expectedSchemaNames.add("bool");
    expectedSchemaNames.add("int");
    expectedSchemaNames.add("string");
    expectedSchemaNames.add("float");

    List<DataType> expectedSchemaTypes = new ArrayList<DataType>();
    expectedSchemaTypes.add(new BoolDataType());
    expectedSchemaTypes.add(new IntDataType());
    expectedSchemaTypes.add(new StringDataType(5));
    expectedSchemaTypes.add(new FloatDataType());
    expectedSchemaTypes.add(new BoolDataType());
    expectedSchemaTypes.add(new IntDataType());
    expectedSchemaTypes.add(new StringDataType(5));
    expectedSchemaTypes.add(new FloatDataType());

    Schema expectedSchema = new Schema(expectedSchemaNames, expectedSchemaTypes);

    assertEquals(expectedSchema, joinOperator.getOutputSchema());
  }

  @Test(timeout=5000)
  public void testSimpleJoin() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new SNLJOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    Iterator<Record> outputIterator = joinOperator.iterator();
    int numRecords = 0;

    List<DataType> expectedRecordValues = new ArrayList<DataType>();
    expectedRecordValues.add(new BoolDataType(true));
    expectedRecordValues.add(new IntDataType(1));
    expectedRecordValues.add(new StringDataType("abcde", 5));
    expectedRecordValues.add(new FloatDataType(1.2f));
    expectedRecordValues.add(new BoolDataType(true));
    expectedRecordValues.add(new IntDataType(1));
    expectedRecordValues.add(new StringDataType("abcde", 5));
    expectedRecordValues.add(new FloatDataType(1.2f));
    Record expectedRecord = new Record(expectedRecordValues);


    while (outputIterator.hasNext()) {
      assertEquals(expectedRecord, outputIterator.next());
      numRecords++;
    }

    assertEquals(100*100, numRecords);
  }

  @Test(timeout=5000)
  public void testEmptyJoin() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator leftSourceOperator = new TestSourceOperator();

    List<Integer> values = new ArrayList<Integer>();
    TestSourceOperator rightSourceOperator = TestUtils.createTestSourceOperatorWithInts(values);
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new SNLJOperator(leftSourceOperator, rightSourceOperator, "int", "int", transaction);
    Iterator<Record> outputIterator = joinOperator.iterator();

    assertFalse(outputIterator.hasNext());
  }

  @Test(timeout=5000, expected=QueryPlanException.class)
  public void testJoinOnInvalidColumn() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    new SNLJOperator(sourceOperator, sourceOperator, "notAColumn", "int", transaction);
  }

  @Test(timeout=5000, expected=QueryPlanException.class)
  public void testJoinOnNonMatchingColumn() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    new SNLJOperator(sourceOperator, sourceOperator, "string", "int", transaction);
  }

  @Test(timeout=5000)
  public void testSimpleJoinPNLJ() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new PNLJOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    Iterator<Record> outputIterator = joinOperator.iterator();
    int numRecords = 0;

    List<DataType> expectedRecordValues = new ArrayList<DataType>();
    expectedRecordValues.add(new BoolDataType(true));
    expectedRecordValues.add(new IntDataType(1));
    expectedRecordValues.add(new StringDataType("abcde", 5));
    expectedRecordValues.add(new FloatDataType(1.2f));
    expectedRecordValues.add(new BoolDataType(true));
    expectedRecordValues.add(new IntDataType(1));
    expectedRecordValues.add(new StringDataType("abcde", 5));
    expectedRecordValues.add(new FloatDataType(1.2f));
    Record expectedRecord = new Record(expectedRecordValues);


    while (outputIterator.hasNext()) {
      assertEquals(expectedRecord, outputIterator.next());
      numRecords++;
    }

    assertEquals(100*100, numRecords);
  }

  @Test(timeout=5000)
  public void testSimpleJoinBNLJ() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new BNLJOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    Iterator<Record> outputIterator = joinOperator.iterator();
    int numRecords = 0;

    List<DataType> expectedRecordValues = new ArrayList<DataType>();
    expectedRecordValues.add(new BoolDataType(true));
    expectedRecordValues.add(new IntDataType(1));
    expectedRecordValues.add(new StringDataType("abcde", 5));
    expectedRecordValues.add(new FloatDataType(1.2f));
    expectedRecordValues.add(new BoolDataType(true));
    expectedRecordValues.add(new IntDataType(1));
    expectedRecordValues.add(new StringDataType("abcde", 5));
    expectedRecordValues.add(new FloatDataType(1.2f));
    Record expectedRecord = new Record(expectedRecordValues);

    while (outputIterator.hasNext()) {
      assertEquals(expectedRecord, outputIterator.next());
      numRecords++;
    }

    assertEquals(100*100, numRecords);
  }

  @Test(timeout=5000)
  public void testSimpleJoinGHJ() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new GraceHashOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    Iterator<Record> outputIterator = joinOperator.iterator();
    int numRecords = 0;

    List<DataType> expectedRecordValues = new ArrayList<DataType>();
    expectedRecordValues.add(new BoolDataType(true));
    expectedRecordValues.add(new IntDataType(1));
    expectedRecordValues.add(new StringDataType("abcde", 5));
    expectedRecordValues.add(new FloatDataType(1.2f));
    expectedRecordValues.add(new BoolDataType(true));
    expectedRecordValues.add(new IntDataType(1));
    expectedRecordValues.add(new StringDataType("abcde", 5));
    expectedRecordValues.add(new FloatDataType(1.2f));
    Record expectedRecord = new Record(expectedRecordValues);

    while (outputIterator.hasNext()) {
      assertEquals(expectedRecord, outputIterator.next());
      numRecords++;
    }

    assertEquals(100*100, numRecords);
  }

  @Test(timeout=5000)
  public void testSimpleJoinPNLJMultiplePages() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator(1000);
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new PNLJOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    Iterator<Record> outputIterator = joinOperator.iterator();
    int numRecords = 0;

    List<DataType> expectedRecordValues = new ArrayList<DataType>();
    expectedRecordValues.add(new BoolDataType(true));
    expectedRecordValues.add(new IntDataType(1));
    expectedRecordValues.add(new StringDataType("abcde", 5));
    expectedRecordValues.add(new FloatDataType(1.2f));
    expectedRecordValues.add(new BoolDataType(true));
    expectedRecordValues.add(new IntDataType(1));
    expectedRecordValues.add(new StringDataType("abcde", 5));
    expectedRecordValues.add(new FloatDataType(1.2f));
    Record expectedRecord = new Record(expectedRecordValues);

    while (outputIterator.hasNext()) {
      assertEquals(expectedRecord, outputIterator.next());
      numRecords++;
    }

    assertEquals(1000*1000, numRecords);
  }

  @Test(timeout=10000)
  public void testSimpleJoinBNLJMultiplePages() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator(2000);
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new BNLJOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    Iterator<Record> outputIterator = joinOperator.iterator();
    int numRecords = 0;

    List<DataType> expectedRecordValues = new ArrayList<DataType>();
    expectedRecordValues.add(new BoolDataType(true));
    expectedRecordValues.add(new IntDataType(1));
    expectedRecordValues.add(new StringDataType("abcde", 5));
    expectedRecordValues.add(new FloatDataType(1.2f));
    expectedRecordValues.add(new BoolDataType(true));
    expectedRecordValues.add(new IntDataType(1));
    expectedRecordValues.add(new StringDataType("abcde", 5));
    expectedRecordValues.add(new FloatDataType(1.2f));
    Record expectedRecord = new Record(expectedRecordValues);

    while (outputIterator.hasNext()) {
      assertEquals(expectedRecord, outputIterator.next());
      numRecords++;
    }

    assertEquals(2000*2000, numRecords);
  }

  @Test(timeout=5000)
  public void testSimpleJoinGHJMultiplePages() throws QueryPlanException, DatabaseException, IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator(1000);
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new GraceHashOperator(sourceOperator, sourceOperator, "int", "int", transaction);

    Iterator<Record> outputIterator = joinOperator.iterator();
    int numRecords = 0;

    List<DataType> expectedRecordValues = new ArrayList<DataType>();
    expectedRecordValues.add(new BoolDataType(true));
    expectedRecordValues.add(new IntDataType(1));
    expectedRecordValues.add(new StringDataType("abcde", 5));
    expectedRecordValues.add(new FloatDataType(1.2f));
    expectedRecordValues.add(new BoolDataType(true));
    expectedRecordValues.add(new IntDataType(1));
    expectedRecordValues.add(new StringDataType("abcde", 5));
    expectedRecordValues.add(new FloatDataType(1.2f));
    Record expectedRecord = new Record(expectedRecordValues);

    while (outputIterator.hasNext()) {
      assertEquals(expectedRecord, outputIterator.next());
      numRecords++;
    }

    assertEquals(1000*1000, numRecords);
  }

  @Test(timeout=5000)
  public void testSimplePNLJOutputOrder() throws QueryPlanException, DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath());
    Database.Transaction transaction = d.beginTransaction();
    Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
    List<DataType> r1Vals = r1.getValues();
    Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
    List<DataType> r2Vals = r2.getValues();

    List<DataType> expectedRecordValues1 = new ArrayList<DataType>();
    List<DataType> expectedRecordValues2 = new ArrayList<DataType>();
    for (int i = 0; i < 2; i++) {
      for (DataType val: r1Vals) {
        expectedRecordValues1.add(val);
      }
      for (DataType val: r2Vals) {
        expectedRecordValues2.add(val);
      }
    }

    Record expectedRecord1 = new Record(expectedRecordValues1);
    Record expectedRecord2 = new Record(expectedRecordValues2);
    d.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
    d.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");

    for (int i = 0; i < 288; i++) {
      List<DataType> vals;
      if (i < 144) {
        vals = r1Vals;
      } else {
        vals = r2Vals;
      }
      transaction.addRecord("leftTable", vals);
      transaction.addRecord("rightTable", vals);
    }

    for (int i = 0; i < 288; i++) {
      if (i < 144) {
        transaction.addRecord("leftTable", r2Vals);
        transaction.addRecord("rightTable", r1Vals);
      } else {
        transaction.addRecord("leftTable", r1Vals);
        transaction.addRecord("rightTable", r2Vals);
      }
    }

    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
    QueryOperator joinOperator = new PNLJOperator(s1, s2, "int", "int", transaction);

    int count = 0;
    Iterator<Record> outputIterator = joinOperator.iterator();

    while (outputIterator.hasNext()) {
      if (count < 20736) {
        assertEquals(expectedRecord1, outputIterator.next());
      } else if (count < 20736*2) {
        assertEquals(expectedRecord2, outputIterator.next());
      } else if (count < 20736*3) {
        assertEquals(expectedRecord1, outputIterator.next());
      } else if (count < 20736*4) {
        assertEquals(expectedRecord2, outputIterator.next());
      } else if (count < 20736*5) {
        assertEquals(expectedRecord2, outputIterator.next());
      } else if (count < 20736*6) {
        assertEquals(expectedRecord1, outputIterator.next());
      } else if (count < 20736*7) {
        assertEquals(expectedRecord2, outputIterator.next());
      } else {
        assertEquals(expectedRecord1, outputIterator.next());
      }
      count++;
    }

    assertTrue(count == 165888);
  }

  @Test(timeout=5000)
  public void testBNLJOutputOrderUsingOneBuffer() throws QueryPlanException, DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath(), 3);
    Database.Transaction transaction = d.beginTransaction();
    Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
    List<DataType> r1Vals = r1.getValues();
    Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
    List<DataType> r2Vals = r2.getValues();
    List<DataType> expectedRecordValues1 = new ArrayList<DataType>();
    List<DataType> expectedRecordValues2 = new ArrayList<DataType>();

    for (int i = 0; i < 2; i++) {
      for (DataType val: r1Vals) {
        expectedRecordValues1.add(val);
      }
      for (DataType val: r2Vals) {
        expectedRecordValues2.add(val);
      }
    }

    Record expectedRecord1 = new Record(expectedRecordValues1);
    Record expectedRecord2 = new Record(expectedRecordValues2);
    d.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
    d.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");

    for (int i = 0; i < 288; i++) {
      List<DataType> vals;
      if (i < 144) {
        vals = r1Vals;
      } else {
        vals = r2Vals;
      }
      transaction.addRecord("leftTable", vals);
      transaction.addRecord("rightTable", vals);
    }

    for (int i = 0; i < 288; i++) {
      if (i < 144) {
        transaction.addRecord("leftTable", r2Vals);
        transaction.addRecord("rightTable", r1Vals);
      } else {
        transaction.addRecord("leftTable", r1Vals);
        transaction.addRecord("rightTable", r2Vals);
      }
    }

    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
    QueryOperator joinOperator = new BNLJOperator(s1, s2, "int", "int", transaction);
    int count = 0;
    Iterator<Record> outputIterator = joinOperator.iterator();
    while (outputIterator.hasNext()) {
      if (count < 20736) {
        assertEquals(expectedRecord1, outputIterator.next());
      } else if (count < 20736*2) {
        assertEquals(expectedRecord2, outputIterator.next());
      } else if (count < 20736*3) {
        assertEquals(expectedRecord1, outputIterator.next());
      } else if (count < 20736*4) {
        assertEquals(expectedRecord2, outputIterator.next());
      } else if (count < 20736*5) {
        assertEquals(expectedRecord2, outputIterator.next());
      } else if (count < 20736*6) {
        assertEquals(expectedRecord1, outputIterator.next());
      } else if (count < 20736*7) {
        assertEquals(expectedRecord2, outputIterator.next());
      } else {
        assertEquals(expectedRecord1, outputIterator.next());
      }
      count++;
    }
    assertTrue(count == 165888);
  }

  @Test(timeout=5000)
  public void testSimpleGHJOutputOrderUsingThreePartitions() throws QueryPlanException, DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath(), 4);
    Database.Transaction transaction = d.beginTransaction();
    Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
    List<DataType> r1Vals = r1.getValues();
    Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
    List<DataType> r2Vals = r2.getValues();
    Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
    List<DataType> r3Vals = r3.getValues();
    List<DataType> expectedRecordValues1 = new ArrayList<DataType>();
    List<DataType> expectedRecordValues2 = new ArrayList<DataType>();
    List<DataType> expectedRecordValues3 = new ArrayList<DataType>();

    for (int i = 0; i < 2; i++) {
      for (DataType val: r1Vals) {
        expectedRecordValues1.add(val);
      }
      for (DataType val: r2Vals) {
        expectedRecordValues2.add(val);
      }
      for (DataType val: r3Vals) {
        expectedRecordValues3.add(val);
      }
    }

    Record expectedRecord1 = new Record(expectedRecordValues1);
    Record expectedRecord2 = new Record(expectedRecordValues2);
    Record expectedRecord3 = new Record(expectedRecordValues3);
    d.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
    d.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");

    for (int i = 0; i < 999; i++) {
      if (i % 3 == 0) {
        transaction.addRecord("leftTable", r3Vals);
        transaction.addRecord("rightTable", r1Vals);
      } else if (i % 3 == 1) {
        transaction.addRecord("leftTable", r2Vals);
        transaction.addRecord("rightTable", r3Vals);
      } else {
        transaction.addRecord("leftTable", r1Vals);
        transaction.addRecord("rightTable", r2Vals);
      }
    }

    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
    QueryOperator joinOperator = new GraceHashOperator(s1, s2, "int", "int", transaction);
    int count = 0;
    Iterator<Record> outputIterator = joinOperator.iterator();

    while (outputIterator.hasNext()) {
      if (count < 333*333) {
        assertEquals(expectedRecord3, outputIterator.next());
      } else if (count < 333*333*2) {
        assertEquals(expectedRecord1, outputIterator.next());
      } else {
        assertEquals(expectedRecord2, outputIterator.next());
      }
      count++;
    }
    assertTrue(count == 333*333*3);
  }

  @Test(timeout=5000)
  public void testGHJOutputOrderUsingThreePartitions() throws QueryPlanException, DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath(), 4);
    Database.Transaction transaction = d.beginTransaction();
    Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
    List<DataType> r1Vals = r1.getValues();
    Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
    List<DataType> r2Vals = r2.getValues();
    Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
    List<DataType> r3Vals = r3.getValues();
    Record r4 = TestUtils.createRecordWithAllTypesWithValue(4);
    List<DataType> r4Vals = r1.getValues();
    Record r5 = TestUtils.createRecordWithAllTypesWithValue(5);
    List<DataType> r5Vals = r2.getValues();
    Record r6 = TestUtils.createRecordWithAllTypesWithValue(6);
    List<DataType> r6Vals = r3.getValues();
    List<DataType> expectedRecordValues1 = new ArrayList<DataType>();
    List<DataType> expectedRecordValues2 = new ArrayList<DataType>();
    List<DataType> expectedRecordValues3 = new ArrayList<DataType>();
    List<DataType> expectedRecordValues4 = new ArrayList<DataType>();
    List<DataType> expectedRecordValues5 = new ArrayList<DataType>();
    List<DataType> expectedRecordValues6 = new ArrayList<DataType>();

    for (int i = 0; i < 2; i++) {
      for (DataType val: r1Vals) {
        expectedRecordValues1.add(val);
      }
      for (DataType val: r2Vals) {
        expectedRecordValues2.add(val);
      }
      for (DataType val: r3Vals) {
        expectedRecordValues3.add(val);
      }
      for (DataType val: r4Vals) {
        expectedRecordValues4.add(val);
      }
      for (DataType val: r5Vals) {
        expectedRecordValues5.add(val);
      }
      for (DataType val: r6Vals) {
        expectedRecordValues6.add(val);
      }
    }

    Record expectedRecord1 = new Record(expectedRecordValues1);
    Record expectedRecord2 = new Record(expectedRecordValues2);
    Record expectedRecord3 = new Record(expectedRecordValues3);
    Record expectedRecord4 = new Record(expectedRecordValues4);
    Record expectedRecord5 = new Record(expectedRecordValues5);
    Record expectedRecord6 = new Record(expectedRecordValues6);
    d.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
    d.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");

    for (int i = 0; i < 999; i++) {
      if (i % 3 == 0) {
        transaction.addRecord("leftTable", r3Vals);
        transaction.addRecord("leftTable", r5Vals);
        transaction.addRecord("rightTable", r1Vals);
        transaction.addRecord("rightTable", r6Vals);
      } else if (i % 3 == 1) {
        transaction.addRecord("leftTable", r2Vals);
        transaction.addRecord("leftTable", r4Vals);
        transaction.addRecord("rightTable", r3Vals);
        transaction.addRecord("rightTable", r4Vals);
      } else {
        transaction.addRecord("leftTable", r1Vals);
        transaction.addRecord("leftTable", r6Vals);
        transaction.addRecord("rightTable", r2Vals);
        transaction.addRecord("rightTable", r5Vals);
      }
    }

    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
    QueryOperator joinOperator = new GraceHashOperator(s1, s2, "int", "int", transaction);
    int count = 0;
    Iterator<Record> outputIterator = joinOperator.iterator();
    while (outputIterator.hasNext()) {
      Record r = outputIterator.next();
      if (count < 666*666) {
        assertTrue(expectedRecord3.equals(r) || expectedRecord6.equals(r));
      } else if (count < 666*666*2) {
        assertTrue(expectedRecord1.equals(r) || expectedRecord4.equals(r));
      } else {
        assertTrue(expectedRecord2.equals(r) || expectedRecord5.equals(r));
      }
      count++;
    }
    assertTrue(count == 666*666*3);
  }


  @Test(timeout=5000)
  public void testBNLJDiffOutPutThanPNLJ() throws QueryPlanException, DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath(), 4);
    Database.Transaction transaction = d.beginTransaction();
    Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
    List<DataType> r1Vals = r1.getValues();
    Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
    List<DataType> r2Vals = r2.getValues();
    Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
    List<DataType> r3Vals = r3.getValues();
    Record r4 = TestUtils.createRecordWithAllTypesWithValue(4);
    List<DataType> r4Vals = r4.getValues();
    List<DataType> expectedRecordValues1 = new ArrayList<DataType>();
    List<DataType> expectedRecordValues2 = new ArrayList<DataType>();
    List<DataType> expectedRecordValues3 = new ArrayList<DataType>();
    List<DataType> expectedRecordValues4 = new ArrayList<DataType>();

    for (int i = 0; i < 2; i++) {
      for (DataType val: r1Vals) {
        expectedRecordValues1.add(val);
      }
      for (DataType val: r2Vals) {
        expectedRecordValues2.add(val);
      }
      for (DataType val: r3Vals) {
        expectedRecordValues3.add(val);
      }
      for (DataType val: r4Vals) {
        expectedRecordValues4.add(val);
      }
    }
    Record expectedRecord1 = new Record(expectedRecordValues1);
    Record expectedRecord2 = new Record(expectedRecordValues2);
    Record expectedRecord3 = new Record(expectedRecordValues3);
    Record expectedRecord4 = new Record(expectedRecordValues4);
    d.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
    d.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");
    for (int i = 0; i < 2*288; i++) {
      if (i < 144) {
        transaction.addRecord("leftTable", r1Vals);
        transaction.addRecord("rightTable", r3Vals);
      } else if (i < 288) {
        transaction.addRecord("leftTable", r2Vals);
        transaction.addRecord("rightTable", r4Vals);
      } else if (i < 432) {
        transaction.addRecord("leftTable", r3Vals);
        transaction.addRecord("rightTable", r1Vals);
      } else {
        transaction.addRecord("leftTable", r4Vals);
        transaction.addRecord("rightTable", r2Vals);
      }
    }
    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
    QueryOperator joinOperator = new BNLJOperator(s1, s2, "int", "int", transaction);
    Iterator<Record> outputIterator = joinOperator.iterator();
    int count = 0;
    while (outputIterator.hasNext()) {
      Record r = outputIterator.next();
      if (count < 144 * 144) {
        assertEquals(expectedRecord3, r);
      } else if (count < 2 * 144 * 144) {
        assertEquals(expectedRecord4, r);
      } else if (count < 3 * 144 * 144) {
        assertEquals(expectedRecord1, r);
      } else {
        assertEquals(expectedRecord2, r);
      }
      count++;
    }
    assertTrue(count == 82944);

  }

  @Test(timeout=5000)
  public void testPNLJDiffOutPutThanBNLJ() throws QueryPlanException, DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("joinTest");
    Database d = new Database(tempDir.getAbsolutePath(), 4);
    Database.Transaction transaction = d.beginTransaction();
    Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
    List<DataType> r1Vals = r1.getValues();
    Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
    List<DataType> r2Vals = r2.getValues();
    Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
    List<DataType> r3Vals = r3.getValues();
    Record r4 = TestUtils.createRecordWithAllTypesWithValue(4);
    List<DataType> r4Vals = r4.getValues();
    List<DataType> expectedRecordValues1 = new ArrayList<DataType>();
    List<DataType> expectedRecordValues2 = new ArrayList<DataType>();
    List<DataType> expectedRecordValues3 = new ArrayList<DataType>();
    List<DataType> expectedRecordValues4 = new ArrayList<DataType>();

    for (int i = 0; i < 2; i++) {
      for (DataType val: r1Vals) {
        expectedRecordValues1.add(val);
      }
      for (DataType val: r2Vals) {
        expectedRecordValues2.add(val);
      }
      for (DataType val: r3Vals) {
        expectedRecordValues3.add(val);
      }
      for (DataType val: r4Vals) {
        expectedRecordValues4.add(val);
      }
    }
    Record expectedRecord1 = new Record(expectedRecordValues1);
    Record expectedRecord2 = new Record(expectedRecordValues2);
    Record expectedRecord3 = new Record(expectedRecordValues3);
    Record expectedRecord4 = new Record(expectedRecordValues4);
    d.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
    d.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");
    for (int i = 0; i < 2*288; i++) {
      if (i < 144) {
        transaction.addRecord("leftTable", r1Vals);
        transaction.addRecord("rightTable", r3Vals);
      } else if (i < 288) {
        transaction.addRecord("leftTable", r2Vals);
        transaction.addRecord("rightTable", r4Vals);
      } else if (i < 432) {
        transaction.addRecord("leftTable", r3Vals);
        transaction.addRecord("rightTable", r1Vals);
      } else {
        transaction.addRecord("leftTable", r4Vals);
        transaction.addRecord("rightTable", r2Vals);
      }
    }
    QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
    QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
    QueryOperator joinOperator = new PNLJOperator(s1, s2, "int", "int", transaction);
    Iterator<Record> outputIterator = joinOperator.iterator();
    int count = 0;
    while (outputIterator.hasNext()) {
      Record r = outputIterator.next();
      if (count < 144 * 144) {
        assertEquals(expectedRecord1, r);
      } else if (count < 2 * 144 * 144) {
        assertEquals(expectedRecord2, r);
      } else if (count < 3 * 144 * 144) {
        assertEquals(expectedRecord3, r);
      } else {
        assertEquals(expectedRecord4, r);
      }
      count++;
    }
    assertTrue(count == 82944);

  }
}
