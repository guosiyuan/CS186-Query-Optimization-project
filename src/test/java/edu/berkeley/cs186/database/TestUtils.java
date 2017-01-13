package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.datatypes.*;
import edu.berkeley.cs186.database.query.QueryPlanException;
import edu.berkeley.cs186.database.query.TestSourceOperator;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.ArrayList;
import java.util.List;

public class TestUtils {
 
 public static Schema createSchemaWithAllTypes() {
    List<DataType> dataTypes = new ArrayList<DataType>();
    List<String> fieldNames = new ArrayList<String>();

    dataTypes.add(new BoolDataType());
    dataTypes.add(new IntDataType());
    dataTypes.add(new StringDataType(5));
    dataTypes.add(new FloatDataType());

    fieldNames.add("bool");
    fieldNames.add("int");
    fieldNames.add("string");
    fieldNames.add("float");

    return new Schema(fieldNames, dataTypes);
  }
 
 public static Schema createSchemaWithTwoInts() {
    List<DataType> dataTypes = new ArrayList<DataType>();
    List<String> fieldNames = new ArrayList<String>();

    dataTypes.add(new IntDataType());
    dataTypes.add(new IntDataType());

    fieldNames.add("int1");
    fieldNames.add("int2");
    
    return new Schema(fieldNames, dataTypes);
  }

 public static Schema createSchemaOfBool() {
    List<DataType> dataTypes = new ArrayList<DataType>();
    List<String> fieldNames = new ArrayList<String>();
    
    dataTypes.add(new BoolDataType());

    fieldNames.add("bool");
    
    return new Schema(fieldNames, dataTypes);
  }
 
 public static Schema createSchemaOfString(int len) {
    List<DataType> dataTypes = new ArrayList<DataType>();
    List<String> fieldNames = new ArrayList<String>();
    
    dataTypes.add(new StringDataType(len));
    fieldNames.add("string");
    
    return new Schema(fieldNames, dataTypes);
  }


  public static Record createRecordWithAllTypes() {
    List<DataType> dataValues = new ArrayList<DataType>();
    dataValues.add(new BoolDataType(true));
    dataValues.add(new IntDataType(1));
    dataValues.add(new StringDataType("abcde", 5));
    dataValues.add(new FloatDataType((float) 1.2));

    return new Record(dataValues);
  }
 
  public static Record createRecordWithAllTypesWithValue(int val) {
    List<DataType> dataValues = new ArrayList<DataType>();
    dataValues.add(new BoolDataType(true));
    dataValues.add(new IntDataType(val));
    dataValues.add(new StringDataType(String.format("%05d", val), 5));
    dataValues.add(new FloatDataType((float) val));
    return new Record(dataValues);
  }


  public static TestSourceOperator createTestSourceOperatorWithInts(List<Integer> values)
      throws QueryPlanException {
    List<String> columnNames = new ArrayList<String>();
    columnNames.add("int");
    List<DataType> columnTypes = new ArrayList<DataType>();
    columnTypes.add(new IntDataType());
    Schema schema = new Schema(columnNames, columnTypes);

    List<Record> recordList = new ArrayList<Record>();

    for (int v : values) {
      List<DataType> recordValues = new ArrayList<DataType>();
      recordValues.add(new IntDataType(v));
      recordList.add(new Record(recordValues));
    }


    return new TestSourceOperator(recordList, schema);
  }

  public static TestSourceOperator createTestSourceOperatorWithFloats(List<Float> values)
      throws QueryPlanException {
    List<String> columnNames = new ArrayList<String>();
    columnNames.add("float");
    List<DataType> columnTypes = new ArrayList<DataType>();
    columnTypes.add(new FloatDataType());
    Schema schema = new Schema(columnNames, columnTypes);

    List<Record> recordList = new ArrayList<Record>();

    for (float v : values) {
      List<DataType> recordValues = new ArrayList<DataType>();
      recordValues.add(new FloatDataType(v));
      recordList.add(new Record(recordValues));
    }


    return new TestSourceOperator(recordList, schema);
  }
}
