package edu.berkeley.cs186.database.datatypes;

import edu.berkeley.cs186.database.StudentTest;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.Rule;
import org.junit.rules.Timeout;

/**
* @author  Sammy Sidhu
* @version 1.0
*/

public class TestFloatDataType {
  @Rule
  public Timeout globalTimeout = Timeout.seconds(1); // 1 seconds max per method tested
  
  @Test
  public void TestFloatDataTypeConstructor() {
    DataType first = new FloatDataType(9.9f);
    assertEquals(first.getFloat(), 9.9f, 1e-9f);
    
    DataType sec = new FloatDataType(-9.9f);
    assertEquals(sec.getFloat(), -9.9f, 1e-9f);
  }
  
  @Test  
  public void TestFloatDataTypeSetters() {
    DataType first = new FloatDataType();
    first.setFloat(1.3f);
    assertEquals(first.getFloat(), 1.3f, 1e-9f);
    first.setFloat(-1.3f); 
    assertEquals(first.getFloat(), -1.3f, 1e-9f);
  }
  
  @Test
  public void TestFloatDataTypeType() {
    DataType first = new FloatDataType();
    assertEquals(first.type(), DataType.Types.FLOAT);
  }
  
  @Test
  public void TestFloatDataTypeEquals() {
    DataType first = new FloatDataType(1.1f);
    DataType second = new FloatDataType(1.1f);
    assertEquals(first, second);
  }

  @Test
  public void TestFloatDataTypeCompare() {
    DataType first = new FloatDataType(1.1f);
    DataType second = new FloatDataType(1.2f);
    assertTrue(first.compareTo(second) == -1);
    first.setFloat(1.2f);
    assertTrue(first.compareTo(second) == 0);
    first.setFloat(1.3f);
    assertTrue(first.compareTo(second) == 1);
  }
  
  @Test
  public void TestFloatDataTypeSerialize() {
    DataType first = new FloatDataType(11);
    byte[] b = first.getBytes();
    assertEquals(b.length, 4);
    assertEquals(b.length, first.getSize());
    DataType sec = new FloatDataType(b);
    assertEquals(first, sec);
  }

  @Test
  public void TestFloatDataTypeSerialize2() {
    DataType first = new FloatDataType(-11);
    byte[] b = first.getBytes();
    assertEquals(b.length, 4);
    assertEquals(b.length, first.getSize());
    DataType sec = new FloatDataType(b);
    assertEquals(first, sec);
  }

  @Test(expected = DataTypeException.class)  
  public void TestFloatDataTypeString() {  
    DataType first = new FloatDataType(1.1f);
    first.getString(); 
  }
  
  @Test(expected = DataTypeException.class)  
  public void TestFloatDataTypeString2() {  
    DataType first = new FloatDataType(1.1f);
    String s = "LOL";
    first.setString(s,s.length()); 
  }

  @Test(expected = DataTypeException.class)  
  public void TestFloatDataTypeInt() {  
    DataType first = new FloatDataType(1.1f);
    first.getInt(); 
  }

  @Test(expected = DataTypeException.class)  
  public void TestFloatDataTypeInt2() {  
    DataType first = new FloatDataType(1.1f);
    first.setInt(1); 
  }
}
