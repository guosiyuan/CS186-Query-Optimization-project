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

public class TestBoolDataType {
  @Rule
  public Timeout globalTimeout = Timeout.seconds(1); // 1 seconds max per method tested
  
  @Test
  public void TestBoolDataTypeConstructor() {
    DataType first = new BoolDataType(true);
    assertEquals(first.getBool(), true);
    
    DataType sec = new BoolDataType(false);
    assertEquals(sec.getBool(), false);
  }
  
  @Test  
  public void TestBoolDataTypeSetters() {
    DataType first = new BoolDataType();
    first.setBool(true);
    assertEquals(first.getBool(), true);
    first.setBool(false); 
    assertEquals(first.getBool(), false);
  }
  
  @Test
  public void TestBoolDataTypeType() {
    DataType first = new BoolDataType(true);
    assertEquals(first.type(), DataType.Types.BOOL);
  }
  
  @Test
  public void TestBoolDataTypeEquals() {
    DataType first = new BoolDataType(false);
    DataType second = new BoolDataType(false);
    assertEquals(first, second);
  }

  @Test
  public void TestBoolDataTypeCompare() {
    DataType first = new BoolDataType(false);
    DataType second = new BoolDataType(true);
    assertTrue(first.compareTo(second) == -1);
    first.setBool(true);
    assertTrue(first.compareTo(second) == 0);
  }

  @Test
  public void TestBoolDataTypeSerialize() {
    DataType first = new BoolDataType(true);
    byte[] b = first.getBytes();
    assertEquals(b.length, 1);
    assertEquals(b.length, first.getSize());
    DataType sec = new BoolDataType(b);
    assertEquals(first, sec);
  }

  @Test
  public void TestBoolDataTypeSerialize2() {
    DataType first = new BoolDataType(false);
    byte[] b = first.getBytes();
    assertEquals(b.length, 1);
    assertEquals(b.length, first.getSize());
    DataType sec = new BoolDataType(b);
    assertEquals(first, sec);
  }

  @Test(expected = DataTypeException.class)  
  public void TestBoolDataTypeString() {  
    DataType first = new BoolDataType(true);
    first.getString(); 
  }
  
  @Test(expected = DataTypeException.class)  
  public void TestBoolDataTypeString2() {  
    DataType first = new BoolDataType(true);
    String s = "LOL";
    first.setString(s,s.length()); 
  }

  @Test(expected = DataTypeException.class)  
  public void TestBoolDataTypeFloat() {  
    DataType first = new BoolDataType(false);
    first.getFloat(); 
  }

  @Test(expected = DataTypeException.class)  
  public void TestBoolDataTypeFloat2() {  
    DataType first = new BoolDataType(true);
    first.setFloat(1.1f); 
  }
}
