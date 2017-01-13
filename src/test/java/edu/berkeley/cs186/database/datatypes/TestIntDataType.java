package edu.berkeley.cs186.database.datatypes;

import edu.berkeley.cs186.database.StudentTest;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.Rule;
import org.junit.rules.Timeout;

public class TestIntDataType {
  @Rule
  public Timeout globalTimeout = Timeout.seconds(1); // 1 seconds max per method tested
  
  @Test
  public void TestIntDataTypeConstructor() {
    DataType first = new IntDataType(99);
    assertEquals(first.getInt(), 99);
    
    DataType sec = new IntDataType(-99);
    assertEquals(sec.getInt(), -99);
  }
  
  @Test  
  public void TestIntDataTypeSetters() {
    DataType first = new IntDataType();
    first.setInt(13);
    assertEquals(first.getInt(), 13);
    first.setInt(-13); 
    assertEquals(first.getInt(), -13);
  }
  
  @Test
  public void TestIntDataTypeType() {
    DataType first = new IntDataType();
    assertEquals(first.type(), DataType.Types.INT);
  }
  
  @Test
  public void TestIntDataTypeEquals() {
    DataType first = new IntDataType(11);
    DataType second = new IntDataType(11);
    assertEquals(first, second);
  }

  @Test
  public void TestIntDataTypeCompare() {
    DataType first = new IntDataType(11);
    DataType second = new IntDataType(12);
    assertTrue(first.compareTo(second) == -1);
    first.setInt(12);
    assertTrue(first.compareTo(second) == 0);
    first.setInt(13);
    assertTrue(first.compareTo(second) == 1);
  }

  @Test
  public void TestIntDataTypeSerialize() {
    DataType first = new IntDataType(11);
    byte[] b = first.getBytes();
    assertEquals(b.length, 4);
    assertEquals(b.length, first.getSize());
    DataType sec = new IntDataType(b);
    assertEquals(first, sec);
  }

  @Test
  public void TestIntDataTypeSerialize2() {
    DataType first = new IntDataType(-11);
    byte[] b = first.getBytes();
    assertEquals(b.length, 4);
    assertEquals(b.length, first.getSize());
    DataType sec = new IntDataType(b);
    assertEquals(first, sec);
  }

  @Test(expected = DataTypeException.class)  
  public void TestIntDataTypeString() {  
    DataType first = new IntDataType(11);
    first.getString(); 
  }
  
  @Test(expected = DataTypeException.class)  
  public void TestIntDataTypeString2() {  
    DataType first = new IntDataType(11);
    String s = "LOL";
    first.setString(s,s.length()); 
  }

  @Test(expected = DataTypeException.class)  
  public void TestIntDataTypeFloat() {  
    DataType first = new IntDataType(11);
    first.getFloat(); 
  }

  @Test(expected = DataTypeException.class)  
  public void TestIntDataTypeFloat2() {  
    DataType first = new IntDataType(11);
    first.setFloat(1.1f); 
  }
}
