package edu.berkeley.cs186.database.datatypes;

import edu.berkeley.cs186.database.StudentTest;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.Rule;
import org.junit.rules.Timeout;

public class TestStringDataType {
  @Rule
  public Timeout globalTimeout = Timeout.seconds(1); // 1 seconds max per method tested
 
 	@Test
  public void TestStringDataTypeConstructor() {
    DataType first = new StringDataType();
    assertEquals(first.getString(), "");
    
    DataType sec = new StringDataType("hello", 5);
    assertEquals(sec.getString(), "hello");

    DataType third = new StringDataType("hello", 3);
    assertEquals(third.getString(), "hel");
   
    DataType fourth = new StringDataType("hello", 10);
    assertEquals(fourth.getString(), "hello     ");
  }
  
	@Test  
  public void TestStringDataTypeSetters() {
    DataType first = new StringDataType();
    assertEquals("", first.getString());
    
		first.setString("test1234", 8);
    assertEquals("test1234", first.getString());
		
		first.setString("test1234", 6);
    assertEquals("test12", first.getString());
		
		first.setString("test1234", 10);
    assertEquals("test1234  ", first.getString());
	}
  
  @Test
  public void TestStringDataTypeType() {
    DataType first = new StringDataType("LOL", 3);
    assertEquals(DataType.Types.STRING, first.type());
  }
  
  @Test
  public void TestStringDataTypeEquals() {
    DataType first = new StringDataType("1234", 4);
    DataType second = new StringDataType("1234",4);
    assertEquals(first, second);
  }
 
  @Test
  public void TestStringDataTypeCompare() {
    DataType first = new StringDataType("ABCC", 4);
    DataType second = new StringDataType("ABCD", 4);
    assertTrue(first.compareTo(second) == -1);
    first.setString("ABCD", 4);
    assertTrue(first.compareTo(second) == 0);
    first.setString("ABCE", 4);
    assertTrue(first.compareTo(second) == 1);
  }

  @Test
  public void TestStringDataTypeSerialize() {
		String testString = "Test Serialize";
    DataType first = new StringDataType(testString, testString.length());
    byte[] b = first.getBytes();
    assertEquals(b.length, testString.length());
    assertEquals(b.length, first.getSize());
    DataType sec = new StringDataType(b);
    assertEquals(first, sec);
    assertEquals(testString, sec.getString());
  }

  @Test
  public void TestStringDataTypeSerialize2() {
		String testString = "Test Serialize";
    DataType first = new StringDataType(testString, testString.length() + 10);
    byte[] b = first.getBytes();
    assertEquals(b.length, testString.length() + 10);
    assertEquals(b.length, first.getSize());
    DataType sec = new StringDataType(b);
    assertEquals(first, sec);
  }

  @Test(expected = DataTypeException.class)  
  public void TestStringDataTypeString() {  
    DataType first = new StringDataType("hello", 3);
    first.getInt(); 
  }
  
  @Test(expected = DataTypeException.class)  
  public void TestStringDataTypeString2() {  
    DataType first = new StringDataType("test ", 5);
    first.setFloat(89.9f); 
  }
}
