package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.StudentTest;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.datatypes.IntDataType;
import edu.berkeley.cs186.database.datatypes.StringDataType;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.junit.Rule;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TestSchema {
  
  @Rule
  public Timeout globalTimeout = Timeout.seconds(1); // 10 seconds max per method tested
  
  @Test
  public void testSchemaRetrieve() {
    Schema schema = TestUtils.createSchemaWithAllTypes();

    Record input = TestUtils.createRecordWithAllTypes();
    byte[] encoded = schema.encode(input);
    Record decoded = schema.decode(encoded);

    assertEquals(input, decoded);
  }

  @Test
  public void testValidRecord() {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    Record input = TestUtils.createRecordWithAllTypes();

    try {
      Record output = schema.verify(input.getValues());
      assertEquals(input, output);
    } catch (SchemaException se) {
      fail();
    }
  }

  @Test(expected = SchemaException.class)
  public void testInvalidRecordLength() throws SchemaException {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    schema.verify(new ArrayList<DataType>());
  }

  @Test(expected = SchemaException.class)
  public void testInvalidFields() throws SchemaException {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    List<DataType> values = new ArrayList<DataType>();

    values.add(new StringDataType("abcde", 5));
    values.add(new IntDataType(10));

    schema.verify(values);
  }

  @Test(expected = SchemaException.class)
  public void testInvalidStringLength() throws SchemaException {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    Record input = TestUtils.createRecordWithAllTypes();

    input.getValues().set(2, new StringDataType("abcdef", 6));
    schema.verify(input.getValues());
  }
}
