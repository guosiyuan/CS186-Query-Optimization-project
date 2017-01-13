package edu.berkeley.cs186.database.datatypes;

/**
 * Abstract DataType for all database primitives Currently supported: integers, booleans, floats,
 * fixed-length strings.
 *
 * DataTypes are also comparable allowing comparisons or sorting.
 *
 * Provides default functionality for all DataType subclasses by assuming that the contained value is
 * not of the type specified.
 */
public abstract class DataType implements Comparable {

  /**
   * An enum with the current supported types.
   */
  public enum Types {BOOL, INT, FLOAT, STRING}

  public DataType() throws DataTypeException {
  }

  public DataType(boolean b) throws DataTypeException {
    throw new DataTypeException("not boolean type");
  }

  public DataType(int i) throws DataTypeException {
    throw new DataTypeException("not int type");
  }
  
  public DataType(float f) throws DataTypeException {
    throw new DataTypeException("not float type");
  }

  public DataType(String s, int len) throws DataTypeException {
    throw new DataTypeException("not String type");
  }

  public DataType(byte[] buf) throws DataTypeException {
    throw new DataTypeException("Not Implemented");
  }

  public boolean getBool() throws DataTypeException {
    throw new DataTypeException("not boolean type");
  }

  public int getInt() throws DataTypeException {
    throw new DataTypeException("not int type");
  }
  
  public float getFloat() throws DataTypeException {
    throw new DataTypeException("not float type");
  }

  public String getString() throws DataTypeException {
    throw new DataTypeException("not String type");
  }
  
  public void setBool(boolean b) throws DataTypeException {
    throw new DataTypeException("not boolean type");
  }

  public void setInt(int i) throws DataTypeException {
    throw new DataTypeException("not int type");
  }
  
  public void setFloat(float f) throws DataTypeException {
    throw new DataTypeException("not float type");
  }

  public void setString(String s, int len) throws DataTypeException {
    throw new DataTypeException("not string type");
  }

  /**
   * Returns the type of the DataType.
   *
   * @return the type from the Types enum
   * @throws DataTypeException
   */
  public Types type() throws DataTypeException {
    throw new DataTypeException("No type");
  }

  /**
   * Returns a byte array with the data contained by this DataType.
   *
   * @return a byte array
   * @throws DataTypeException
   */
  public byte[] getBytes() throws DataTypeException {
    throw new DataTypeException("Not Implemented");
  }

  /**
   * Returns the fixed size of this DataType.
   *
   * @return the size of the DataType
   * @throws DataTypeException
   */
  public int getSize() throws DataTypeException {
    throw new DataTypeException("Not Implemented");
  }
  
  public int compareTo(Object obj) throws DataTypeException {
    throw new DataTypeException("Not Implemented");
  }

  @Override
  public String toString() throws DataTypeException {
    throw new DataTypeException("Not Implemented");
  }
}
