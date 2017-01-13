package edu.berkeley.cs186.database.datatypes;

/**
* Exception that is thrown for DataType errors such as type mismatches
*/
public class DataTypeException extends RuntimeException {

  public DataTypeException() {
    super();
  }

  public DataTypeException(String message) {
    super(message);
  }
}
