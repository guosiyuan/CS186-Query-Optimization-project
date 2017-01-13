package edu.berkeley.cs186.database.datatypes;
import java.lang.Boolean;
import java.nio.ByteBuffer;

/**
 * Boolean data type which serializes to 1 byte.
 */
public class BoolDataType extends DataType {
  private boolean bool;

  /**
   * Construct an empty BoolDataType.
   */
  public BoolDataType() {
    this.bool = false;
  }

  /**
   * Construct a BoolDataType with value b.
   *
   * @param b the value of the BoolDataType
   */
  public BoolDataType(boolean b) {
    this.bool = b;
  }

  /**
   * Construct a BoolDataType from a byte buffer.
   *
   * @param buf the byte buffer source
   */
  public BoolDataType(byte[] buf) {
    if (buf.length != this.getSize()) {
      throw new DataTypeException("Wrong size buffer for boolean");
    }
    this.bool = (buf[0] != 0);
  }

  @Override
  public boolean getBool() {
    return this.bool;
  }

  @Override
  public void setBool(boolean b) {
    this.bool = b;
  }

  @Override
  public Types type() {
    return DataType.Types.BOOL;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (this == null)
      return false;
    if (this.getClass() != obj.getClass())
      return false;
    BoolDataType other = (BoolDataType) obj;
    return Boolean.compare(this.getBool(),other.getBool()) == 0;
  }

  @Override
  public int hashCode() {
    return this.getBool() ? 1 : 0;
  }

  public int compareTo(Object obj) {
    if (this.getClass() != obj.getClass()) {
      throw new DataTypeException("Invalid Comparsion");
    }
    BoolDataType other = (BoolDataType) obj;
    return Boolean.compare(this.getBool(), other.getBool());
  }

  @Override
  public byte[] getBytes() {
    byte val = this.bool? (byte) 1 : (byte) 0;
    return ByteBuffer.allocate(1).put(val).array();
  }

  @Override
  public int getSize() {
    return 1;
  }

  @Override
  public String toString() {
    if (this.bool) {
      return "true";
    } else {
      return "false";
    }
  }
}
