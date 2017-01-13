package edu.berkeley.cs186.database.datatypes;
import java.lang.Integer;
import java.nio.ByteBuffer;

/**
 * Integer data type which serializes to 4 bytes
 */
public class IntDataType extends DataType {
  private int i;

  /**
   * Construct an empty IntDataType with value 0.
   */
  public IntDataType() {
    this.i = 0;
  }

  /**
   * Constructs an IntDataType with value i.
   *
   * @param i the value of the IntDataType
   */
  public IntDataType(int i) {
    this.i = i;
  }

  /**
   * Construct an IntDataType from the bytes in buf.
   *
   * @param buf the byte buffer source
   */
  public IntDataType(byte[] buf) {
    if (buf.length != this.getSize()) {
      throw new DataTypeException("Wrong size buffer for int");
    }
    this.i = ByteBuffer.wrap(buf).getInt();
  }

  @Override
  public int getInt() {
    return this.i;
  }

  @Override
  public void setInt(int i) {
    this.i = i;
  }

  @Override 
  public Types type() {
    return DataType.Types.INT; 
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) 
      return true;
    if (this == null) 
      return false;
    if (this.getClass() != obj.getClass())
      return false;
    IntDataType other = (IntDataType) obj;
    return this.getInt() == other.getInt();
  }

  @Override
  public int hashCode() {
    return Math.abs(this.getInt());
  }

  public int compareTo(Object obj) {
    if (this.getClass() != obj.getClass()) {
      throw new DataTypeException("Invalid Comparsion");
    }
    IntDataType other = (IntDataType) obj;
    return Integer.compare(this.getInt(), other.getInt());
  }

  @Override
  public byte[] getBytes() {
    return ByteBuffer.allocate(4).putInt(this.i).array();
  }

  @Override
  public int getSize() {
    return 4;
  }
  
  @Override
  public String toString() {
    return "" + this.i;
  }
}
