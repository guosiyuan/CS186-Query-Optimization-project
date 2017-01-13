package edu.berkeley.cs186.database.datatypes;
import java.lang.Float;
import java.nio.ByteBuffer;

/**
 * Float data type which serializes to 14 bytes.
 */
public class FloatDataType extends DataType {
  private float f;

  /**
   * Construct an empty FloatDataType with value 0.
   */
  public FloatDataType() {
    this.f = 0.0f;
  }

  /**
   * Construct an empty FloatDataType with value f.
   *
   * @param f the value of the FloatDataType
   */
  public FloatDataType(float f) {
    this.f = f;
  }

  /**
   * Construct a FloatDataType from the bytes in buf
   *
   * @param buf the bytes to construct the FloatDataType from
   */
  public FloatDataType(byte[] buf) {
    if (buf.length != this.getSize()) {
      throw new DataTypeException("Wrong size buffer for float");
    }
    this.f = ByteBuffer.wrap(buf).getFloat();
  }

  @Override
  public float getFloat() {
    return this.f;
  }

  @Override
  public void setFloat(float f) {
    this.f = f;
  }

  @Override
  public Types type() {
    return DataType.Types.FLOAT;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (this == null)
      return false;
    if (this.getClass() != obj.getClass())
      return false;
    FloatDataType other = (FloatDataType) obj;
    return this.getFloat() == other.getFloat();
  }

  @Override
  public int hashCode() {
    return (int) this.getFloat();
  }

  public int compareTo(Object obj) {
    if (this.getClass() != obj.getClass()) {
      throw new DataTypeException("Invalid Comparsion");
    }
    FloatDataType other = (FloatDataType) obj;
    return Float.compare(this.getFloat(), other.getFloat());
  }

  @Override
  public byte[] getBytes() {
    return ByteBuffer.allocate(4).putFloat(this.f).array();
  }

  @Override
  public int getSize() {
    return 4;
  }

  @Override
  public String toString() {
    return "" + this.f;
  }
}

