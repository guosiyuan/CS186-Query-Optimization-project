package edu.berkeley.cs186.database.datatypes;

import java.lang.String;
import java.nio.charset.Charset;
import java.nio.ByteBuffer;

/**
 * Fixed-length String data type which serializes to UTF-8 bytes.
 */
public class StringDataType extends DataType {
  private String s;

  /**
   * Construct an empty StringDataType.
   */
  public StringDataType() {
    this.s = "";
  }

  /**
   * Construct a StringDataType with length len and value s.
   *
   * @param s the value of the StringDataType
   * @param len the length of the StringDataType
   */
  public StringDataType(String s, int len) {
		if (len < s.length()) {
    	this.s = s.substring(0, len);
    } else {
 			this.s = String.format("%-" + len + "s", s);
    }
  }

  /**
   * Construct a StringDataType from the bytes in buf.
   *
   * @param buf the byte buffer source
   */
  public StringDataType(byte[] buf) {
    this.s = new String(buf, Charset.forName("UTF-8"));
  }

  public StringDataType(int len) {
    this.s = "";

    for (int i = 0; i < len; i++) {
      this.s += " ";
    }
  }

  @Override
  public String getString() {
    return this.s;
  }

  @Override
  public void setString(String s, int len) {
		if (len < s.length()) {
    	this.s = s.substring(0, len);
    } else {
 			this.s = String.format("%-" + len + "s", s);
    }
  }

  @Override 
  public Types type() {
    return DataType.Types.STRING; 
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) 
      return true;
    if (this == null) 
      return false;
    if (this.getClass() != obj.getClass())
      return false;
    StringDataType other = (StringDataType) obj;
    return this.getString().equals(other.getString());
  }

  @Override
  public int hashCode() {
    return this.getString().hashCode();
  }

  @Override
  public int compareTo(Object obj) {
    if (this.getClass() != obj.getClass()) {
      throw new DataTypeException("Invalid Comparsion");
    }
    StringDataType other = (StringDataType) obj;
    return this.getString().compareTo(other.getString());
  }

  @Override
  public byte[] getBytes() {
    return this.s.getBytes(Charset.forName("UTF-8"));
  }

  @Override
  public int getSize() {
    return s.getBytes(Charset.forName("UTF-8")).length;
  }

  @Override
  public String toString() {
    return this.s;
  }
}
