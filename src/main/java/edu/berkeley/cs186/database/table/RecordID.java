package edu.berkeley.cs186.database.table;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Represents the ID of a single record. Stores the id of a page and the slot number where this
 * record lives within that page.
 */
public class RecordID {
  private int pageNum;
  private short slotNumber;

  public RecordID(int pageNum, int slotNumber) {
    this.pageNum = pageNum;
    this.slotNumber = (short) slotNumber;
  }

  public RecordID(byte[] buff) {
    ByteBuffer bb = ByteBuffer.wrap(buff);
    this.pageNum = bb.getInt();
    this.slotNumber = bb.getShort();
  }

  public int getPageNum() {
    return this.pageNum;
  }

  public int getSlotNumber() {
    return (int) this.slotNumber;
  }

  @Override
  public String toString() {
    return "(PageNumber: " + this.pageNum + ", SlotNumber: " + this.slotNumber + ")";
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof RecordID)) {
      return false;
    }

    RecordID otherRecord = (RecordID) other;
    return otherRecord.getPageNum() == this.getPageNum() && 
          otherRecord.getSlotNumber() == this.getSlotNumber();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getPageNum(), getSlotNumber());
  }

  public byte[] getBytes() {
    return ByteBuffer.allocate(6).putInt(pageNum).putShort(slotNumber).array(); 
  }
  
  static public int getSize() {
    return 6;
  }
  
  public int compareTo(Object obj) {
    RecordID other = (RecordID) obj;
    int pageCompVal = Integer.compare(this.getPageNum(), other.getPageNum());

    if (pageCompVal == 0) {
      return Integer.compare(this.getSlotNumber(), other.getSlotNumber());
    }

    return pageCompVal;
  }

}
