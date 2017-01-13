package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.table.RecordID;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

/**
 * A B+ tree node. A node is represented as a page with a page header, slot
 * bitmap, and entries. The type of page header and entry are determined by
 * the subclasses InnerNode and LeafNode.
 *
 * Properties:
 * `keySchema`: the DataType for this index's search key
 * `entrySize`: the physical size (in bytes) of a page entry of this node
 * `numEntries`: number of entries this node can hold
 * `bitMapSize`: physical size (in bytes) of a page header slot bitmap
 * `headerSize`: physical size (in bytes) of the rest of the page header
 * `tree`: the BPlusTree containing this node
 * `pageNum`: the page number corresponding to this node
 */
public abstract class BPlusNode {
  private DataType keySchema;
  
  private int entrySize;
  protected int numEntries;
  private int bitMapSize;
  private int headerSize;
  private BPlusTree tree;
  private int pageNum;
  
  /**
   * Abstract Constructor for BPlusNode for existing Nodes
   *
   * @param tree the BPlusTree this Node belongs to
   * @param pageNum the pageNum where to open this node on
   * @param isLeaf is this node a leaf
   */

  public BPlusNode(BPlusTree tree, int pageNum, boolean isLeaf) {
    this.keySchema = tree.keySchema;
    this.tree = tree;
    this.pageNum = pageNum;
    if (isLeaf) {
      this.headerSize = 13;
      this.entrySize = keySchema.getSize() + RecordID.getSize();
    } else {
      this.headerSize = 9;
      this.entrySize = keySchema.getSize() + 4;
    }

    this.bitMapSize = (8 * (Page.pageSize - 13) / (1 + 8 * this.entrySize)) / 8;
    this.numEntries = bitMapSize * 8;
  }
  
  /**
   * Abstract Constructor for BPlusNode for new Nodes.
   * Auto-allocates Page for this node
   * @param tree the BPlusTree this Node belongs to
   * @param isLeaf is this node a leaf
   */
  public BPlusNode(BPlusTree tree, boolean isLeaf) {
    this(tree, tree.allocator.allocPage(), isLeaf);
  }
  
  /**
   * Helper method to return an existing Node from a Page
   * @param tree the BPlusTree this Node belongs to
   * @param pageNum the page number on where this node exists on
   * @return BPlusNode object that exists on this Page
   */
  public static BPlusNode getBPlusNode(BPlusTree tree, int pageNum) {
    if (tree.allocator.fetchPage(pageNum).readByte(0) == (byte) 0) {
      return new InnerNode(tree, pageNum);  
    }
    return new LeafNode(tree, pageNum);  
  }

  /**
   * Helper method to fetch page this Node exists on
   * @return the Page that this BPlusNode exists on
   */
  public Page getPage() {
    return tree.allocator.fetchPage(this.pageNum);
  }

  public int getPageNum() {
    return pageNum;
  }
  
  public boolean hasSpace() {
    return findFreeSlot() > -1;
  }
  
  /**
   * Retrieves the BPlusTree that this BPlusNode belongs to
   * @return the BPlusTree that owns this node
   */
  public BPlusTree getTree() {
    return tree;
  }

  public void splitNode() {
    throw new BPlusTreeException("Not Implemented");
  }
 
  public boolean isLeaf() {
    throw new BPlusTreeException("Not Implemented");
  }

  public boolean isRoot() {
    return getParent() == -1;
  }

  public int getParent() {
    return getPage().readInt(1);
  }

  public void setParent(int val) {
    getPage().writeInt(1, val);
  }
  
  private byte[] getBitMap() {
    return getPage().readBytes(headerSize, bitMapSize);
  }

  private void setBitMap(byte[] bitMap) {
    getPage().writeBytes(headerSize, bitMapSize, bitMap);
  }

  public int getOffset(int slotNum) {
    return slotNum*entrySize + this.headerSize + this.bitMapSize;
  }

  /**
   * Writes a given entry into the given slot specified.
   *
   * @param slot the slot number to fill
   * @param ent the entry to write
   */
  private void writeEntry(int slot, BEntry ent) {
    int byteOffset = slot/8;
    int bitOffset = 7 - (slot % 8);
    byte mask = (byte) (1 << bitOffset);
    
    byte[] bitMap = getBitMap();
    bitMap[byteOffset] = (byte) (bitMap[byteOffset] | mask);
    setBitMap(bitMap); 
    int entryOffset = getOffset(slot);
    getPage().writeBytes(entryOffset, entrySize, ent.toBytes());
  }

  /**
   * Reads an entry from the given slot specified.
   *
   * @param slot the slot number to read from
   * @return the entry corresponding to the slot
   */
  private BEntry readEntry(int slot) {
    if (isLeaf()) {
      return new LeafEntry(this.keySchema, getPage().readBytes(getOffset(slot), entrySize));
    } else {
      return new InnerEntry(this.keySchema, getPage().readBytes(getOffset(slot), entrySize));
    }
  }
  
  /**
   * Returns the slot number of the first free slot of this node.
   *
   * @return the first free slot, otherwise -1 if none exists
   */
  private int findFreeSlot() {
    byte[] bitMap = this.getBitMap();

    for (int i = 0; i < this.numEntries; i++) {
      int byteOffset = i/8;
      int bitOffset = 7 - (i % 8);
      byte mask = (byte) (1 << bitOffset);
      byte value = (byte) (bitMap[byteOffset] & mask);
      if (value == 0) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Returns a list of valid, existing entries of this node.
   *
   * @return a list of entries that have the valid bit set
   */
  protected List<BEntry> getAllValidEntries() {
    byte[] bitMap = this.getBitMap();
    List<BEntry> entries = new ArrayList<BEntry>(); 
    for (int i = 0; i < this.numEntries; i++) {
      int byteOffset = i/8;
      int bitOffset = 7 - (i % 8);
      byte mask = (byte) (1 << bitOffset);
      
      byte value = (byte) (bitMap[byteOffset] & mask);
      
      if (value != 0) {
        entries.add(readEntry(i));
      }
    }
    return entries;
  }

  /**
   * Clears all the entries of this node, and writes all the given entries into
   * the node, starting from the first slot.
   *
   * @param entries the list of entries to write
   */
  protected void overwriteBNodeEntries(List<BEntry> entries) {
    byte[] zeros = new byte[bitMapSize];
    setBitMap(zeros);
    if (entries.size() > numEntries) {
      throw new BPlusTreeException("too many BEntry given to fit on page");
    }

    for (int i = 0; i < entries.size(); i++) {
      writeEntry(i, entries.get(i));
    }
  }

  /**
   * Inserts an entry into this node. Note that we split this node immediately
   * after it becomes full rather than when trying to insert an entry into a
   * full node.
   *
   * @param ent the entry to insert
   */
  public void insertBEntry(BEntry ent) {
    if (!hasSpace()) {
      throw new BPlusTreeException("Node should have split before; Currently is full");
    }
    List<BEntry> entries = getAllValidEntries();
    entries.add(ent);
    Collections.sort(entries);
    overwriteBNodeEntries(entries);
    if (!hasSpace()) {
      splitNode();
    }
   }

  /**
   * Recursively locate the child that leads to the leaf node. If a key spans
   * multiple pages and findFirst is true, this method returns the first leaf
   * that contains the key. Otherwise, it returns the last leaf that contains
   * the key.
   *
   * @param key the key to search for
   * @param findFirst if true, returns the first leaf with the key
   * @return the LeafNode found
   */
  public LeafNode locateLeaf(DataType key, boolean findFirst) {
    throw new BPlusTreeException("Not Implemented");
  }

  /**
   * Insert a key and record id into the node.
   *
   * @param key search key
   * @param rid RecordID to insert into the leaf
   */
  public void insertKey(DataType key, RecordID rid) {
    LeafNode leaf = locateLeaf(key, false);
    LeafEntry ent = new LeafEntry(key, rid);
    leaf.insertBEntry(ent);
  }

}
