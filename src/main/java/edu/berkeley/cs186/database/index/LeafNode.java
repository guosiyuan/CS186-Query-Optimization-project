package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.RecordID;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

/**
 * A B+ tree leaf node. A leaf node header contains the page number of the
 * parent node (or -1 if no parent exists), the page number of the previous leaf
 * node (or -1 if no previous leaf exists), and the page number of the next leaf
 * node (or -1 if no next leaf exists). A leaf node contains LeafEntry's.
 *
 * Inherits all the properties of a BPlusNode.
 */
public class LeafNode extends BPlusNode {

  public LeafNode(BPlusTree tree) {
    super(tree, true);
    getPage().writeByte(0, (byte) 1);
    setPrevLeaf(-1);
    setParent(-1);
    setNextLeaf(-1);
  }
  
  public LeafNode(BPlusTree tree, int pageNum) {
    super(tree, pageNum, true);
    if (getPage().readByte(0) != (byte) 1) {
      throw new BPlusTreeException("Page is not Leaf Node!");
    }
  }

  @Override
  public boolean isLeaf() {
    return true;
  }

  /**
   * See BPlusNode#locateLeaf documentation.
   */
  @Override
  public LeafNode locateLeaf(DataType key, boolean findFirst) {
    if (!findFirst) {
      return this;
    }

    LeafNode lastLeaf = this;
    LeafNode currLeaf = this; 
    Iterator<RecordID> currIter = currLeaf.scanForKey(key);

    while(currIter.hasNext()) {
      lastLeaf = currLeaf;
      int prevLeafNum = currLeaf.getPrevLeaf();
      if (prevLeafNum <= 0) {
        break;
      }
      currLeaf = new LeafNode(getTree(), prevLeafNum);
      currIter = currLeaf.scanForKey(key);
    }
    return lastLeaf;
  }

  /**
   * Splits this node and copies up the middle key. Note that we split this node
   * immediately after it becomes full rather than when trying to insert an
   * entry into a full node. Thus a full leaf node of 2d entries will be split
   * into a left node with d entries and a right node with d entries, with the
   * leftmost key of the right node copied up.
   */
  @Override
  public void splitNode() {
    LeafNode right = new LeafNode(getTree());
    List<BEntry> allEntries = this.getAllValidEntries();
    int len = allEntries.size();
    this.overwriteBNodeEntries(allEntries.subList(0, len/2));
    BEntry middle = allEntries.get(len/2);
    right.overwriteBNodeEntries(allEntries.subList(len/2, len));

    int leftPageOldNextNum = this.getNextLeaf();
    int leftPageNum = this.getPageNum();
    this.setNextLeaf(right.getPageNum());

    right.setPrevLeaf(leftPageNum);
    right.setNextLeaf(leftPageOldNextNum);
    
    if (leftPageOldNextNum > 0) {
      LeafNode leftOldNext = new LeafNode(getTree(), leftPageOldNextNum);
      leftOldNext.setPrevLeaf(right.getPageNum());
    }
    
    int leftPageParentNum;
    InnerNode parNode;
    
    if (isRoot()) {
      parNode = new InnerNode(getTree());
      leftPageParentNum = parNode.getPageNum();
      parNode.setFirstChild(leftPageNum);
      this.setParent(leftPageParentNum);
      getTree().updateRoot(leftPageParentNum);
    } else {
      leftPageParentNum = this.getParent();
      parNode = new InnerNode(getTree(), leftPageParentNum);
    }
    
    right.setParent(leftPageParentNum);

    InnerEntry inEnt = new InnerEntry(middle.getKey(), right.getPageNum());
    parNode.insertBEntry(inEnt); 
  }
  
  public int getPrevLeaf() {
    return getPage().readInt(5);
  }

  public int getNextLeaf() {
    return getPage().readInt(9);
  }
  
  public void setPrevLeaf(int val) {
    getPage().writeInt(5, val);
  }

  public void setNextLeaf(int val) {
    getPage().writeInt(9, val);
  }

  /**
   * Creates an iterator of RecordID's for all entries in this node.
   *
   * @return an iterator of RecordID's
   */
  public Iterator<RecordID> scan() {
    List<BEntry> validEntries = getAllValidEntries();
    List<RecordID> rids = new ArrayList<RecordID>();

    for (BEntry le : validEntries) {
      rids.add(le.getRecordID());
    }

    return rids.iterator();
  }

  /**
   * Creates an iterator of RecordID's whose keys are greater than or equal to
   * the given start value key.
   *
   * @param startValue the start value key
   * @return an iterator of RecordID's
   */
  public Iterator<RecordID> scanFrom(DataType startValue) {
    List<BEntry> validEntries = getAllValidEntries();
    List<RecordID> rids = new ArrayList<RecordID>();

    for (BEntry le : validEntries) {
      if (startValue.compareTo(le.getKey()) < 1) { 
        rids.add(le.getRecordID());
      }
    }
    return rids.iterator();
  }

  /**
   * Creates an iterator of RecordID's that correspond to the given key.
   *
   * @param key the search key
   * @return an iterator of RecordID's
   */
  public Iterator<RecordID> scanForKey(DataType key) {
    List<BEntry> validEntries = getAllValidEntries();
    List<RecordID> rids = new ArrayList<RecordID>();

    for (BEntry le : validEntries) {
      if (key.compareTo(le.getKey()) == 0) { 
        rids.add(le.getRecordID());
      }
    }
    return rids.iterator();
  }
}
