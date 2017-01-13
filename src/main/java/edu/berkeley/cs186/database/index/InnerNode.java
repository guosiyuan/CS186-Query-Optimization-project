package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;

import java.util.List;

/**
 * A B+ tree inner node. An inner node header contains the page number of the
 * parent node (or -1 if no parent exists), and the page number of the first
 * child node (or -1 if no child exists). An inner node contains InnerEntry's.
 * Note that an inner node can have duplicate keys if a key spans multiple leaf
 * pages.
 *
 * Inherits all the properties of a BPlusNode.
 */
public class InnerNode extends BPlusNode {

  public InnerNode(BPlusTree tree) {
    super(tree, false);
    getPage().writeByte(0, (byte) 0);
    setFirstChild(-1);
    setParent(-1);
  }
  
  public InnerNode(BPlusTree tree, int pageNum) {
    super(tree, pageNum, false);
    if (getPage().readByte(0) != (byte) 0) {
      throw new BPlusTreeException("Page is not Inner Node!");
    }
  }

  @Override
  public boolean isLeaf() {
    return false;
  }

  public int getFirstChild() {
    return getPage().readInt(5);
  }
  
  public void setFirstChild(int val) {
    getPage().writeInt(5, val);
  }
  
  public int findChildFromKey(DataType key) {
    int keyPage = getFirstChild();
    List<BEntry> entries = getAllValidEntries();
    for (BEntry ent : entries) {
      if (key.compareTo(ent.getKey()) < 0) {
        break;
      }
      keyPage = ent.getPageNum();
    }
    return keyPage;
  }

  /**
   * See BPlusNode#locateLeaf documentation.
   */
  @Override
  public LeafNode locateLeaf(DataType key, boolean findFirst) {
    int childPage = findChildFromKey(key);
    BPlusNode child = getBPlusNode(getTree(), childPage);
    return child.locateLeaf(key, findFirst);
  }

  /**
   * Splits this node and pushes up the middle key. Note that we split this node
   * immediately after it becomes full rather than when trying to insert an
   * entry into a full node. Thus a full inner node of 2d entries will be split
   * into a left node with d entries and a right node with d-1 entries, with the
   * middle key pushed up.
   */
  @Override
  public void splitNode() {
    InnerNode right = new InnerNode(getTree());
    int leftPageNum = this.getPageNum();
    List<BEntry> allEntries = this.getAllValidEntries();
    int len = allEntries.size();

    List<BEntry> leftEntries = allEntries.subList(0, len/2);
    BEntry middle = allEntries.get(len/2);
    List<BEntry> rightEntries = allEntries.subList(len/2 + 1, len);
    
    this.overwriteBNodeEntries(leftEntries);
    
    for (BEntry ent : leftEntries) {
      int entPageNum = ent.getPageNum();
      getBPlusNode(getTree(), entPageNum).setParent(leftPageNum);
    }
    
    right.overwriteBNodeEntries(rightEntries);
    
    for (BEntry ent : rightEntries) {
      int entPageNum = ent.getPageNum();
      getBPlusNode(getTree(), entPageNum).setParent(right.getPageNum());
    }

    right.setFirstChild(middle.getPageNum());
    
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
}
