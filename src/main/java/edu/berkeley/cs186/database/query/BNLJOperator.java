package edu.berkeley.cs186.database.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.stats.TableStats;
import edu.berkeley.cs186.database.table.RecordID;

public class BNLJOperator extends JoinOperator {

  private int numBuffers;

  public BNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

    this.numBuffers = transaction.getNumMemoryPages();
    this.stats = this.estimateStats();
    this.cost = this.estimateIOCost();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new BNLJIterator();
  }

  public int estimateIOCost() throws QueryPlanException {
    // TODO: implement me!
    return (int)(Math.ceil((double)this.getLeftSource().getStats().getNumPages()/(double)(this.numBuffers - 2)) * this.getRightSource().getStats().getNumPages()) + this.getLeftSource().getStats().getNumPages();
    
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class BNLJIterator implements Iterator<Record> {
    private String leftTableName;//use
    private String rightTableName;//use
    private Iterator<Page> leftIterator;//use
    private Iterator<Page> rightIterator;//use
    private Record leftRecord;
    private Record nextRecord;
    private Record rightRecord;
    private Page leftPage;//use
    private Page rightPage;//use
    private ArrayList<Page> leftBlock;//use
    private ArrayList<Page> rightBlock;//use
    private int leftPageNumInBlock;//use
    private int rightPageNumInBlock;//use
    private int blockMinusTwo;//use
    private byte[] leftPageHeader;//use, can be null
    private byte[] rightPageHeader;//use, can be null
    private int leftSlotId;//use, can be -1
    private int rightSlotId;//use, can be -1
    private int lnEPP;//how many slots/page for table left
    private int rnEPP;//see above





    public BNLJIterator() throws QueryPlanException, DatabaseException {
      if (BNLJOperator.this.getLeftSource().isSequentialScan()) {
        this.leftTableName = ((SequentialScanOperator)BNLJOperator.this.getLeftSource()).getTableName();
      } else {
        this.leftTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getLeftColumnName() + "Left";
        BNLJOperator.this.createTempTable(BNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName);
        Iterator<Record> leftIter = BNLJOperator.this.getLeftSource().iterator();
        while (leftIter.hasNext()) {
          BNLJOperator.this.addRecord(leftTableName, leftIter.next().getValues());
        }
      }
      if (BNLJOperator.this.getRightSource().isSequentialScan()) {
        this.rightTableName = ((SequentialScanOperator)BNLJOperator.this.getRightSource()).getTableName();
      } else {
        this.rightTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getRightColumnName() + "Right";
        BNLJOperator.this.createTempTable(BNLJOperator.this.getRightSource().getOutputSchema(), rightTableName);
        Iterator<Record> rightIter = BNLJOperator.this.getRightSource().iterator();
        while (rightIter.hasNext()) {
          BNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
        }
      }
      // TODO: implement me!
      this.blockMinusTwo = BNLJOperator.this.numBuffers-2;
      if (this.blockMinusTwo<1) {
        this.blockMinusTwo = 1;//for B ==1 cases..
      }
      this.leftIterator = BNLJOperator.this.getPageIterator(leftTableName);//page iterator
      this.rightIterator = BNLJOperator.this.getPageIterator(rightTableName);

      leftRecord = null;
      rightRecord = null;
      nextRecord = null;

      leftBlock = new ArrayList(blockMinusTwo);
      rightBlock = new ArrayList(blockMinusTwo);

      if (leftIterator.hasNext()) {
        leftIterator.next();//skip the header page
      }
      if (rightIterator.hasNext()) {
        rightIterator.next();//skip the header page
      }

      leftSlotId = -1;//left entry num, initialize to -1
      if (leftIterator.hasNext()) {
        this.leftPage = leftIterator.next();//the first page in the table, left page
        this.leftPageHeader = BNLJOperator.this.getPageHeader(leftTableName, leftPage);//left page header
        this.lnEPP = BNLJOperator.this.getNumEntriesPerPage(leftTableName);//max number of entries can be contained in page
      } else {//nothing in the table
        this.leftPage = null;
        this.leftPageHeader = null;
        this.lnEPP = 0;
      }
    
      rightSlotId = -1;//right entry num
      if (rightIterator.hasNext()) {
        this.rightPage = rightIterator.next();//the first page in the table, left page
        this.rightPageHeader = BNLJOperator.this.getPageHeader(rightTableName, rightPage);//left page header
        this.rnEPP = BNLJOperator.this.getNumEntriesPerPage(rightTableName);//max number of entries can be contained in page
      } else {
        this.rightPage = null;
        this.rightPageHeader = null;
        this.rnEPP = 0;
      }
      //add b-2 pages to block, left+right
      leftBlock.add(0, leftPage);//add the first page to block
      rightBlock.add(0, rightPage);
      leftPageNumInBlock = 0;
      rightPageNumInBlock = 0;

      for (int i = 1; i<blockMinusTwo; i++) {//then add later pages to block
        if (leftIterator.hasNext()) {
          leftBlock.add(i, leftIterator.next());
        }
        // if (rightIterator.hasNext()) {
        //   rightBlock.add(i, rightIterator.next());
        // }
      }
    }
//************************************helper functions below****************************************
    private int[] getNextLeftPageAndSlotNum(){//next available slot num on he current page, [-1,-1] if reach the end of page
      if (leftPage == null) {
        int[] a = {-1, -1};
        return a;//no pages in left table
      }
      int slotNum = -1;
      for (int i = leftSlotId+1; i<lnEPP; i++) {//find first rec on the right page
        byte b = leftPageHeader[i/8];//i serves as the current slot id we check
        int bitOffset = 7 - (i % 8);
        byte mask = (byte) (1 << bitOffset);
        byte value = (byte) (b & mask);//if value == 1 that means the entryNum is valid, there are record in the place
        if (value!= 0) {//find the first valid slot id
          slotNum = i;
          int[] a = {leftPageNumInBlock, slotNum};
          return a;//search a record in current page
        }
      }
      if (leftPageNumInBlock == blockMinusTwo) {
        int[] a = {-1,-1};
        return a;//last page in block and nothing available in this page
      }
      for (int j = leftPageNumInBlock+1; j<blockMinusTwo; j ++) {//scan all later pages in leftblock
        if (j>=leftBlock.size()) {//avoid index out of bounds
          int[] a = {-1, -1};
          return a;
        }
        Page pge = leftBlock.get(j);//page considering//may be error
        // if (pge == null) {
        //   int[] a = {-1, -1};
        //   return a;
        // }
        byte[] lpgeHeader;
        try {
          lpgeHeader = BNLJOperator.this.getPageHeader(leftTableName, pge);
        } catch (DatabaseException e){
          int[] a = {-1, -1};
          return a;
        }
        for (int i = 0; i<lnEPP; i++) {//find first rec on the right page
          byte b = lpgeHeader[i/8];//i serves as the current slot id we check
          int bitOffset = 7 - (i % 8);
          byte mask = (byte) (1 << bitOffset);
          byte value = (byte) (b & mask);//if value == 1 that means the entryNum is valid, there are record in the place
          if (value!= 0) {//find the first valid slot id
            slotNum = i;
            int pgeNum = j;
            int[] a = {pgeNum, slotNum};
            return a;
            //search a record in the page
          }
        }
      }
      int[] a = {-1, -1};
      return a;//eof, no next record on this page
    }
    //same as above, right ver

    private int getNextRightPageAndSlotNum(){//next available slot num on he current page, [-1,-1] if reach the end of page
      if (this.rightPage == null) return -1;//no element in subsequent tables
      for (int i = rightSlotId+1; i<rnEPP; i++) {//find first rec on the right page
        byte b = rightPageHeader[i/8];//i serves as the current slot id we check
        int bitOffset = 7 - (i % 8);
        byte mask = (byte) (1 << bitOffset);
        byte value = (byte) (b & mask);//if value == 1 that means the entryNum is valid, there are record in the place
        if (value!= 0) {//find the first valid slot id
          return i;
        }
      }
      return -1;//eof, no next record on this page
    }

    //larger helper func No.3, get next left record
    private Record getNextLeftRecord() {//locate next
      int[] nextLeftPageSlotNum = getNextLeftPageAndSlotNum();//e.g. [1, 12]
      if (nextLeftPageSlotNum[0] != -1) {//1. left has next on this block, but need to update info about pages, also need to refresh the right block
        RecordID rid = new RecordID(leftBlock.get(nextLeftPageSlotNum[0]).getPageNum(), nextLeftPageSlotNum[1]);//pop the record with next slotid
        leftSlotId = nextLeftPageSlotNum[1];//update leftslotnum with new slotid, change left pages
        leftPage = leftBlock.get(nextLeftPageSlotNum[0]);
        leftPageNumInBlock = nextLeftPageSlotNum[0];
        try {
          leftPageHeader = BNLJOperator.this.getPageHeader(leftTableName, leftPage);
        } catch (DatabaseException e){
          return null;
        }

        rightSlotId = -1;//refresh the right page 
        //rightPage = rightBlock.get(0);
        //rightPageNumInBlock = 0;
        // try {
        //   rightPageHeader = BNLJOperator.this.getPageHeader(rightTableName, rightPage);
        // } catch (DatabaseException e){
        //   return null;
        // }
        try {
          return BNLJOperator.this.getTransaction().getRecord(leftTableName, rid);//return the record*****error datab fixed
        } catch (DatabaseException e) {
          return null;
        }
      } else {//reach the end of this left page 
        if (rightIterator.hasNext()) {//2. 3. then do the initialization for the next right pages, then return the first of this left page,##right has next page
          //rightBlock = new ArrayList(blockMinusTwo);
          rightPage = rightIterator.next();
          // for (int i = 0; i<blockMinusTwo; i++) {//then add later pages to block
          //   if (rightIterator.hasNext()) {
          //     rightBlock.add(i, rightIterator.next());
          //   }
          // }
          this.rightSlotId = -1;
          //rightPage = rightBlock.get(0);//rb has at least 1 element
          //rightPageNumInBlock = 0;
          try {
            rightPageHeader = BNLJOperator.this.getPageHeader(rightTableName, rightPage);
          } catch (DatabaseException e){
            return null;
          }

          //then set the left count to -1
          leftSlotId = -1;//initialize to -1
          leftPage = leftBlock.get(0);
          leftPageNumInBlock = 0;
          try {
            leftPageHeader = BNLJOperator.this.getPageHeader(leftTableName, leftPage);
          } catch (DatabaseException e){
            return null;
          }
          //leftSlotId = getNextLeftSlotNum();//rebuild leftslotid to top most value
          return getNextLeftRecord();
        } else {//right reach the end of file, left proceed to the next page
          if (leftIterator.hasNext()) {//4. update to next left block, also initialize right pages to beginning condition
            leftBlock = new ArrayList(blockMinusTwo);
            for (int i = 0; i<blockMinusTwo; i++) {//then add later pages to block
              if (leftIterator.hasNext()) {
                leftBlock.add(i, leftIterator.next());
              }
            }
            leftSlotId = -1;
            leftPage = leftBlock.get(0);
            leftPageNumInBlock = 0;
            try {
              leftPageHeader = BNLJOperator.this.getPageHeader(leftTableName, leftPage);//*****error data fixed
              rightIterator = BNLJOperator.this.getPageIterator(rightTableName);//new right iterator//*******error data fixed
              
            } catch (DatabaseException e) {
              return null;
            }
            if (rightIterator.hasNext()) {
              rightIterator.next();//skip the header page
            }
              
            //initialize all of right pages 
            if (rightIterator.hasNext()) {
              //rightBlock = new ArrayList(blockMinusTwo);
              // for (int i = 0; i<blockMinusTwo; i++) {//then add later pages to block
              //   if (rightIterator.hasNext()) {
              //     rightBlock.add(i, rightIterator.next());
              //   }
              // }
              rightPage = rightIterator.next();
              //rightPageNumInBlock = 0;
              rightSlotId = -1;
              //rightIterator.next();//skip the header page
              try {
                this.rightPageHeader = BNLJOperator.this.getPageHeader(rightTableName, rightPage);//left page header// ******errorfixed 
              } catch (DatabaseException e) {
                 return null;
              }
            }
            
            return getNextLeftRecord();//initialized left and right, then call next things
          } else {//5. end all iterator
            return null;//reach end of the loop
          }
        }

      }

    }
    private Record getNextRightRecord() {
      int nextRightSlotNum = getNextRightPageAndSlotNum();
      if (nextRightSlotNum != -1) {//right has next on this page
        RecordID rid = new RecordID(rightPage.getPageNum(), nextRightSlotNum);//pop the record with next slotid
        rightSlotId = nextRightSlotNum;
        try {
          return BNLJOperator.this.getTransaction().getRecord(rightTableName, rid);//return the record******errorfixed
        } catch (DatabaseException e) {
          return null;
        }
      } else {
        return null;//wait to call getNextLeft rec to update the right pages to proceed
      }    
    }







//************************************helper functions above****************************************

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      // TODO: implement me!

      if (this.nextRecord != null) return true;
      if (leftRecord == null) {//initialize to first value
        leftRecord = getNextLeftRecord();
      }
      
      while (leftRecord!= null) {//loop left value
        rightRecord = getNextRightRecord();//initial value, proceed to next unseen value
        while (rightRecord != null) {//if == null call next leftrec, loop right value
          DataType leftJoinValue = leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
          DataType rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());

          if (leftJoinValue.equals(rightJoinValue)) {//do the comparison
            List<DataType> leftValues = new ArrayList<DataType>(leftRecord.getValues());
            List<DataType> rightValues = new ArrayList<DataType>(rightRecord.getValues());

            leftValues.addAll(rightValues);
            this.nextRecord = new Record(leftValues);
            return true;//get the wanted value and left right pointers all point to the desired slot id
          } else {
            rightRecord = getNextRightRecord();//update right loop 
          }
        
        }
        leftRecord = getNextLeftRecord();
      }
      return false;

    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      // TODO: implement me!
      if (this.hasNext()) {
        Record r = this.nextRecord;
        this.nextRecord = null;
        return r;
      } 
      throw new NoSuchElementException();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
