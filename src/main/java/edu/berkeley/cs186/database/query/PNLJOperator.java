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
import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.stats.TableStats;
import edu.berkeley.cs186.database.table.RecordID;
public class PNLJOperator extends JoinOperator {

  public PNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
          rightSource,
          leftColumnName,
          rightColumnName,
          transaction,
          JoinType.PNLJ);

    this.stats = this.estimateStats();
    this.cost = this.estimateIOCost();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new PNLJIterator();
  }

  public int estimateIOCost() throws QueryPlanException {
    // TODO: implement me!
    return (this.getLeftSource().getStats().getNumPages() * this.getRightSource().getStats().getNumPages()) + this.getLeftSource().getStats().getNumPages();
    
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
    private class PNLJIterator implements Iterator<Record> {
      private String leftTableName;//use
      private String rightTableName;//use
      private Iterator<Page> leftIterator;//use
      private Iterator<Page> rightIterator;//use
      private Record leftRecord;//use
      private Record nextRecord;//use, can be null
      private Record rightRecord;//use
      private Page leftPage;//use, can be null
      private Page rightPage;//use, can be null
      private byte[] leftPageHeader;//use, can be null
      private byte[] rightPageHeader;//use, can be null
      private int leftSlotId;//use, can be -1
      private int rightSlotId;//use, can be -1
      private int lnEPP;//how many slots/page for table left
      private int rnEPP;//see above
      //private int leftRecGivenPage//use, left records given so far on this page, left
      //private int rightRecGivenPage//use, right records given so far on this page, right


      public PNLJIterator() throws QueryPlanException, DatabaseException {//constructor
          if (PNLJOperator.this.getLeftSource().isSequentialScan()) {
              this.leftTableName = ((SequentialScanOperator) PNLJOperator.this.getLeftSource()).getTableName();
          } else {
              this.leftTableName = "Temp" + PNLJOperator.this.getJoinType().toString() + "Operator" + PNLJOperator.this.getLeftColumnName() + "Left";
              PNLJOperator.this.createTempTable(PNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName);
              Iterator<Record> leftIter = PNLJOperator.this.getLeftSource().iterator();
              while (leftIter.hasNext()) {
                  PNLJOperator.this.addRecord(leftTableName, leftIter.next().getValues());
              }
          }

          if (PNLJOperator.this.getRightSource().isSequentialScan()) {
              this.rightTableName = ((SequentialScanOperator) PNLJOperator.this.getRightSource()).getTableName();
          } else {
              this.rightTableName = "Temp" + PNLJOperator.this.getJoinType().toString() + "Operator" + PNLJOperator.this.getRightColumnName() + "Right";
              PNLJOperator.this.createTempTable(PNLJOperator.this.getRightSource().getOutputSchema(), rightTableName);
              Iterator<Record> rightIter = PNLJOperator.this.getRightSource().iterator();
              while (rightIter.hasNext()) {
                  PNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
              }
          }

          // TODO: implement me!
          
          this.leftIterator = PNLJOperator.this.getPageIterator(leftTableName);//page iterator
          this.rightIterator = PNLJOperator.this.getPageIterator(rightTableName);

          leftRecord = null;
          rightRecord = null;
          nextRecord = null;
                  
          if (leftIterator.hasNext()) {
            leftIterator.next();//skip the header page
          }
          if (rightIterator.hasNext()) {
            rightIterator.next();//skip the header page
          }
          
          
          leftSlotId = -1;//left entry num, initialize to -1
          if (leftIterator.hasNext()) {
            this.leftPage = leftIterator.next();//the first page in the table, left page
            this.leftPageHeader = PNLJOperator.this.getPageHeader(leftTableName, leftPage);//left page header
            this.lnEPP = PNLJOperator.this.getNumEntriesPerPage(leftTableName);//max number of entries can be contained in page
          } else {//nothing in the table
            this.leftPage = null;
            this.leftPageHeader = null;
            this.lnEPP = 0;
          }
          


          rightSlotId = -1;//right entry num
          if (rightIterator.hasNext()) {
            this.rightPage = rightIterator.next();//the first page in the table, left page
            this.rightPageHeader = PNLJOperator.this.getPageHeader(rightTableName, rightPage);//left page header
            this.rnEPP = PNLJOperator.this.getNumEntriesPerPage(rightTableName);//max number of entries can be contained in page
          } else {
            this.rightPage = null;
            this.rightPageHeader = null;
            this.rnEPP = 0;
          }
      }

    //little helper func, find next element on curr left page
    private int getNextLeftSlotNum(){//next available slot num on he current page, -1 if reach the end of page
      if (this.leftPage == null) return -1;//no pages in left table
      for (int i = leftSlotId+1; i<lnEPP; i++) {//find first rec on the right page
        byte b = leftPageHeader[i/8];//i serves as the current slot id we check
        int bitOffset = 7 - (i % 8);
        byte mask = (byte) (1 << bitOffset);
        byte value = (byte) (b & mask);//if value == 1 that means the entryNum is valid, there are record in the place
        if (value!= 0) {//find the first valid slot id
          return i;
        }
      }
      return -1;//eof, no next record on this page
    }
    //same as above, right ver
    private int getNextRightSlotNum(){//next available slot num on he current page, -1 if reach the end of page
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

    //larger helper func, get next left record
    private Record getNextLeftRecord() {//locate next
      int nextLeftSlotNum = getNextLeftSlotNum();
      if (nextLeftSlotNum != -1) {//1. left has next on this page, also need to refresh the right page
        RecordID rid = new RecordID(leftPage.getPageNum(), nextLeftSlotNum);//pop the record with next slotid
        leftSlotId = nextLeftSlotNum;//update leftslotnum with new slotid, page don't change

        rightSlotId = -1;//refresh the right page 
        try {
          return PNLJOperator.this.getTransaction().getRecord(leftTableName, rid);//return the record*****error datab fixed
        } catch (DatabaseException e) {
          return null;
        }
      } else {//reach the end of this left page 
        if (rightIterator.hasNext()) {//2. 3. then do the initialization for the next right pages, then return the first of this left page,##right has next page
          this.rightSlotId = -1;
          rightPage = rightIterator.next();
          try {
            rightPageHeader = PNLJOperator.this.getPageHeader(rightTableName, rightPage);//*******error data fixed
          } catch (DatabaseException e) {
            return null;
          }

          //then set the left count to -1
          leftSlotId = -1;//initialize to -1
          //leftSlotId = getNextLeftSlotNum();//rebuild leftslotid to top most value
          return getNextLeftRecord();
        } else {//right reach the end of file, left proceed to the next page
          if (leftIterator.hasNext()) {//4. update to next left page, also initialize right pages to beginning condition
            leftSlotId = -1;
            leftPage = leftIterator.next();
            try {
              leftPageHeader = PNLJOperator.this.getPageHeader(leftTableName, leftPage);//*****error data fixed
              rightIterator = PNLJOperator.this.getPageIterator(rightTableName);//new right iterator//*******error data fixed

            } catch (DatabaseException e) {
              return null;
            }
            //initialize all of right pages 
            if (rightIterator.hasNext()) {
              rightIterator.next();//skip the header page
            }

            rightSlotId = -1;//right entry num
            this.rightPage = rightIterator.next();//the first page in the table, right page, we already knoe that this iterator has a next
            try {
              this.rightPageHeader = PNLJOperator.this.getPageHeader(rightTableName, rightPage);//left page header// ******errorfixed
              
            } catch (DatabaseException e) {
               return null;
            }
            return getNextLeftRecord();//initialized left and right, then call next things
          } else {//5. end all iterator
            return null;//reach end of the loop
          }
        }

      }

    }
    //helper func, get next right record
    private Record getNextRightRecord() {
      int nextRightSlotNum = getNextRightSlotNum();
      if (nextRightSlotNum != -1) {//right has next on this page
        RecordID rid = new RecordID(rightPage.getPageNum(), nextRightSlotNum);//pop the record with next slotid
        rightSlotId = nextRightSlotNum;
        try {
          return PNLJOperator.this.getTransaction().getRecord(rightTableName, rid);//return the record******errorfixed
        } catch (DatabaseException e) {
          return null;
        }
      } else {
        return null;//wait to call getNextLeft rec to update the right pages to proceed
      }
    }

    public boolean hasNext() {
      // TODO: implement me!
      if (this.nextRecord != null) return true;
      if (leftRecord == null) {//initialize to first value
        leftRecord = getNextLeftRecord();
      }
      
      while (leftRecord!= null) {//loop left value
        rightRecord = getNextRightRecord();//initial value, proceed to next unseen value
        while (rightRecord != null) {//if == null call next leftrec, loop right value
          DataType leftJoinValue = leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
          DataType rightJoinValue = rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());

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
