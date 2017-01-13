package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.stats.TableStats;


public class GraceHashOperator extends JoinOperator {

  private int numBuffers;

  public GraceHashOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
          rightSource,
          leftColumnName,
          rightColumnName,
          transaction,
          JoinType.GRACEHASH);

    this.numBuffers = transaction.getNumMemoryPages();
    this.stats = this.estimateStats();
    this.cost = this.estimateIOCost();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new GraceHashIterator();
  }

  public int estimateIOCost() throws QueryPlanException {
    // TODO: implement me!
    return 3 * (this.getLeftSource().getStats().getNumPages() + this.getRightSource().getStats().getNumPages());
    
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class GraceHashIterator implements Iterator<Record> {
    private Iterator<Record> leftIterator;
    private Iterator<Record> rightIterator;
    private Record nextRecord;
    private String[] leftPartitions;
    private String[] rightPartitions;
    private int currentPartition;
    private Map<DataType, ArrayList<Record>> inMemoryHashTable;
    private Iterator<Record> leftRecordIter;//to store states
    private Iterator<Record> rightRecordIter;//to store states
    private Record rightRec;



    public GraceHashIterator() throws QueryPlanException, DatabaseException {
      this.leftIterator = getLeftSource().iterator();
      this.rightIterator = getRightSource().iterator();
      leftPartitions = new String[numBuffers - 1];//store table names, 1,2,3...["table1", "table2",...]
      rightPartitions = new String[numBuffers - 1];
      String leftTableName;
      String rightTableName;
      for (int i = 0; i < numBuffers - 1; i++) {
        leftTableName = "Temp HashJoin Left Partition " + Integer.toString(i);
        rightTableName = "Temp HashJoin Right Partition " + Integer.toString(i);
        GraceHashOperator.this.createTempTable(getLeftSource().getOutputSchema(), leftTableName);
        GraceHashOperator.this.createTempTable(getRightSource().getOutputSchema(), rightTableName);
        leftPartitions[i] = leftTableName;
        rightPartitions[i] = rightTableName;
      }

      // TODO: implement me!
    
      phaseOne();
      currentPartition = -1;


    }

//helper func below************************************************************************helper func below(partition 1, partition 2)
    private void phaseOne() {
//doing left partition phase 1
      while (leftIterator.hasNext()) {
        Record lrec = leftIterator.next();
        int destinationTableNum = lrec.getValues().get(GraceHashOperator.this.getLeftColumnIndex()).hashCode()%(numBuffers - 1);
        String tableName = leftPartitions[destinationTableNum];
        try {
          GraceHashOperator.this.addRecord(tableName, lrec.getValues());
        } catch (DatabaseException e) {
          return;
        }
      }
//doing right partition phase1
      while (rightIterator.hasNext()) {
        Record rrec = rightIterator.next();
        int destinationTableNum = rrec.getValues().get(GraceHashOperator.this.getRightColumnIndex()).hashCode()%(numBuffers - 1);
        String tableName = rightPartitions[destinationTableNum];
        try {
          GraceHashOperator.this.addRecord(tableName, rrec.getValues());
        } catch (DatabaseException e) {
          return;
        }
      }
    }

    private void loadNextCurrPartition() {//to update for next rightRecordIter & leftRecordIter, only be called when currentPartition is used up
      inMemoryHashTable = new HashMap();
      //currentPartition+=1;
      int i = currentPartition+1;
      if (i == numBuffers-1) return;//out of bounds
      String leftTableName = leftPartitions[i];
      String rightTableName = rightPartitions[i];
      Iterator<Record> leftRIter;
      try {
        leftRIter = GraceHashOperator.this.getTableIterator(leftTableName);
        rightRecordIter = GraceHashOperator.this.getTableIterator(rightTableName);
        leftRecordIter = null;
      } catch (DatabaseException e) {
        return;
      }
      //begin loading left records to in memory hash table
      while (leftRIter.hasNext()) {
        Record lRec = leftRIter.next();
        DataType leftDataType = lRec.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
        //then append the record to the place corresponding to leftDataType
        if (inMemoryHashTable.containsKey(leftDataType)) {//already has the key
          ArrayList<Record> arrList = inMemoryHashTable.get(leftDataType);
          arrList.add(lRec);
        } else {
          ArrayList<Record> arrList = new ArrayList();
          arrList.add(lRec);
          inMemoryHashTable.put(leftDataType, arrList);
        }
      }
    }









//helper func above****************************************************************************helper func above(partition1, partition 2)


    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      if (nextRecord!= null) return true;
      if (currentPartition == numBuffers-1) return false;//eof
      if (currentPartition == -1) {
        loadNextCurrPartition();//initial condition
        currentPartition+=1;
        return hasNext();
      }
      if (leftRecordIter!= null && leftRecordIter.hasNext()) {//inner loop, loop it over
        while (leftRecordIter.hasNext()) {//finish the tail
          Record leftRec = leftRecordIter.next();
          //get two records, begin comparing
          DataType leftJoinValue = leftRec.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
          DataType rightJoinValue = rightRec.getValues().get(GraceHashOperator.this.getRightColumnIndex());
          if (leftJoinValue.equals(rightJoinValue)) {
            List<DataType> leftValues = new ArrayList<DataType>(leftRec.getValues());
            List<DataType> rightValues = new ArrayList<DataType>(rightRec.getValues());
            leftValues.addAll(rightValues);
            nextRecord = new Record(leftValues);
            return true;
          }
        }
        return hasNext();//finish the inner tail and fail to find goods, so proceed to next loop of hasNext();
      } else if (rightRecordIter.hasNext()) {
        while (rightRecordIter.hasNext()) {
          rightRec = rightRecordIter.next();
          DataType rightDataType = rightRec.getValues().get(GraceHashOperator.this.getRightColumnIndex());
          ArrayList<Record> arrList = inMemoryHashTable.get(rightDataType);
          leftRecordIter = arrList.iterator();
          while (leftRecordIter.hasNext()) {
            Record leftRec = leftRecordIter.next();
            //get two records, begin comparing
            DataType leftJoinValue = leftRec.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
            DataType rightJoinValue = rightRec.getValues().get(GraceHashOperator.this.getRightColumnIndex());
            if (leftJoinValue.equals(rightJoinValue)) {
              List<DataType> leftValues = new ArrayList<DataType>(leftRec.getValues());
              List<DataType> rightValues = new ArrayList<DataType>(rightRec.getValues());
              leftValues.addAll(rightValues);
              nextRecord = new Record(leftValues);
              return true;
            }
          }
        }
        return hasNext();//cant find good record in this currentPartition, proceed to next one
      } else {//outer most loop
        loadNextCurrPartition();
        currentPartition+=1;
        return hasNext();
      }
      // TODO: implement me!

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
