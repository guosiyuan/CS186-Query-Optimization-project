package edu.berkeley.cs186.database.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordID;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;
import edu.berkeley.cs186.database.io.Page;

public abstract class JoinOperator extends QueryOperator {

  public enum JoinType {
    SNLJ,
    PNLJ,
    BNLJ,
    GRACEHASH
  }

  private String leftColumnName;
  private String rightColumnName;

  private int leftColumnIndex;
  private int rightColumnIndex;

  private JoinType joinType;
  private QueryOperator leftSource;
  private QueryOperator rightSource;
  private Database.Transaction transaction;

  /**
   * Create a join operator that pulls tuples from leftSource and rightSource. Returns tuples for which
   * leftColumnName and rightColumnName are equal. The type of join to execute must be specified.
   *
   * @param leftSource the left source operator
   * @param rightSource the right source operator
   * @param leftColumnName the column to join on from leftSource
   * @param rightColumnName the column to join on from rightSource
   * @param joinType the type of join this operator executes
   * @throws QueryPlanException
   */
  public JoinOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction,
                      JoinType joinType) throws QueryPlanException, DatabaseException {
    super(OperatorType.JOIN);

    this.joinType = joinType;

    this.leftSource = leftSource;
    this.rightSource = rightSource;

    this.leftColumnName = leftColumnName;
    this.rightColumnName = rightColumnName;
    this.setOutputSchema(this.computeSchema());

    this.transaction = transaction;
  }

  /**
   * Joins tuples from leftSource and rightSource and returns an iterator of records.
   * Executes a join using simple nested loop join.
   *
   * @return an iterator of records
   * @throws QueryPlanException
   * @throws DatabaseException
   */
  public Iterator<Record> execute() throws QueryPlanException, DatabaseException {
    List<Record> newRecords = new ArrayList<Record>();
    Iterator<Record> leftIterator = this.leftSource.execute();

    while (leftIterator.hasNext()) {
      Record leftRecord = leftIterator.next();

      Iterator<Record> rightIterator = this.rightSource.execute();
      while (rightIterator.hasNext()) {
        Record rightRecord = rightIterator.next();

        DataType leftJoinValue = leftRecord.getValues().get(this.leftColumnIndex);
        DataType rightJoinValue = rightRecord.getValues().get(this.rightColumnIndex);

        if (leftJoinValue.equals(rightJoinValue)) {
          List<DataType> leftValues = new ArrayList<DataType>(leftRecord.getValues());
          List<DataType> rightValues = new ArrayList<DataType>(rightRecord.getValues());

          leftValues.addAll(rightValues);
          newRecords.add(new Record(leftValues));
        }
      }
    }

    return newRecords.iterator();
  }

  public abstract Iterator<Record> iterator() throws QueryPlanException, DatabaseException;

  @Override
  public QueryOperator getSource() throws QueryPlanException {
    throw new QueryPlanException("There is no single source for join operators. Please use " +
        "getRightSource and getLeftSource and the corresponding set methods.");
  }

  public QueryOperator getLeftSource() {
    return this.leftSource;
  }

  public QueryOperator getRightSource() {
    return this.rightSource;
  }

  public void setLeftSource(QueryOperator leftSource) {
    this.leftSource = leftSource;
  }

  public void setRightSource(QueryOperator rightSource) {
    this.rightSource = rightSource;
  }

  public Schema computeSchema() throws QueryPlanException {
    Schema leftSchema = this.leftSource.getOutputSchema();
    Schema rightSchema = this.rightSource.getOutputSchema();
    List<String> leftSchemaNames = new ArrayList<String>(leftSchema.getFieldNames());
    List<String> rightSchemaNames = new ArrayList<String>(rightSchema.getFieldNames());

    this.leftColumnName = this.checkSchemaForColumn(leftSchema, this.leftColumnName);
    this.leftColumnIndex = leftSchemaNames.indexOf(leftColumnName);

    this.rightColumnName = this.checkSchemaForColumn(rightSchema, this.rightColumnName);
    this.rightColumnIndex = rightSchemaNames.indexOf(rightColumnName);

    List<DataType> leftSchemaTypes = new ArrayList<DataType>(leftSchema.getFieldTypes());
    List<DataType> rightSchemaTypes = new ArrayList<DataType>(rightSchema.getFieldTypes());

    if (!leftSchemaTypes.get(this.leftColumnIndex).getClass().equals(rightSchemaTypes.get(
        this.rightColumnIndex).getClass())) {
      throw new QueryPlanException("Mismatched types of columns " + leftColumnName + " and "
          + rightColumnName + ".");
    }

    leftSchemaNames.addAll(rightSchemaNames);
    leftSchemaTypes.addAll(rightSchemaTypes);

    return new Schema(leftSchemaNames, leftSchemaTypes);
  }

  public String str() {
    return "type: " + this.joinType +
        "\nleftColumn: " + this.leftColumnName +
        "\nrightColumn: " + this.rightColumnName;
  }

  public String toString() {
    String r = this.str();
    if (this.leftSource != null) {
      r += "\n" + ("(left)\n" + this.leftSource.toString()).replaceAll("(?m)^", "\t");
    }
    if (this.rightSource != null) {
      if (this.leftSource != null) {
        r += "\n";
      }
      r += "\n" + ("(right)\n" + this.rightSource.toString()).replaceAll("(?m)^", "\t");
    }
    return r;
  }

  /**
   * Estimates the table statistics for the result of executing this query operator.
   *
   * @return estimated TableStats
   */
  public TableStats estimateStats() throws QueryPlanException {
    TableStats leftStats = this.leftSource.getStats();
    TableStats rightStats = this.rightSource.getStats();

    return leftStats.copyWithJoin(this.leftColumnIndex,
                                  rightStats,
                                  this.rightColumnIndex);
  }

  public abstract int estimateIOCost() throws QueryPlanException;

  public Iterator<Page> getPageIterator(String tableName) throws DatabaseException {
    return this.transaction.getPageIterator(tableName);
  }

  public byte[] getPageHeader(String tableName, Page p) throws DatabaseException {
    return this.transaction.readPageHeader(tableName, p);
  }

  public int getNumEntriesPerPage(String tableName) throws DatabaseException {
    return this.transaction.getNumEntriesPerPage(tableName);
  }

  public int getEntrySize(String tableName) throws DatabaseException {
    return this.transaction.getEntrySize(tableName);
  }

  public int getHeaderSize(String tableName) throws DatabaseException {
    return this.transaction.getPageHeaderSize(tableName);
  }

  public String getLeftColumnName() {
    return this.leftColumnName;
  }

  public String getRightColumnName() {
    return this.rightColumnName;
  }

  public Database.Transaction getTransaction() {
    return this.transaction;
  }

  public int getLeftColumnIndex() {
    return this.leftColumnIndex;
  }

  public int getRightColumnIndex() {
    return this.rightColumnIndex;
  }

  public Iterator<Record> getTableIterator(String tableName) throws DatabaseException {
    return this.transaction.getRecordIterator(tableName);
  }

  public void createTempTable(Schema schema, String tableName) throws DatabaseException {
    this.transaction.createTempTable(schema, tableName);
  }

  public RecordID addRecord(String tableName, List<DataType> values) throws DatabaseException {
    return this.transaction.addRecord(tableName, values);
  }

  public JoinType getJoinType() {
    return this.joinType;
  }
}
