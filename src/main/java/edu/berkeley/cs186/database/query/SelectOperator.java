package edu.berkeley.cs186.database.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.datatypes.FloatDataType;
import edu.berkeley.cs186.database.datatypes.IntDataType;
import edu.berkeley.cs186.database.table.MarkerRecord;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

public class SelectOperator extends QueryOperator {
  private List<String> columns;
  private List<Integer> indices;
  private boolean hasCount;
  private int averageColumnIndex;
  private int sumColumnIndex;
  private boolean hasAggregate = false;

  private int countValue;
  private double sumValue;
  private double averageSumValue;
  private int averageCountValue;

  private String sumColumn;
  private String averageColumn;

  private boolean sumIsFloat;

  /**
   * Creates a new SelectOperator that reads tuples from source and filters out columns. Optionally
   * computers an aggregate if it is specified.
   *
   * @param source
   * @param columns
   * @param count
   * @param averageColumn
   * @param sumColumn
   * @throws QueryPlanException
   */
  public SelectOperator(QueryOperator source,
                        List<String> columns,
                        boolean count,
                        String averageColumn,
                        String sumColumn) throws QueryPlanException {
    super(OperatorType.SELECT);

    this.columns = columns;
    this.indices = new ArrayList<Integer>();

    this.countValue = 0;
    this.sumValue = 0;
    this.averageCountValue = 0;
    this.averageSumValue = 0;

    this.averageColumnIndex = -1;
    this.sumColumnIndex = -1;

    this.sumColumn = sumColumn;
    this.averageColumn = averageColumn;

    this.hasCount = count;
    this.hasAggregate = this.hasCount || averageColumn != null || sumColumn != null;

    // NOTE: Don't need to explicitly set the output schema because setting the source recomputes
    // the schema for the query optimization case.
    this.setSource(source);

    this.stats = this.estimateStats();
    this.cost = this.estimateIOCost();
  }

  protected Schema computeSchema() throws QueryPlanException {
    // check to make sure that the source operator is giving us columns that we select
    Schema sourceSchema = this.getSource().getOutputSchema();
    List<String> sourceColumnNames = new ArrayList<String>(sourceSchema.getFieldNames());
    List<DataType> sourceColumnTypes = new ArrayList<DataType>(sourceSchema.getFieldTypes());
    List<DataType> columnTypes = new ArrayList<DataType>();

    for (String columnName : this.columns) {
      columnName = this.checkSchemaForColumn(sourceSchema, columnName);

      int sourceColumnIndex = sourceColumnNames.indexOf(columnName);
      columnTypes.add(sourceColumnTypes.get(sourceColumnIndex));
      this.indices.add(sourceColumnIndex);
    }

    if (this.sumColumn != null) {
      this.sumColumn = this.checkSchemaForColumn(sourceSchema, this.sumColumn);
      this.sumColumnIndex = sourceColumnNames.indexOf(this.sumColumn);

      if (!(sourceColumnTypes.get(this.sumColumnIndex) instanceof IntDataType) &&
          !(sourceColumnTypes.get(this.sumColumnIndex) instanceof FloatDataType)) {
        throw new QueryPlanException("Cannot compute sum over a non-integer column: " + this.sumColumn + ".");
      }
    }

    if (this.averageColumn != null) {
      this.averageColumn = this.checkSchemaForColumn(sourceSchema, this.averageColumn);
      this.averageColumnIndex = sourceColumnNames.indexOf(this.averageColumn);

      if (!(sourceColumnTypes.get(this.averageColumnIndex) instanceof IntDataType) &&
          !(sourceColumnTypes.get(this.sumColumnIndex) instanceof FloatDataType)) {
        throw new QueryPlanException("Cannot compute sum over a non-integer column: " + this.averageColumn + ".");
      }
    }


    // make sure we add the correct columns to the output schema if we have aggregates in the
    // selection
    if (this.hasAggregate) {
      if (this.hasCount) {
        this.columns.add("countAgg");
        columnTypes.add(new IntDataType());
      }

      if (this.sumColumn != null) {
        this.columns.add("sumAgg");

        if (sourceColumnTypes.get(this.sumColumnIndex) instanceof IntDataType) {
          columnTypes.add(new IntDataType());
          this.sumIsFloat = false;
        } else {
          columnTypes.add(new FloatDataType());
          this.sumIsFloat = true;
        }
      }

      if (this.averageColumn != null) {
        this.columns.add("averageAgg");
        columnTypes.add(new FloatDataType());
      }
    }

    return new Schema(this.columns, columnTypes);
  }

  /**
   * Projects tuples from the source operator onto the columns specified in the SELECT.
   * Preserves groups and calculates aggregates as well.
   *
   * @return an iterator of records
   * @throws QueryPlanException
   * @throws DatabaseException
   */
  public Iterator<Record> execute() throws QueryPlanException, DatabaseException {
    Iterator<Record> sourceIterator = this.getSource().execute();
    List<Record> newRecords = new ArrayList<Record>();
    MarkerRecord markerRecord = MarkerRecord.getMarker();

    // the case where the select has one or more aggregates in it
    if (this.hasAggregate) {
      // pretend that there's a marker record at the beginning of the table
      boolean prevWasMarker = true;
      List<DataType> baseValues = new ArrayList<DataType>();

      while (sourceIterator.hasNext()) {
        Record r = sourceIterator.next();
        List<DataType> recordValues = r.getValues();

        // if the record is a MarkerRecord, that means we reached the end of a group... we reset
        // the aggregates and add the appropriate new record to the new Records
        if (r == markerRecord) {
          if (this.hasCount) {
            int count = this.getAndResetCount();
            baseValues.add(new IntDataType(count));
          }

          if (this.sumColumnIndex != -1) {
            double sum = this.getAndResetSum();

            if (this.sumIsFloat) {
              baseValues.add(new FloatDataType((float) sum));
            } else {
              baseValues.add(new IntDataType((int) sum));
            }
          }

          if (this.averageColumnIndex != -1) {
            double average = (float) this.getAndResetAverage();
            baseValues.add(new FloatDataType((float) average));
          }

          // record that we just saw a marker record
          prevWasMarker = true;

          newRecords.add(new Record(baseValues));
          baseValues = new ArrayList<DataType>();
        } else {
          // if the previous record was a marker (or for the first record) we have to get the relevant
          // fields out of the record
          if (prevWasMarker) {
            for (int index : this.indices) {
              baseValues.add(recordValues.get(index));
            }

            prevWasMarker = false;
          }

          if (this.hasCount) {
            this.addToCount();
          }

          if (this.sumColumnIndex != -1) {
            this.addToSum(r);
          }

          if (this.averageColumnIndex != -1) {
            this.addToAverage(r);
          }
        }
      }

      // at the very end, we need to make sure we add all the aggregated records to the result
      // either because there was no group by or to add the last group we saw
      if (this.hasCount) {
        int count = this.getAndResetCount();
        baseValues.add(new IntDataType(count));
      }

      if (this.sumColumnIndex != -1) {
        double sum = this.getAndResetSum();

        if (this.sumIsFloat) {
          baseValues.add(new FloatDataType((float) sum));
        } else {
          baseValues.add(new IntDataType((int) sum));
        }
      }

      if (this.averageColumnIndex != -1) {
        double average = this.getAndResetAverage();
        baseValues.add(new FloatDataType((float) average));
      }

      newRecords.add(new Record(baseValues));
      baseValues = new ArrayList<DataType>();
    } else {
      // in the case where there are no aggregates, we simply iterate over the list of records
      // and project out the columns
      while (sourceIterator.hasNext()) {
        Record r = sourceIterator.next();
        List<DataType> recordValues = r.getValues();
        List<DataType> newValues = new ArrayList<DataType>();

        // if there is a marker record (in the case we're selecting from a group by), we simply
        // leave the marker records in
        if (r == markerRecord) {
          newRecords.add(markerRecord);
        } else {
          for (int index : this.indices) {
            newValues.add(recordValues.get(index));
          }

          Record newRecord = new Record(newValues);
          newRecords.add(newRecord);
        }
      }
    }

    return newRecords.iterator();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException { return new SelectIterator(); }

  private void addToCount() {
    this.countValue++;
  }

  private int getAndResetCount() {
    int result = this.countValue;
    this.countValue = 0;
    return result;
  }

  private void addToSum(Record record) {
    if (this.sumIsFloat) {
      this.sumValue += record.getValues().get(this.sumColumnIndex).getFloat();
    } else {
      this.sumValue += record.getValues().get(this.sumColumnIndex).getInt();
    }
  }

  private double getAndResetSum() {
    double result = this.sumValue;
    this.sumValue = 0;
    return result;
  }

  private void addToAverage(Record record) {
    this.averageCountValue++;
    this.averageSumValue += record.getValues().get(this.averageColumnIndex).getInt();
  }

  private double getAndResetAverage() {
    if (this.averageCountValue == 0) {
      return 0f;
    }

    double result =  this.averageSumValue / this.averageCountValue;
    this.averageSumValue = 0;
    this.averageCountValue = 0;
    return result;
  }

  public String str() {
    return "type: " + this.getType() +
        "\ncolumns: " + this.columns;
  }

  /**
   * Estimates the table statistics for the result of executing this query operator.
   *
   * @return estimated TableStats
   */
  public TableStats estimateStats() throws QueryPlanException {
    return this.getSource().getStats();
  }

  public int estimateIOCost() throws QueryPlanException {
    return this.getSource().getIOCost();
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class SelectIterator implements Iterator<Record> {
    private Iterator<Record> sourceIterator;
    private MarkerRecord markerRecord;
    private Record nextRecord;
    private boolean prevWasMarker;
    private List<DataType> baseValues;

    public SelectIterator() throws QueryPlanException, DatabaseException {
      this.sourceIterator = SelectOperator.this.getSource().iterator();
      this.markerRecord = MarkerRecord.getMarker();
      this.nextRecord = null;
      this.prevWasMarker = true;
      this.baseValues = new ArrayList<DataType>();
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      return this.sourceIterator.hasNext();
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      if (this.hasNext()) {
        if (SelectOperator.this.hasAggregate) {
          while (this.sourceIterator.hasNext()) {
            Record r = this.sourceIterator.next();
            List<DataType> recordValues = r.getValues();

            // if the record is a MarkerRecord, that means we reached the end of a group... we reset
            // the aggregates and add the appropriate new record to the new Records
            if (r == this.markerRecord) {
              if (SelectOperator.this.hasCount) {
                int count = SelectOperator.this.getAndResetCount();
                this.baseValues.add(new IntDataType(count));
              }

              if (SelectOperator.this.sumColumnIndex != -1) {
                double sum = SelectOperator.this.getAndResetSum();

                if (SelectOperator.this.sumIsFloat) {
                  this.baseValues.add(new FloatDataType((float) sum));
                } else {
                  this.baseValues.add(new IntDataType((int) sum));
                }
              }

              if (SelectOperator.this.averageColumnIndex != -1) {
                double average = (float) SelectOperator.this.getAndResetAverage();
                this.baseValues.add(new FloatDataType((float) average));
              }

              // record that we just saw a marker record
              this.prevWasMarker = true;

              return new Record(this.baseValues);
            } else {
              // if the previous record was a marker (or for the first record) we have to get the relevant
              // fields out of the record
              if (this.prevWasMarker) {
                this.baseValues = new ArrayList<DataType>();
                for (int index : SelectOperator.this.indices) {
                  this.baseValues.add(recordValues.get(index));
                }

                this.prevWasMarker = false;
              }

              if (SelectOperator.this.hasCount) {
                SelectOperator.this.addToCount();
              }

              if (SelectOperator.this.sumColumnIndex != -1) {
                SelectOperator.this.addToSum(r);
              }

              if (SelectOperator.this.averageColumnIndex != -1) {
                SelectOperator.this.addToAverage(r);
              }
            }
          }

          // at the very end, we need to make sure we add all the aggregated records to the result
          // either because there was no group by or to add the last group we saw
          if (SelectOperator.this.hasCount) {
            int count = SelectOperator.this.getAndResetCount();
            this.baseValues.add(new IntDataType(count));
          }

          if (SelectOperator.this.sumColumnIndex != -1) {
            double sum = SelectOperator.this.getAndResetSum();

            if (SelectOperator.this.sumIsFloat) {
              this.baseValues.add(new FloatDataType((float) sum));
            } else {
              this.baseValues.add(new IntDataType((int) sum));
            }
          }

          if (SelectOperator.this.averageColumnIndex != -1) {
            double average = SelectOperator.this.getAndResetAverage();
            this.baseValues.add(new FloatDataType((float) average));
          }

          return new Record(this.baseValues);
        } else {
          Record r = this.sourceIterator.next();
          List<DataType> recordValues = r.getValues();
          List<DataType> newValues = new ArrayList<DataType>();

          // if there is a marker record (in the case we're selecting from a group by), we simply
          // leave the marker records in
          if (r == this.markerRecord) {
            return markerRecord;
          } else {
            for (int index : SelectOperator.this.indices) {
              newValues.add(recordValues.get(index));
            }

            return new Record(newValues);
          }
        }
      }
      throw new NoSuchElementException();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
