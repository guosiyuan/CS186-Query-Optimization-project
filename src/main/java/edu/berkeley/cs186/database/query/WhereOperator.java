package edu.berkeley.cs186.database.query;

import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.Iterator;
import java.util.List;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.table.MarkerRecord;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

public class WhereOperator extends QueryOperator {
  private int columnIndex;
  private String columnName;
  private QueryPlan.PredicateOperator predicate;
  private DataType value;

  /**
   * Creates a new WhereOperator that pulls from source and only returns tuples for which the
   * predicate is satisfied.
   *
   * @param source the source of this operator
   * @param columnName the name of the column to evaluate the predicate on
   * @param predicate the actual comparator
   * @param value the value to compare against
   * @throws QueryPlanException
   */
  public WhereOperator(QueryOperator source,
                       String columnName,
                       QueryPlan.PredicateOperator predicate,
                       DataType value) throws QueryPlanException {
    super(OperatorType.WHERE, source);
    this.predicate = predicate;
    this.value = value;

    this.columnName = this.checkSchemaForColumn(source.getOutputSchema(), columnName);
    this.columnIndex = this.getOutputSchema().getFieldNames().indexOf(this.columnName);

    this.stats = this.estimateStats();
    this.cost = this.estimateIOCost();
  }

  public Iterator<Record> execute() throws QueryPlanException, DatabaseException {
    Iterator<Record> sourceIterator = this.getSource().execute();
    List<Record> validRecords = new ArrayList<Record>();
    MarkerRecord markerRecord = MarkerRecord.getMarker();

    while (sourceIterator.hasNext()) {
      Record r = sourceIterator.next();

      if (r == markerRecord) {
        validRecords.add(r);
      } else {

        switch (this.predicate) {
          case EQUALS:
            if (r.getValues().get(this.columnIndex).equals(value)) {
              validRecords.add(r);
            }
            break;

          case NOT_EQUALS:
            if (!r.getValues().get(this.columnIndex).equals(value)) {
              validRecords.add(r);
            }
            break;

          case LESS_THAN:
            if (r.getValues().get(this.columnIndex).compareTo(value) == -1) {
              validRecords.add(r);
            }
            break;

          case LESS_THAN_EQUALS:
            if (r.getValues().get(this.columnIndex).compareTo(value) == -1) {
              validRecords.add(r);
            } else if (r.getValues().get(this.columnIndex).compareTo(value) == 0) {
              validRecords.add(r);
            }
            break;
          case GREATER_THAN:
            if (r.getValues().get(this.columnIndex).compareTo(value) == 1) {
              validRecords.add(r);
            }
            break;

          case GREATER_THAN_EQUALS:
            if (r.getValues().get(this.columnIndex).compareTo(value) == 1) {
              validRecords.add(r);
            } else if (r.getValues().get(this.columnIndex).compareTo(value) == 0) {
              validRecords.add(r);
            }
            break;

          default:
            break;
        }
      }

    }

    return validRecords.iterator();
  }

  public Schema computeSchema() throws QueryPlanException {
    return this.getSource().getOutputSchema();
  }

  public String str() {
    return "type: " + this.getType() +
        "\ncolumn: " + this.columnName +
        "\npredicate: " + this.predicate +
        "\nvalue: " + this.value;
  }

  /**
   * Estimates the table statistics for the result of executing this query operator.
   *
   * @return estimated TableStats
   */
  public TableStats estimateStats() throws QueryPlanException {
    TableStats stats = this.source.getStats();
    return stats.copyWithPredicate(this.columnIndex,
                                   this.predicate,
                                   this.value);
  }

  public int estimateIOCost() throws QueryPlanException {
    return this.getSource().getIOCost();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException { return new WhereIterator(); }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class WhereIterator implements Iterator<Record> {
    private Iterator<Record> sourceIterator;
    private MarkerRecord markerRecord;
    private Record nextRecord;

    public WhereIterator() throws QueryPlanException, DatabaseException {
      this.sourceIterator = WhereOperator.this.getSource().iterator();
      this.markerRecord = MarkerRecord.getMarker();
      this.nextRecord = null;
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      if (this.nextRecord != null) {
        return true;
      }
      while (this.sourceIterator.hasNext()) {
        Record r = this.sourceIterator.next();

        if (r == this.markerRecord) {
          this.nextRecord = r;
          return true;
        }

        switch (WhereOperator.this.predicate) {
          case EQUALS:
            if (r.getValues().get(WhereOperator.this.columnIndex).equals(value)) {
              this.nextRecord = r;
              return true;
            }
            break;
          case NOT_EQUALS:
            if (!r.getValues().get(WhereOperator.this.columnIndex).equals(value)) {
              this.nextRecord = r;
              return true;
            }
            break;
          case LESS_THAN:
            if (r.getValues().get(WhereOperator.this.columnIndex).compareTo(value) == -1) {
              this.nextRecord = r;
              return true;
            }
            break;
          case LESS_THAN_EQUALS:
            if (r.getValues().get(WhereOperator.this.columnIndex).compareTo(value) == -1) {
              this.nextRecord = r;
              return true;
            } else if (r.getValues().get(WhereOperator.this.columnIndex).compareTo(value) == 0) {
              this.nextRecord = r;
              return true;
            }
            break;
          case GREATER_THAN:
            if (r.getValues().get(WhereOperator.this.columnIndex).compareTo(value) == 1) {
              this.nextRecord = r;
              return true;
            }
            break;
          case GREATER_THAN_EQUALS:
            if (r.getValues().get(WhereOperator.this.columnIndex).compareTo(value) == 1) {
              this.nextRecord = r;
              return true;
            } else if (r.getValues().get(WhereOperator.this.columnIndex).compareTo(value) == 0) {
              this.nextRecord = r;
              return true;
            }
            break;
          default:
            break;
        }
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
      while (this.hasNext()) {
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
