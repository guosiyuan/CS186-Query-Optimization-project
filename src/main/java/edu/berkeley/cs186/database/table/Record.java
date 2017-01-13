package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.datatypes.DataType;

import java.util.List;
import java.lang.StringBuilder;

/**
 * A wrapper class for an individual record. Simply stores a list of DataTypes.
 */
public class Record {
  private List<DataType> values;

  public Record(List<DataType> values) {
    this.values = values;
  }

  public List<DataType> getValues() {
    return this.values;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Record)) {
      return false;
    }

    Record otherRecord = (Record) other;

    if (values.size() != otherRecord.values.size()) {
      return false;
    }

    for (int i = 0; i < values.size(); i++) {
      if (!(values.get(i).equals(otherRecord.values.get(i)))) {
        return false;
      }
    }

    return true;
  }

  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    for (DataType d : values) {
      s.append(d.toString().trim());
      s.append(", ");
    }
    return s.substring(0, s.length() -2);
  }
}
