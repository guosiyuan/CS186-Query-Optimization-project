package edu.berkeley.cs186.database.table;

import java.util.ArrayList;

import edu.berkeley.cs186.database.datatypes.DataType;

/**
 * An empty record used to delineate groups in the GroupByOperator.
 */
public class MarkerRecord extends Record{
  private static final MarkerRecord record = new MarkerRecord();

  private MarkerRecord() {
    super(new ArrayList<DataType>());
  }

  public static MarkerRecord getMarker() {
    return MarkerRecord.record;
  }
}
