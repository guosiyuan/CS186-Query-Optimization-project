package edu.berkeley.cs186.database.table.stats;

import edu.berkeley.cs186.database.StudentTest;
import edu.berkeley.cs186.database.StudentTestP2;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

public class FloatHistogramTest {

  @Test(timeout=1000)
  public void testSimpleHistogram() {
    FloatHistogram histogram = new FloatHistogram();

    for (int i = 0; i < 10; i++) {
      histogram.addValue(i * 1.0f);
    }

    assertEquals(10, histogram.getEntriesInRange(0.0f, 10.0f));
  }

  @Test(timeout=1000)
  public void testComplexHistogram() {
    FloatHistogram histogram = new FloatHistogram();

    for (int i = 0; i < 40; i++) {
      histogram.addValue(i * 1.0f);
    }

    assertEquals(11, histogram.getEntriesInRange(0.0f, 10.0f));
  }
}
