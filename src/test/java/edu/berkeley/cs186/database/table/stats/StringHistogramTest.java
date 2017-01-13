package edu.berkeley.cs186.database.table.stats;

import edu.berkeley.cs186.database.StudentTest;
import edu.berkeley.cs186.database.StudentTestP2;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import edu.berkeley.cs186.database.datatypes.StringDataType;
import edu.berkeley.cs186.database.query.QueryPlan.PredicateOperator;

public class StringHistogramTest {
  private String alphaNumeric = StringHistogram.alphaNumeric;

  @Test(timeout=1000)
  public void testStringHistogram() {
    StringHistogram histogram = new StringHistogram();

    for (int i = 0; i < alphaNumeric.length(); i++) {
      String iString = alphaNumeric.substring(i, i + 1);
      histogram.addValue(iString + iString + iString);
    }

    for (Bucket<String> bucket : histogram.getAllBuckets()) {
      assertEquals(1, bucket.getCount());
    }

    assertEquals(36, histogram.getNumDistinct());
    assertEquals(0, histogram.getMinValue());
    assertEquals(35, histogram.getMaxValue());
  }

  @Test(timeout=1000)
  public void testComputeReductionFactor() {
    StringHistogram histogram = new StringHistogram();

    for (int i = 0; i < alphaNumeric.length() - 12; i++) {
      String iString = alphaNumeric.substring(i, i + 1);
      histogram.addValue(iString + iString + iString);
    }

    assertEquals(24, histogram.getNumDistinct());

    StringDataType lessThanValue = new StringDataType("predicate", 3);
    assertEquals(0.625f,
                 histogram.computeReductionFactor(PredicateOperator.LESS_THAN,
                                                  lessThanValue),
                 0.001f);

    StringDataType greaterThanValue = new StringDataType("fragmentation", 3);
    assertEquals(0.75f,
                 histogram.computeReductionFactor(PredicateOperator.GREATER_THAN,
                                                  greaterThanValue),
                 0.001f);
  }

  @Test(timeout=1000)
  public void testCopyWithReduction() {
    StringHistogram histogram = new StringHistogram();

    for (int i = 0; i < alphaNumeric.length(); i++) {
      String iString = alphaNumeric.substring(i, i + 1);
      histogram.addValue(iString + iString + iString);
      histogram.addValue(iString + iString + iString);
      histogram.addValue(iString + iString + iString);
      histogram.addValue(iString + iString + iString);
    }

    assertEquals(36, histogram.getNumDistinct());

    StringHistogram copyHistogram = histogram.copyWithReduction(0.25f);

    assertEquals(36, copyHistogram.getEntriesInRange("a", "9"));

    // Note that `9` isn't the actual numDistinct in copyHistogram -
    // this is a good example of how approximations are not always correct.
    assertEquals(9, copyHistogram.getNumDistinct());
  }

  @Test(timeout=1000)
  public void testCopyWithPredicate() {
    StringHistogram histogram = new StringHistogram();

    for (int i = 0; i < alphaNumeric.length(); i++) {
      String iString = alphaNumeric.substring(i, i + 1);
      histogram.addValue(iString + iString + iString);
      histogram.addValue(iString + iString + iString);
    }

    assertEquals(36, histogram.getNumDistinct());

    StringDataType value = new StringDataType("joins", 5);
    StringHistogram copyHistogram = histogram.copyWithPredicate(PredicateOperator.LESS_THAN,
                                                                value);

    assertEquals(18, copyHistogram.getEntriesInRange("a", "9"));
    assertEquals(9, copyHistogram.getNumDistinct());
  }
}
