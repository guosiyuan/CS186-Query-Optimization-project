package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.table.RecordID;
import edu.berkeley.cs186.database.datatypes.*;
import edu.berkeley.cs186.database.StudentTest;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runners.MethodSorters;
import org.junit.experimental.categories.Category;

import java.util.Iterator;
import java.util.Arrays;
import java.util.Random;
import static org.junit.Assert.*;

public class TestBPlusTree {
  public static final String testFile = "BPlusTreeTest";
  private BPlusTree bp;
  public static final int intLeafPageSize = 400;
  public static final int intInnPageSize = 496;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  
  @Rule
  public Timeout globalTimeout = Timeout.seconds(80); // 80 seconds max per method tested

  @Before
  public void beforeEach() throws Exception {
    tempFolder.newFile(testFile);
    String tempFolderPath = tempFolder.getRoot().getAbsolutePath();
    this.bp = new BPlusTree(new IntDataType(), testFile, tempFolderPath);
  }

  @Test
  public void testBPlusTreeInsert() {

    for (int i = 0; i < 10; i++) {
      bp.insertKey(new IntDataType(i), new RecordID(i,0));
    }
  
    Iterator<RecordID> rids = bp.sortedScan();
    int count = 0;
    while (rids.hasNext()) {
      assertEquals(count, rids.next().getPageNum());
      count++;
    }
  }
  @Test
  public void testBPlusTreeInsertBackwards() {
    for (int i = 9; i >= 0; i--) {
      bp.insertKey(new IntDataType(i), new RecordID(i,0));
    }
    Iterator<RecordID> rids = bp.sortedScan();
    int count = 0;
    while (rids.hasNext()) {
      assertEquals(count, rids.next().getPageNum());
      count++;
    }
  }
  
  @Test
  public void testBPlusTreeInsertIterateFrom() {
    for (int i = 16; i >= 0; i--) {
      bp.insertKey(new IntDataType(i), new RecordID(i,0));
    }
    Iterator<RecordID> rids = bp.sortedScanFrom(new IntDataType(10));
    int count = 10;
    while (rids.hasNext()) {
      assertEquals(count, rids.next().getPageNum());
      count++;
    }
    assertEquals(17, count);
  }
  
  @Test
  public void testBPlusTreeInsertIterateFromDuplicate() {
    for (int i = 10; i >= 0; i--) {
      for (int j = 0; j < 8; j++) {
        bp.insertKey(new IntDataType(i), new RecordID(i,j));
      }
    }
    Iterator<RecordID> rids = bp.sortedScanFrom(new IntDataType(5));
    int counter = 0;
    while (rids.hasNext()) {
      RecordID rid = rids.next();
      assertEquals(5 + counter/8, rid.getPageNum());
      assertEquals(counter % 8, rid.getSlotNumber());
      counter++;
    }
    assertEquals((5+1)*8, counter);
  }
  
  @Test
  public void testBPlusTreeInsertIterateLookup() {
    for (int i = 10; i >= 0; i--) {
      for (int j = 0; j < 8; j++) {
        bp.insertKey(new IntDataType(i), new RecordID(i,j));
      }
    }
    Iterator<RecordID> rids = bp.lookupKey(new IntDataType(5));
    int counter = 0;
    while (rids.hasNext()) {
      RecordID rid = rids.next();
      assertEquals(5, rid.getPageNum());
      assertEquals(counter, rid.getSlotNumber());
      counter++;
    }
    assertEquals(8, counter);
  }
  
  @Test
  public void testBPlusTreeInsertIterateFullLeafNode() {
    for (int i = 0; i < 400; i++) {
      bp.insertKey(new IntDataType(i), new RecordID(i,0));
    }
    Iterator<RecordID> rids = bp.sortedScan();
    int counter = 0;
    while (rids.hasNext()) {
      RecordID rid = rids.next();
      assertEquals(counter, rid.getPageNum());
      counter++;
    }
    assertEquals(400, counter);
  }
  
  @Test
  public void testBPlusTreeInsertIterateFullLeafSplit() {

    //Insert full leaf of records + 1
    for (int i = 0; i < intLeafPageSize + 1; i++) {
      bp.insertKey(new IntDataType(i), new RecordID(i,0));
    }

    Iterator<RecordID> rids = bp.sortedScan();
    assertTrue(rids.hasNext());
    int counter = 0;
    while (rids.hasNext()) {
      RecordID rid = rids.next();
      assertEquals(counter, rid.getPageNum());
      counter++;
    }
    assertEquals(intLeafPageSize + 1, counter);
  }
  
  @Test
  public void testBPlusTreeInsertAppendIterateMultipleFullLeafSplit() {

    //Insert 3 full leafs of records + 1 in append fashion
    for (int i = 0; i < 3*intLeafPageSize + 1; i++) {
      bp.insertKey(new IntDataType(i), new RecordID(i,0));
    }

    Iterator<RecordID> rids = bp.sortedScan();
    int counter = 0;
    while (rids.hasNext()) {
      RecordID rid = rids.next();
      assertEquals(counter, rid.getPageNum());
      counter++;
    }
    assertEquals(3*intLeafPageSize + 1, counter);
  }
  
  @Test
  public void testBPlusTreeSweepInsertSortedScanMultipleFullLeafSplit() {

    //Insert 3 full leafs of records + 1 in sweeping fashion
    for (int i = 0; i < 3*intLeafPageSize + 1; i++) {
      bp.insertKey(new IntDataType(i % 3), new RecordID(i % 3, i));
    }
    Iterator<RecordID> rids = bp.sortedScan();
    assertTrue(rids.hasNext());
    
    for (int i = 0; i < intLeafPageSize + 1; i++) {
      assertTrue(rids.hasNext());
      RecordID rid = rids.next();
      assertEquals(0, rid.getPageNum());
    }

    for (int i = 0; i < intLeafPageSize; i++) {
      assertTrue(rids.hasNext());
      RecordID rid = rids.next();
      assertEquals(1, rid.getPageNum());
    }
    
    for (int i = 0; i < intLeafPageSize; i++) {
      assertTrue(rids.hasNext());
      RecordID rid = rids.next();
      assertEquals(2, rid.getPageNum());
    }
    assertFalse(rids.hasNext());
  }
  
  @Test
  public void testBPlusTreeRandomInsertSortedScanLeafSplit() {
    Random rand = new Random(0); //const seed 
    for (int i = 0; i < 10*intLeafPageSize; i++) {
      int val = rand.nextInt();
      bp.insertKey(new IntDataType(val), new RecordID(val, 0));
    }
    Iterator<RecordID> rids = bp.sortedScan();
    assertTrue(rids.hasNext());
    int last = rids.next().getPageNum();
    for (int i = 0; i < 10*intLeafPageSize - 1; i++) {
      assertTrue(rids.hasNext());
      RecordID rid = rids.next();
      assertTrue(last + " not less than " + rid.getPageNum(), last <= rid.getPageNum());
      last = rid.getPageNum();
    }
    assertFalse(rids.hasNext());
  }
  
  @Test
  public void testBPlusTreeSweepInsertLookupKeyMultipleFullLeafSplit() {

    //Insert 4 full leafs of records in sweeping fashion
    for (int i = 0; i < 8*intLeafPageSize; i++) {
      bp.insertKey(new IntDataType(i % 4), new RecordID(i % 4, i));
    }
    Iterator<RecordID> rids = bp.lookupKey(new IntDataType(0));
    assertTrue(rids.hasNext());

    for (int i = 0; i < 2*intLeafPageSize; i++) {
      assertTrue("iteration " + i, rids.hasNext());
      RecordID rid = rids.next();
      assertEquals(0, rid.getPageNum());
    }
    assertFalse(rids.hasNext());
    
    rids = bp.lookupKey(new IntDataType(1));
    assertTrue(rids.hasNext());
    for (int i = 0; i < 2*intLeafPageSize; i++) {
      assertTrue(rids.hasNext());
      RecordID rid = rids.next();
      assertEquals(1, rid.getPageNum());
    }
    assertFalse(rids.hasNext());
    
    rids = bp.lookupKey(new IntDataType(2));
    assertTrue(rids.hasNext());
    
    for (int i = 0; i < 2*intLeafPageSize; i++) {
      assertTrue(rids.hasNext());
      RecordID rid = rids.next();
      assertEquals(2, rid.getPageNum());
    }
    assertFalse(rids.hasNext());
  
    rids = bp.lookupKey(new IntDataType(3));
    assertTrue(rids.hasNext());
    
    for (int i = 0; i < 2*intLeafPageSize; i++) {
      assertTrue(rids.hasNext());
      RecordID rid = rids.next();
      assertEquals(3, rid.getPageNum());
    }
    assertFalse(rids.hasNext());
  
  }
  
  @Test
  public void testBPlusTreeSweepInsertSortedScanLeafSplit() {

    //Insert 10 full leafs of records in sweeping fashion
    for (int i = 0; i < 10*intLeafPageSize; i++) {
      bp.insertKey(new IntDataType(i % 5), new RecordID(i % 5, i));
    }

    Iterator<RecordID> rids = bp.sortedScan();
    assertTrue(rids.hasNext());
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 2*intLeafPageSize; j++) {
        assertTrue(rids.hasNext());
        RecordID rid = rids.next();
        assertEquals(i, rid.getPageNum());
      }
    }
    assertFalse(rids.hasNext());
    
  }

  @Test
  public void testBPlusTreeSweepInsertSortedScanFromLeafSplit() {

    //Insert 10 full leafs of records in sweeping fashion
    for (int i = 0; i < 10*intLeafPageSize; i++) {
      bp.insertKey(new IntDataType(i % 5), new RecordID(i % 5, i));
    }
    for (int k = 0; k < 5; k++) {
      Iterator<RecordID> rids = bp.sortedScanFrom(new IntDataType(k));
      assertTrue(rids.hasNext());
      for (int i = k; i < 5; i++) {
        for (int j = 0; j < 2*intLeafPageSize; j++) {
          assertTrue(rids.hasNext());
          RecordID rid = rids.next();
          assertEquals(i, rid.getPageNum());
        }
      }
      assertFalse(rids.hasNext());
    } 
  }

  @Test
  public void testBPlusTreeAppendInsertSortedScanInnerSplit() {
    //insert enough for InnerNode Split
    for (int i = 0; i < (intInnPageSize/2 + 1)*(intLeafPageSize); i++) {
      bp.insertKey(new IntDataType(i), new RecordID(i, 0));
    }
    Iterator<RecordID> rids = bp.sortedScan();
    
    for (int i = 0; i < (intInnPageSize/2 + 1)*(intLeafPageSize); i++) {
      assertTrue(rids.hasNext());
      RecordID rid = rids.next();
      assertEquals(i, rid.getPageNum());
    }
    assertFalse(rids.hasNext());
    
  }
  
  @Test
  public void testBPlusTreeSweepInsertLookupInnerSplit() {
    //insert enough for InnerNode Split; numEntries + firstChild
    //each key should span 2 pages
    
    for (int i = 0; i < 2*intLeafPageSize; i++) {
      for (int k = 0; k < 250; k++) {
        bp.insertKey(new IntDataType(k), new RecordID(k, 0));
      }
    }

    for (int k = 0; k < 250; k++) {
      Iterator<RecordID> rids = bp.lookupKey(new IntDataType(k));
      for (int i = 0; i < 2*intLeafPageSize; i++) {
        assertTrue("Loop: " + k + " iteration " + i, rids.hasNext());
        RecordID rid = rids.next();
        assertEquals(k, rid.getPageNum());
      }
      assertFalse(rids.hasNext());
    }
  }
  @Test
  public void testBPlusTreeRandomInsertSortedScanInnerSplit() {
    //insert enough for InnerNode Split; numEntries + firstChild
    Random rand = new Random(0); //const seed 
    int innerNodeSplit = intInnPageSize;
    
    for (int i = 0; i < innerNodeSplit*intLeafPageSize; i++) {
      int val = rand.nextInt();
      bp.insertKey(new IntDataType(val), new RecordID(val, 0));
    }
    Iterator<RecordID> rids = bp.sortedScan();
    assertTrue(rids.hasNext());
    int last = rids.next().getPageNum();
    for (int i = 0; i < innerNodeSplit*intLeafPageSize - 1; i++) {
      assertTrue(rids.hasNext());
      RecordID rid = rids.next();
      assertTrue("iteration: " + i + " last: " + last + " curr: " + rid.getPageNum(), last <= rid.getPageNum());
      last = rid.getPageNum();
    }
    assertFalse(rids.hasNext());
  }
}

