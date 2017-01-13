package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

//additional inport from TestLockManager
import edu.berkeley.cs186.database.*;
import edu.berkeley.cs186.database.table.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.lang.Runnable;


public class TestDeadlockPrevention {
  private static final String TestDir = "testDatabase";
  private Database db;
  private String filename;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public Timeout maxGlobalTimeout = Timeout.seconds(10); // 10 seconds max per method tested


  @Before
  public void beforeEach() throws IOException, DatabaseException {
    File testDir = tempFolder.newFolder(TestDir);
    this.filename = testDir.getAbsolutePath();
    this.db = new Database(filename);
    this.db.deleteAllTables();
  }

  @After
  public void afterEach() {
    this.db.deleteAllTables();
    this.db.close();
  }

  @Test
  public void testNoCycleDeadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Second Thread");

    AsyncDeadlockTesterThread thread4 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Thread");

    try {
      thread1.start();
      thread1.join(100); //waits for thread to finish (timeout of .1 sec)
      thread1.test();
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

      thread2.start();
      thread2.join(100); //waits for thread to finish (timeout of .1 sec)
      thread2.test();
      assertTrue("Transaction 2 Thread should not have finished", thread2.isAlive()); //T2 should be waiting on T1 for A

      thread3.start();
      thread3.join(100); //waits for thread to finish (timeout of .1 sec)
      thread3.test();
      assertFalse("Transaction 2 Second Thread should have finished", thread3.isAlive()); //T2 should not be blocked on B

      thread4.start();
      thread4.join(100);
      thread4.test();
      assertTrue("Transaction 3 Thread should not have finished", thread4.isAlive()); //T3 should be blocked on B
    } catch (DeadlockException d) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }

  }

  @Test
  public void testNoDirectedCycleDeadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Thread");

    AsyncDeadlockTesterThread thread4 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Second Thread");

    AsyncDeadlockTesterThread thread5 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Second Thread");

    try {
      thread1.start();
      thread1.join(100); //waits for thread to finish (timeout of .1 sec)
      thread1.test();
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

      thread2.start();
      thread2.join(100); //waits for thread to finish (timeout of .1 sec)
      thread2.test();
      assertTrue("Transaction 2 Thread should not have finished", thread2.isAlive()); //T2 should be waiting on T1 for A

      thread3.start();
      thread3.join(100); //waits for thread to finish (timeout of .1 sec)
      thread3.test();
      assertTrue("Transaction 3 Thread should not have finished", thread3.isAlive()); //T3 should be waiting on T1 for A

      thread4.start();
      thread4.join(100);
      thread4.test();
      assertFalse("Transaction 2 Second Thread should have finished", thread4.isAlive()); //T2 should not be blocked on B

      thread5.start();
      thread5.join(100);
      thread5.test();
      assertTrue("Transaction 3 Second Thread should not have finished", thread5.isAlive()); //T3 should be waiting on T2 for B
    } catch (DeadlockException d) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }

  }

  @Test
  public void testTwoTransactionCycleDeadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Second Thread");

    AsyncDeadlockTesterThread thread4 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Second Thread");

    try {
      thread1.start();
      thread1.join(100); //waits for thread to finish (timeout of .1 sec)
      thread1.test();
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

      thread2.start();
      thread2.join(100); //waits for thread to finish (timeout of .1 sec)
      thread2.test();
      assertTrue("Transaction 2 Thread should not have finished", thread2.isAlive()); //T2 should be waiting on T1 for A

      thread3.start();
      thread3.join(100); //waits for thread to finish (timeout of .1 sec)
      thread3.test();
      assertFalse("Transaction 2 Second Thread should have finished", thread3.isAlive()); //T2 should not be blocked on B
    } catch (DeadlockException d) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }

    try {
      thread4.start();
      thread4.join(100);
      thread4.test();
      fail("Deadlock Exception not thrown.");
    } catch (DeadlockException d) {

    }

  }

  @Test
  public void testThreeTransactionCycleDeadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Second Thread");

    AsyncDeadlockTesterThread thread4 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Thread");

    AsyncDeadlockTesterThread thread5 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("C", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Second Thread");

    AsyncDeadlockTesterThread thread6 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("C", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Second Thread");

    try {
      thread1.start();
      thread1.join(100); //waits for thread to finish (timeout of .1 sec)
      thread1.test();
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

      thread2.start();
      thread2.join(100); //waits for thread to finish (timeout of .1 sec)
      thread2.test();
      assertTrue("Transaction 2 Thread should not have finished", thread2.isAlive()); //T2 should be waiting on T1 for A

      thread3.start();
      thread3.join(100); //waits for thread to finish (timeout of .1 sec)
      thread3.test();
      assertFalse("Transaction 2 Second Thread should have finished", thread3.isAlive()); //T2 should not be blocked on B

      thread4.start();
      thread4.join(100);
      thread4.test();
      assertTrue("Transaction 3 Thread should not have finished", thread4.isAlive()); //T3 should be blocked on B

      thread5.start();
      thread5.join(100);
      thread5.test();
      assertFalse("Transaction 3 Second Thread should have finished", thread5.isAlive()); //T3 should not be blocked on C
    } catch (DeadlockException d) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }

    try {
      thread6.start();
      thread6.join(100);
      thread6.test();
      fail("Deadlock Exception not thrown.");
    } catch (DeadlockException d) {

    }

  }

  @Test
  public void testThreeTransactionCycleDeadlock2() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Thread");

    AsyncDeadlockTesterThread thread4 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Second Thread");

    AsyncDeadlockTesterThread thread5 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("C", 1, LockManager.LockType.SHARED);
      }
    }, "Transaction 1 Third Thread");

    AsyncDeadlockTesterThread thread6 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("C", 2, LockManager.LockType.SHARED);
      }
    }, "Transaction 2 Second Thread");

    AsyncDeadlockTesterThread thread7 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("C", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Second Thread");

    try {
      thread1.start();
      thread1.join(100); //waits for thread to finish (timeout of .1 sec)
      thread1.test();
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked on A

      thread2.start();
      thread2.join(100); //waits for thread to finish (timeout of .1 sec)
      thread2.test();
      assertTrue("Transaction 2 Thread should not have finished", thread2.isAlive()); //T2 should be waiting on T1 for A

      thread3.start();
      thread3.join(100); //waits for thread to finish (timeout of .1 sec)
      thread3.test();
      assertFalse("Transaction 3 Thread should have finished", thread3.isAlive()); //T3 should not be blocked on B

      thread4.start();
      thread4.join(100); //waits for thread to finish (timeout of .1 sec)
      thread4.test();
      assertTrue("Transaction 1 Second Thread should not have finished", thread4.isAlive()); //T1 should be waiting on T3 for B

      thread5.start();
      thread5.join(100); //waits for thread to finish (timeout of .1 sec)
      thread5.test();
      assertFalse("Transaction 1 Third Thread should have finished", thread5.isAlive()); //T1 should not be blocked on C

      thread6.start();
      thread6.join(100); //waits for thread to finish (timeout of .1 sec)
      thread6.test();
      assertFalse("Transaction 2 Second Thread should have finished", thread6.isAlive()); //T2 should not be blocked on C
    } catch (DeadlockException d) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }

    try {
      thread7.start();
      thread7.join(100); //waits for thread to finish (timeout of .1 sec)
      thread7.test();
      fail("Deadlock Exception not thrown.");
    } catch (DeadlockException d) {

    }

  }

  @Test
  public void testNoSelfLoopsCycleDeadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.SHARED);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.SHARED);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Second Thread");

    try {
      thread1.start();
      thread1.join(100); //waits for thread to finish (timeout of .1 sec)
      thread1.test();
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

      thread2.start();
      thread2.join(100); //waits for thread to finish (timeout of .1 sec)
      thread2.test();
      assertFalse("Transaction 2 Thread should have finished", thread2.isAlive()); //T2 should not be blocked

      thread3.start();
      thread3.join(100); //waits for thread to finish (timeout of .1 sec)
      thread3.test();
      assertTrue("Transaction 2 Second Thread should not have finished", thread3.isAlive()); //T2 should be blocked
    } catch (DeadlockException d) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }

  }

  //****************** student tests below ************************** student tests below *****************************************
  //test 6
  @Test
  @Category(StudentTestP3.class)
  public void testNoSelfLoopsCycle1Deadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.SHARED);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.SHARED);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.SHARED);
      }
    }, "Transaction 2 Second Thread");

    try {
      thread1.start();
      thread1.join(100); //waits for thread to finish (timeout of .1 sec)
      thread1.test();
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

      thread2.start();
      thread2.join(100); //waits for thread to finish (timeout of .1 sec)
      thread2.test();
      assertFalse("Transaction 2 Thread should have finished", thread2.isAlive()); //T2 should not be blocked

      thread3.start();
      thread3.join(100); //waits for thread to finish (timeout of .1 sec)
      thread3.test();
      assertFalse("Transaction 2 Second Thread should have finished", thread3.isAlive()); //T2 should be blocked
    } catch (DeadlockException d) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }

  }
  //test 7
  @Test
  @Category(StudentTestP3.class)
  public void testNoSelfLoopsCycle2Deadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.SHARED);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.SHARED);
      }
    }, "Transaction 2 Second Thread");

    try {
      thread1.start();
      thread1.join(100); //waits for thread to finish (timeout of .1 sec)
      thread1.test();
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

      thread2.start();
      thread2.join(100); //waits for thread to finish (timeout of .1 sec)
      thread2.test();
      assertTrue("Transaction 2 Thread should not have finished", thread2.isAlive()); //T2 should not be blocked

      thread3.start();
      thread3.join(100); //waits for thread to finish (timeout of .1 sec)
      thread3.test();
      assertFalse("Transaction 2 Second Thread should have finished", thread3.isAlive()); //T2 should be blocked
    } catch (DeadlockException d) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }

  }

  //test 8
  @Test
  @Category(StudentTestP3.class)
  public void testNoSelfLoopsCycle3Deadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.SHARED);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.SHARED);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.SHARED);
      }
    }, "Transaction 1 Second Thread");

    try {
      thread1.start();
      thread1.join(100); //waits for thread to finish (timeout of .1 sec)
      thread1.test();
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

      thread2.start();
      thread2.join(100); //waits for thread to finish (timeout of .1 sec)
      thread2.test();
      assertFalse("Transaction 2 Thread should have finished", thread2.isAlive()); //T2 should not be blocked

      thread3.start();
      thread3.join(100); //waits for thread to finish (timeout of .1 sec)
      thread3.test();
      assertFalse("Transaction 1 Second Thread should have finished", thread3.isAlive()); //T2 should be blocked
    } catch (DeadlockException d) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }

  }

  //test 9
  @Test
  @Category(StudentTestP3.class)
  public void testNoSelfLoopsCycle4Deadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.SHARED);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.SHARED);
      }
    }, "Transaction 2 Thread");

    try {
      thread1.start();
      thread1.join(100); //waits for thread to finish (timeout of .1 sec)
      thread1.test();
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

      thread2.start();
      thread2.join(100); //waits for thread to finish (timeout of .1 sec)
      thread2.test();
      assertFalse("Transaction 2 Thread should have finished", thread2.isAlive()); //T2 should not be blocked

    } catch (DeadlockException d) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }

  }

  //test 10
  @Test
  @Category(StudentTestP3.class)
  public void testNoSelfLoopsCycle5Deadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.SHARED);
      }
    }, "Transaction 1 Thread");

    try {
      thread1.start();
      thread1.join(100); //waits for thread to finish (timeout of .1 sec)
      thread1.test();
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

    } catch (DeadlockException d) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }

  }



}
