package edu.berkeley.cs186.database.concurrency;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The LockManager provides allows for table-level locking by keeping
 * track of which transactions own and are waiting for locks on specific tables.
 *
 * THIS CODE IS FOR PROJECT 3.
 */
public class LockManager {
  
  public enum LockType {SHARED, EXCLUSIVE};
  private ConcurrentHashMap<String, Lock> tableNameToLock;
  private WaitsForGraph waitsForGraph;
  
  public LockManager() {
    tableNameToLock = new ConcurrentHashMap<String, Lock>();
    waitsForGraph = new WaitsForGraph();
  }

  /**
   * Acquires a lock on tableName of type lockType for transaction transNum.
   *
   * @param tableName the database to lock on
   * @param transNum the transactions id
   * @param lockType the type of lock
   */
  public void acquireLock(String tableName, long transNum, LockType lockType) {
    if (!this.tableNameToLock.containsKey(tableName)) {
      this.tableNameToLock.put(tableName, new Lock(lockType));
    }
    Lock lock = this.tableNameToLock.get(tableName);

    handlePotentialDeadlock(lock, transNum, lockType);

    lock.acquire(transNum, lockType);

  }

  /**
   * Adds any nodes/edges caused the by the specified LockRequest to
   * this LockManager's WaitsForGraph
   * @param lock the lock on the table that the LockRequest is for
   * @param transNum the transNum of the lock request
   * @param lockType the lockType of the lcok request
   */
  private void handlePotentialDeadlock(Lock lock, long transNum, LockType lockType) {
    //TODO: Implement Me!!
    //pass 1, check if we need to add to graph
    boolean needToWait = false;
    if (lock.getType().equals(LockManager.LockType.EXCLUSIVE)) {//then add to wait graph
      if (!lock.getOwners().contains(transNum)) {
        needToWait = true;
      }
      //needToWait = true;
    } else if (lock.getType().equals(LockManager.LockType.SHARED) && lockType.equals(LockManager.LockType.EXCLUSIVE)) {//then need to wait
      if (!lock.getOwners().contains(transNum)) {//not a promotion
        needToWait = true;
      } else if (lock.getSize() != 1) {//many trans share a shared lock, also not a promotion
        needToWait = true;
      }
    }

    if (lock.isEmpty()) {//empty lock doesnt need to wait
      needToWait = false;
    }

    //pass 2, check deadlock
    if (needToWait) {
      waitsForGraph.addNode(transNum);
      for (Long destTransNum : lock.getOwners()) {
        waitsForGraph.addNode(destTransNum);
        if (transNum!= destTransNum) {//only distinct nodes makes sense
          if (!waitsForGraph.edgeCausesCycle(transNum, destTransNum)) {
            waitsForGraph.addEdge(transNum, destTransNum);
          } else {
            throw new DeadlockException("dead lock");
          }
        }
      }
    }
    return;
  }


  /**
   * Releases transNum's lock on tableName.
   *
   * @param tableName the table that was locked
   * @param transNum the transaction that held the lock
   */
  public void releaseLock(String tableName, long transNum) {
    if (this.tableNameToLock.containsKey(tableName)) {
      Lock lock = this.tableNameToLock.get(tableName);
      lock.release(transNum);
    }
  }

  /**
   * Returns a boolean indicating whether or not transNum holds a lock of type lt on tableName.
   *
   * @param tableName the table that we're checking
   * @param transNum the transaction that we're checking for
   * @param lockType the lock type
   * @return whether the lock is held or not
   */
  public boolean holdsLock(String tableName, long transNum, LockType lockType) {
    if (!this.tableNameToLock.containsKey(tableName)) {
      return false;
    }

    Lock lock = this.tableNameToLock.get(tableName);
    return lock.holds(transNum, lockType);
  }
}
