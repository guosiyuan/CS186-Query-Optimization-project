package edu.berkeley.cs186.database.concurrency;
import java.util.Iterator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Each table will have a lock object associated with it in order
 * to implement table-level locking. The lock will keep track of its
 * transaction owners, type, and the waiting queue.
 */
public class Lock {


  private Set<Long> transactionOwners;
  private ConcurrentLinkedQueue<LockRequest> transactionQueue;
  private LockManager.LockType type;

  public Lock(LockManager.LockType type) {
    this.transactionOwners = new HashSet<Long>();
    this.transactionQueue = new ConcurrentLinkedQueue<LockRequest>();
    this.type = type;
  }

  protected Set<Long> getOwners() {
    return this.transactionOwners;
  }

  public LockManager.LockType getType() {
    return this.type;
  }

  private void setType(LockManager.LockType newType) {
    this.type = newType;
  }

  public int getSize() {
    return this.transactionOwners.size();
  }

  public boolean isEmpty() {
    return this.transactionOwners.isEmpty();
  }

  private boolean containsTransaction(long transNum) {
    return this.transactionOwners.contains(transNum);
  }

  private void addToQueue(long transNum, LockManager.LockType lockType) {
    LockRequest lockRequest = new LockRequest(transNum, lockType);
    this.transactionQueue.add(lockRequest);
  }

  private void removeFromQueue(long transNum, LockManager.LockType lockType) {
    LockRequest lockRequest = new LockRequest(transNum, lockType);
    this.transactionQueue.remove(lockRequest);
  }

  private void addOwner(long transNum) {
    this.transactionOwners.add(transNum);
  }

  private void removeOwner(long transNum) {
    this.transactionOwners.remove(transNum);
  }

  /**
   * Attempts to resolve the specified lockRequest. Adds the request to the queue
   * and calls wait() until the request can be promoted and removed from the queue.
   * It then modifies this lock's owners/type as necessary.
   * @param transNum transNum of the lock request
   * @param lockType lockType of the lock request
   */
  protected synchronized void acquire(long transNum, LockManager.LockType lockType) {
    //TODO: Implement Me!!
    //edge cases
    if (getType().equals(LockManager.LockType.EXCLUSIVE) && containsTransaction(transNum)) {
      return;
    }

    //check duplicate in this.transactionQueue
    LockRequest lockRe1 = new LockRequest(transNum, LockManager.LockType.EXCLUSIVE);
    LockRequest lockRe2 = new LockRequest(transNum, LockManager.LockType.SHARED);
    if (transactionQueue.contains(lockRe1) || transactionQueue.contains(lockRe2)) {//if in queue, do nothing
      return;
    }
    //main things
    addToQueue(transNum, lockType);
    while (! compatibleForPromotion(transNum, lockType)) {
      try {
        this.wait();
      } catch (InterruptedException e) {
        throw new DeadlockException("hhh");
      }
    }
    removeFromQueue(transNum, lockType);
    if (lockType.equals(LockManager.LockType.EXCLUSIVE)) {//exclusive, then empty all previous owners
      transactionOwners = new HashSet<Long>();
      addOwner(transNum);
      setType(lockType);
    } else {//is shared
      addOwner(transNum);
    }
    //this.notifyAll();
    //lockType.equals(LockManager.LockType.SHARED);
    return;
  }
  //************************HELPER BELOW
  protected boolean compatibleForPromotion(long transNum, LockManager.LockType lockType) {
    LockManager.LockType reqLt = lockType;//request lock type
    LockManager.LockType curLt = getType();//this lock's lock type
    long reqTransnum = transNum;

    //if empty and first then grant the lock
    if (isEmpty()) {//if empty then add it
      if (transactionQueue.peek().equals(new LockRequest(transNum, lockType))) {
        return true;
      }
    }

    //first pass
    boolean firstPass = false;
    if (getSize() == 1) {
      if (containsTransaction(transNum)) {//only one and matches
        firstPass = true;
      } else {//only one and not match
        if (reqLt == LockManager.LockType.SHARED && curLt == LockManager.LockType.SHARED) {//if both shared then can coexist, otherwise not
          firstPass = true;
        }
      }
    } else {//many, then can only require shared data
      if (reqLt == LockManager.LockType.SHARED && curLt == LockManager.LockType.SHARED) {//many and contains one
        firstPass = true;
      }

    }
    if (! firstPass) {
      return false;
    }

    //second pass
    //case 1
    if (curLt.equals(LockManager.LockType.SHARED) && reqLt.equals(LockManager.LockType.EXCLUSIVE)) {
      if (getSize() == 1 && containsTransaction(transNum)) {
        return true;
      }
    }
    //case 2
    if (curLt.equals(LockManager.LockType.SHARED) && reqLt.equals(LockManager.LockType.SHARED)) {
      if (transactionQueue.peek().equals(new LockRequest(transNum, lockType))) {
        return true;
      }
    }
    //case 3
    Iterator<LockRequest> transIter= transactionQueue.iterator();
    boolean allShared = true;
    while (transIter.hasNext()) {
      LockRequest lockRec = transIter.next();
      if (lockRec.transNum ==transNum) {
        break;
      } else {
        if (lockRec.lockType == LockManager.LockType.EXCLUSIVE) {
          allShared = false;
        }
      }
    }
    if (allShared == true) {//all previous are shared
      return true;
    }
    return false;
  }
  //************************HELPER ABOVE

  /**
   * transNum releases ownership of this lock
   * @param transNum transNum of transaction that is releasing ownership of this lock
   */
  protected synchronized void release(long transNum) {
    //TODO: Implement Me!!
    removeOwner(transNum);
    if (isEmpty()) {
      setType(LockManager.LockType.SHARED);
    }
    this.notifyAll();
    return;
  }

  /**
   * Checks if the specified transNum holds a lock of lockType on this lock object
   * @param transNum transNum of lock request
   * @param lockType lock type of lock request
   * @return true if transNum holds the lock of type lockType
   */
  protected synchronized boolean holds(long transNum, LockManager.LockType lockType) {
    //TODO: Implement Me!!
    if (containsTransaction(transNum) && lockType.equals(getType())) {
      return true;
    }
    return false;
  }

  /**
   * LockRequest objects keeps track of the transNum and lockType.
   * Two LockRequests are equal if they have the same transNum and lockType.
   */
  private class LockRequest {
      private long transNum;
      private LockManager.LockType lockType;
      private LockRequest(long transNum, LockManager.LockType lockType) {
        this.transNum = transNum;
        this.lockType = lockType;
      }

      @Override
      public int hashCode() {
        return (int) transNum;
      }

      @Override
      public boolean equals(Object obj) {
        if (!(obj instanceof LockRequest))
          return false;
        if (obj == this)
          return true;

        LockRequest rhs = (LockRequest) obj;
        return (this.transNum == rhs.transNum) && (this.lockType == rhs.lockType);
      }

  }

}
