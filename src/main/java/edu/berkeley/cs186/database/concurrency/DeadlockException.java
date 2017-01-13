package edu.berkeley.cs186.database.concurrency;

/**
 * Should be thrown when a deadlock occurs (indicated by a cycle
 * in the WaitsForGraph of the LockManager)
 */
public class DeadlockException extends RuntimeException {
  private String message;

  public DeadlockException(String message) {
    this.message = message;
  }

  public DeadlockException(Exception e) {
    this.message = e.getClass().toString() + ": " + e.getMessage();
  }

  @Override
  public String getMessage() {
    return this.message;
  }
}
