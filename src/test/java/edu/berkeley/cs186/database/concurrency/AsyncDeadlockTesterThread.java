package edu.berkeley.cs186.database.concurrency;

/**
 * Used to keep track of if the thread threw an exception
 * and report it to the main thread so that Junit can
 * detect errors outside the main thread.
 */
class AsyncDeadlockTesterThread {
  private Thread thread;
  private volatile DeadlockException exc;

  public AsyncDeadlockTesterThread(final Runnable runnable, String threadName){
    thread = new Thread(new Runnable(){
      public void run(){
        try{
          runnable.run();
        }catch(DeadlockException e){
          exc = e;
        }
      }
    }, threadName);
  }

  public void start(){
    thread.start();
  }

  public void join(long millis) throws InterruptedException {
    thread.join(millis);
  }

  public boolean isAlive() {
    return thread.isAlive();
  }

  public void test() throws InterruptedException {
    thread.join(100);
    if (exc != null)
      throw exc;
  }
}
