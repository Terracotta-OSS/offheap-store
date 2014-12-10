package com.terracottatech.offheapstore.buffersource;

import com.terracottatech.offheapstore.util.FindbugsSuppressWarnings;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimingBufferSource implements BufferSource {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(TimingBufferSource.class);

  private final BufferSource delegate;
  private final long slowNanos;
  private final long criticalNanos;
  private final boolean haltOnCritical;
  
  public TimingBufferSource(BufferSource source, long slow, TimeUnit slowUnit, long critical, TimeUnit criticalUnit, boolean haltOnCritical) {
    this.delegate = source;
    this.slowNanos = slowUnit.toNanos(slow);
    this.criticalNanos = criticalUnit.toNanos(critical);
    this.haltOnCritical = haltOnCritical;
  }
  
  @Override
  public ByteBuffer allocateBuffer(int size) {
    long beforeAllocationTime = System.nanoTime();
    try {
      return delegate.allocateBuffer(size);
    } finally {
      long allocationDelay = System.nanoTime() - beforeAllocationTime;
      if (allocationDelay >= criticalNanos) {
        if (haltOnCritical) {
            LOGGER.error("Off heap memory allocation is way too slow - attempting to halt VM to prevent swap depletion." +
                    " Please review your -XX:MaxDirectMemorySize and make sure the OS has sufficient resources.");
            commitSuicide("attempted VM halt");
        } else {
            LOGGER.error("Off heap memory allocation is way too slow." +
                    " Please review your -XX:MaxDirectMemorySize and make sure the OS has sufficient resources.");
        }
      } else if (allocationDelay > slowNanos) {
          LOGGER.warn("Off heap memory allocation is too slow - is the OS swapping?");
      }
    }
  }

  /**
   * Try to terminate the VM as soon as possible no matter the method and how abrupt it may be.
   * @param msg an error message which may end up being reported.
   */
  private static void commitSuicide(String msg) {
    Thread t = new Thread() {
      @Override
      @FindbugsSuppressWarnings("DM_EXIT")
      public void run() {
        // halt the JVM immediately unless the security manager prevents this
        try {
          System.exit(-1);
        } catch (SecurityException ex) {
          LOGGER.info("SecurityException prevented system exit", ex);
        }
        // log an error stating that the JVM should be manually aborted
        try {
          while (true) {
            Thread.sleep(5000L);
            LOGGER.error("VM is in an unreliable state - please abort it!");
          }
        } catch (InterruptedException e) {
          LOGGER.info("JVM Instability logger terminated by interrupt");
        }
      }
    };
    t.setDaemon(true);
    // spawn a thread here to avoid provoking deadlocks
    t.start();

    throw new Error(msg);
  }

}
