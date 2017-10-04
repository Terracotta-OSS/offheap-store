/*
 * Copyright 2015 Terracotta, Inc., a Software AG company.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.terracotta.offheapstore.buffersource;

import org.terracotta.offheapstore.util.FindbugsSuppressWarnings;
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
    Thread t = new Thread(() -> {
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
    });
    t.setDaemon(true);
    // spawn a thread here to avoid provoking deadlocks
    t.start();

    throw new Error(msg);
  }

}
