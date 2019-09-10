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
package org.terracotta.offheapstore.paging;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Assume;
import org.junit.Test;

import org.terracotta.offheapstore.WriteLockedOffHeapClockCache;
import org.terracotta.offheapstore.storage.StorageEngine;
import org.terracotta.offheapstore.storage.portability.StringPortability;
import org.terracotta.offheapstore.util.OffHeapAndDiskStorageEngineDependentTest;

import static org.hamcrest.core.IsNull.notNullValue;
import static org.terracotta.offheapstore.util.MemoryUnit.MEGABYTES;


/**
 *
 * @author Chris Dennis
 */
public class CrossedStealingDeadlockIT extends OffHeapAndDiskStorageEngineDependentTest {

  private static final Method DEADLOCKED_THREADS_METHOD;
  static {
    Method method;
    try {
      method = ThreadMXBean.class.getMethod("findDeadlockedThreads");
    } catch (Throwable t) {
      method = null;
    }
    DEADLOCKED_THREADS_METHOD =  method;
  }

  public CrossedStealingDeadlockIT(TestMode mode) {
    super(mode);
  }

  @Test
  public void testRecursiveTableShrink() throws InterruptedException {
    Assume.assumeThat(DEADLOCKED_THREADS_METHOD, notNullValue());

    PageSource source = createPageSource(4, MEGABYTES);

    Collection<Thread> threads = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      threads.add(new Thread(new Loader(createSingleStripedCache(source), 5, TimeUnit.SECONDS)));
    }


    for (Thread t : threads) {
      t.start();
    }

    new DeadlockDetector(threads).run();

    for (Thread t : threads) {
      t.join();
    }
  }

  private Map<String, String> createSingleStripedCache(PageSource source) {
    StorageEngine<String, String> storageEngine = create(source, StringPortability.INSTANCE, StringPortability.INSTANCE, true, true);
    return new WriteLockedOffHeapClockCache<>(source, true, storageEngine);
  }

  static class Loader implements Runnable {

    private final Map<String, String> cache;
    private final long duration;

    Loader(Map<String, String> cache, long duration, TimeUnit unit) {
      this.cache = cache;
      this.duration = unit.toNanos(duration);
    }

    @Override
    public void run() {
      Random rndm = new Random();
      long end = System.nanoTime() + duration;
      do {
        byte[] data = new byte[16];
        rndm.nextBytes(data);
        UUID uuid = UUID.nameUUIDFromBytes(data);
        cache.put(uuid.toString(), uuid.toString());
      } while (System.nanoTime() < end);
    }
  }


  static class DeadlockDetector implements Runnable {

    private final Collection<Thread> interest;

    public DeadlockDetector(Collection<Thread> interest) {
      this.interest = new ArrayList<>(interest);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void run() {
      ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

      while (interestIsAlive()) {
        long[] deadlocked;
        try {
          deadlocked = (long[]) DEADLOCKED_THREADS_METHOD.invoke(threadBean);
        } catch (NoSuchMethodError e) {
          Assume.assumeNoException(e);
          throw e;
        } catch (Throwable t) {
          throw new AssertionError(t);
        }
        if (deadlocked == null) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ex) {
            //ignore
          }
        } else {
          ThreadInfo[] threads = threadBean.getThreadInfo(deadlocked, Integer.MAX_VALUE);
          synchronized (System.err) {
            System.err.println("Deadlocked Threads:");
            for (ThreadInfo tInfo : threads) {
              System.err.println(dumpThread(tInfo));
            }
          }
          for (Thread t : interest) {
            t.stop();
          }
          throw new AssertionError("Deadlock was detected");
        }
      }
    }

    private boolean interestIsAlive() {
      for (Thread t : interest) {
        if (t.isAlive()) {
          return true;
        }
      }
      return false;
    }

  }

  private static String dumpThread(ThreadInfo thread) {
    StringBuilder sb = new StringBuilder("\"" + thread.getThreadName() + "\"" +
                                         " Id=" + thread.getThreadId() + " " +
                                         thread.getThreadState());
    if (thread.getLockName() != null) {
        sb.append(" on " + thread.getLockName());
    }
    if (thread.getLockOwnerName() != null) {
        sb.append(" owned by \"" + thread.getLockOwnerName() +
                  "\" Id=" + thread.getLockOwnerId());
    }
    if (thread.isSuspended()) {
        sb.append(" (suspended)");
    }
    if (thread.isInNative()) {
        sb.append(" (in native)");
    }
    sb.append('\n');

    StackTraceElement[] trace = thread.getStackTrace();
    for (StackTraceElement ste : trace) {
      sb.append("\tat " + ste.toString());
      sb.append('\n');
    }
    sb.append('\n');
    return sb.toString();
  }
}
