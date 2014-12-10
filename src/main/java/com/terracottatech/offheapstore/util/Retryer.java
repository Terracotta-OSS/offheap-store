/*
 * All content copyright (c) Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.util;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cdennis
 */
public class Retryer {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(Retryer.class);
  
  private final ScheduledThreadPoolExecutor executor;
  
  private final long minimumDelay;
  private final long maximumDelay;
  private final TimeUnit unit;
  
  public Retryer(long minDelay, long maxDelay, TimeUnit unit, ThreadFactory threadFactory) {
    if (unit == null) {
      throw new IllegalArgumentException("Time unit must be non-null");
    }
    if (minDelay <= 0) {
      throw new IllegalArgumentException("Minimum delay must be greater than zero");
    }
    if (maxDelay < minDelay) {
      throw new IllegalArgumentException("Maximum delay cannot be less than minimum delay");
    }
    if (threadFactory == null) {
      throw new IllegalArgumentException("Thread factory must be non-null");
    }
    
    this.minimumDelay = minDelay;
    this.maximumDelay = maxDelay;
    this.unit = unit;
    this.executor = new ScheduledThreadPoolExecutor(1, threadFactory);
  }
  
  public void completeAsynchronously(Runnable task) {
    scheduleTask(task, 0);
  }
  
  public void shutdownNow() {
    executor.shutdownNow();
  }
  
  private void scheduleTask(final Runnable task, final long delay) {
    if (executor.isShutdown()) {
      
    }
    executor.schedule(new Runnable() {
      @Override
      public void run() {
        try {
          task.run();
        } catch (Throwable t) {
          long nextDelay = nextDelay(delay);
          LOGGER.warn(task + " failed, retrying in " + nextDelay + " " + unit.toString().toLowerCase(), t);
          scheduleTask(task, nextDelay);
        }
      }
    }, delay, unit);
  }
  
  private long nextDelay(long delay) {
    if (delay < minimumDelay) {
      return minimumDelay;
    } else if (delay >= maximumDelay) {
      return maximumDelay;
    } else {
      return Math.min(delay * 2, maximumDelay);
    }
  }
}
