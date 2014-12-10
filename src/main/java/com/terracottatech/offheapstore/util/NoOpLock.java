/*
 * All content copyright (c) 2011 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 *
 * @author cdennis
 */
public final class NoOpLock implements Lock {
  
  public static final NoOpLock INSTANCE = new NoOpLock();
  
  private NoOpLock() {
    //not instantiable
  }

  @Override
  public void lock() {
    //no-op
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
  }

  @Override
  public boolean tryLock() {
    return true;
  }

  @Override
  public boolean tryLock(long l, TimeUnit tu) throws InterruptedException {
    if (Thread.interrupted()) {
      throw new InterruptedException();
    } else {
      return true;
    }
  }

  @Override
  public void unlock() {
    //no-op
  }

  @Override
  public Condition newCondition() {
    throw new UnsupportedOperationException();
  }
}
