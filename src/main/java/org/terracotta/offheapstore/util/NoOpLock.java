/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
 * Copyright IBM Corp. 2024, 2025
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
package org.terracotta.offheapstore.util;

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
