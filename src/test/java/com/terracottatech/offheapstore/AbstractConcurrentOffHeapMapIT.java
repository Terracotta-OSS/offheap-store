/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import com.terracottatech.offheapstore.buffersource.HeapBufferSource;
import com.terracottatech.offheapstore.paging.UnlimitedPageSource;
import com.terracottatech.offheapstore.util.Generator;
import com.terracottatech.offheapstore.util.Generator.SpecialInteger;

import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author cdennis
 */
public abstract class AbstractConcurrentOffHeapMapIT extends AbstractOffHeapMapIT {

  @Override
  protected abstract ConcurrentMap<SpecialInteger, SpecialInteger> createGoodMap();

  @Override
  protected abstract ConcurrentMap<SpecialInteger, SpecialInteger> createBadMap();

  @Test
  public final void testConcurrentMapGood() {
    MapTestRoutines.testConcurrentMap(Generator.GOOD_GENERATOR, createGoodMap());
  }

  @Test
  public final void testConcurrentMapBad() {
    MapTestRoutines.testConcurrentMap(Generator.BAD_GENERATOR, createBadMap());
  }

  @Test
  public final void testSubCollectionsGood() throws InterruptedException {
    MapTestRoutines.testSubCollections(Generator.GOOD_GENERATOR, createGoodMap());
  }

  @Test
  public final void testSubCollectionsBad() throws InterruptedException {
    MapTestRoutines.testSubCollections(Generator.BAD_GENERATOR, createBadMap());
  }

  @Test
  public final void testConcurrentSubCollectionsGood() throws InterruptedException {
    MapTestRoutines.testConcurrentSubCollections(Generator.GOOD_GENERATOR, createGoodMap());
  }

  @Test
  public final void testConcurrentSubCollectionsBad() throws InterruptedException {
    MapTestRoutines.testConcurrentSubCollections(Generator.BAD_GENERATOR, createBadMap());
  }

  @Test
  public final void testConcurrentRemovalGood() {
    MapTestRoutines.testConcurrentRemoval(Generator.GOOD_GENERATOR, createGoodMap());
  }

  @Test
  public final void testConcurrentRemovalBad() {
    MapTestRoutines.testConcurrentRemoval(Generator.BAD_GENERATOR, createBadMap());
  }

  @Test
  public final void testConcurrentAdditionGood() {
    MapTestRoutines.testConcurrentAddition(Generator.GOOD_GENERATOR, createGoodMap());
  }

  @Test
  public final void testConcurrentAdditionBad() {
    MapTestRoutines.testConcurrentAddition(Generator.BAD_GENERATOR, createBadMap());
  }
  
  @Test
  public final void testConcurrentIterationAndMutation() {
    MapTestRoutines.testConcurrentIterationAndMutation(Generator.GOOD_GENERATOR, createGoodMap());
  }

  @Test
  public final void testConcurrentIterationAndMutationWithHalfOffHeapStorageEngine() {
    final Map<Integer, byte[]> m = createOffHeapBufferMap(new UnlimitedPageSource(new HeapBufferSource()));

    final AtomicReference<Throwable> mutatorExceptionRef = new AtomicReference<Throwable>();
    final AtomicBoolean stopped = new AtomicBoolean(false);
    Thread mutator = new Thread() {

      @Override
      public void run() {
        try {
          Random rndm = new Random();
          while (!stopped.get()) {
            Integer v = rndm.nextInt(8192);
            if (rndm.nextBoolean()) {
              m.remove(v);
            } else {
              byte[] value = new byte[v >>> 3];
              for (int i = 0; i < value.length; i++) {
                value[i] = (byte) (i % 10);
              }
              m.put(v, value);
            }
          }
        } catch (Throwable t) {
          mutatorExceptionRef.set(t);
        }
      }
    };
    
    mutator.start();
    
    boolean interrupted = false;
    try {
      for (int i = 0; i < 10; i++) {
        int checked = 0;
        for (Map.Entry<Integer, byte[]> e : m.entrySet()) {
          Integer key = e.getKey();
          byte[] value = e.getValue();
          Assert.assertEquals(key.intValue() >>> 3, value.length);
          for (int j = 0; j < value.length; j++) {
            Assert.assertEquals(j % 10, value[j]);
          }
          checked++;
        }
        if (checked == 0) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            interrupted = true;
          }
        } else {
          Thread.yield();
        }
      }
    } finally {
      stopped.set(true);
      while (mutator.isAlive()) {
        try {
          mutator.join();
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
    
    Throwable mutatorException = mutatorExceptionRef.get();
    if (mutatorException != null) {
      throw new AssertionError(mutatorException);
    }
  }

  @Test
  public final void testConcurrentIterationMutationAndClearingWithHalfOffHeapStorageEngine() {
    final Map<Integer, byte[]> m = createOffHeapBufferMap(new UnlimitedPageSource(new HeapBufferSource()));

    final AtomicReference<Throwable> mutatorExceptionRef = new AtomicReference<Throwable>();
    final AtomicBoolean stopped = new AtomicBoolean(false);
    Thread mutator = new Thread() {

      @Override
      public void run() {
        try {
          Random rndm = new Random();
          while (!stopped.get()) {
            Integer v = rndm.nextInt(8192);
            if (rndm.nextInt(100) == 0) {
              m.clear();
            } else if (rndm.nextBoolean()) {
              m.remove(v);
            } else {
              byte[] value = new byte[v >>> 3];
              for (int i = 0; i < value.length; i++) {
                value[i] = (byte) (i % 10);
              }
              m.put(v, value);
            }
          }
        } catch (Throwable t) {
          mutatorExceptionRef.set(t);
        }
      }
    };
    
    mutator.start();
    
    boolean interrupted = false;
    try {
      for (int i = 0; i < 10; i++) {
        int checked = 0;
        for (Map.Entry<Integer, byte[]> e : m.entrySet()) {
          Integer key = e.getKey();
          byte[] value = e.getValue();
          Assert.assertEquals(key.intValue() >>> 3, value.length);
          for (int j = 0; j < value.length; j++) {
            Assert.assertEquals(j % 10, value[j]);
          }
          checked++;
        }
        if (checked == 0) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            interrupted = true;
          }
        } else {
          Thread.yield();
        }
      }
    } finally {
      stopped.set(true);
      while (mutator.isAlive()) {
        try {
          mutator.join();
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
    
    Throwable mutatorException = mutatorExceptionRef.get();
    if (mutatorException != null) {
      throw new AssertionError(mutatorException);
    }
  }
}
