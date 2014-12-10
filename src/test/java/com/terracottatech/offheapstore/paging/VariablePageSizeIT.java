/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.paging;

import java.io.Serializable;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;

import com.terracottatech.offheapstore.AbstractConcurrentOffHeapMapIT;
import com.terracottatech.offheapstore.buffersource.OffHeapBufferSource;
import com.terracottatech.offheapstore.concurrent.ConcurrentOffHeapClockCache;
import com.terracottatech.offheapstore.storage.OffHeapBufferStorageEngine;
import com.terracottatech.offheapstore.storage.PointerSize;
import com.terracottatech.offheapstore.storage.portability.SerializablePortability;
import com.terracottatech.offheapstore.util.Factory;
import com.terracottatech.offheapstore.util.Generator.SpecialInteger;
import com.terracottatech.offheapstore.util.ParallelParameterized;
import com.terracottatech.offheapstore.util.PointerSizeParameterizedTest;

import java.util.Collection;
import org.junit.runner.RunWith;

/**
 *
 * @author cdennis
 */
@RunWith(ParallelParameterized.class)
public class VariablePageSizeIT extends AbstractConcurrentOffHeapMapIT {

  private final PointerSize pointerWidth;
  private final int initialPageSize;
  private final int maximalPageSize;
  
  @ParallelParameterized.Parameters
  public static Collection<Object[]> data() {
    return PointerSizeParameterizedTest.data();
  }
  
  public VariablePageSizeIT(PointerSize pointerWidth) {
    this.pointerWidth = pointerWidth;
    long seed = System.nanoTime();
    System.err.println("VariablePageSizeTest seed=" + seed);
    Random rndm = new Random(seed);
    int initialShift = rndm.nextInt(20);
    int maximalShift = initialShift + rndm.nextInt(20 - initialShift);
    this.initialPageSize = 1 << initialShift;
    this.maximalPageSize = 1 << maximalShift;
  }
  
  @Test
  public void testSerializablePortability() {
    Map<Serializable, Serializable> map = createSerializableMap(new UnlimitedPageSource(new OffHeapBufferSource()));
            
    for (int i = 0; i < 100; i++) {
      map.put(Integer.toString(i), Integer.toString(i));
      
      for (int j = 0; j <= i; j++) {
        Assert.assertEquals(Integer.toString(j), map.get(Integer.toString(j)));
      }
    }

    Assert.assertEquals(100, map.size());
    Assert.assertFalse(map.isEmpty());
    
    for (int i = 0; i < 100; i++) {
      Assert.assertEquals(Integer.toString(i), map.remove(Integer.toString(i)));
      
      for (int j = i + 1; j < 100; j++) {
        Assert.assertEquals(Integer.toString(j), map.get(Integer.toString(j)));
      }
    }
    
    Assert.assertTrue(map.isEmpty());
  }
  
  @Override
  protected ConcurrentMap<SpecialInteger, SpecialInteger> createGoodMap() {
    throw new AssumptionViolatedException("Not bothered with non-paging tests");
  }

  @Override
  protected ConcurrentMap<SpecialInteger, SpecialInteger> createBadMap() {
    throw new AssumptionViolatedException("Not bothered with non-paging tests");
  }

  @Override
  protected Map<Integer, byte[]> createOffHeapBufferMap(PageSource source) {
    return new ConcurrentOffHeapClockCache<Integer, byte[]>(source, createStorageEngineFactory(source));
  }
  
  protected Map<Serializable, Serializable> createSerializableMap(PageSource source) {
    return new ConcurrentOffHeapClockCache<Serializable, Serializable>(source, createStorageEngineFactory(source));
  }

  private Factory<OffHeapBufferStorageEngine<Serializable, Serializable>> createStorageEngineFactory(PageSource source) {
    return OffHeapBufferStorageEngine.createFactory(pointerWidth, source, initialPageSize, maximalPageSize, new SerializablePortability(), new SerializablePortability(), false, false);
  }
}
