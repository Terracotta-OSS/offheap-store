/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.terracottatech.offheapstore;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.hamcrest.core.IsNull;

import org.junit.Assert;
import org.junit.Test;

import com.terracottatech.offheapstore.buffersource.HeapBufferSource;
import com.terracottatech.offheapstore.concurrent.AbstractConcurrentOffHeapMap;
import com.terracottatech.offheapstore.concurrent.ConcurrentMapInternals;
import com.terracottatech.offheapstore.paging.UnlimitedPageSource;
import com.terracottatech.offheapstore.storage.IntegerStorageEngine;
import com.terracottatech.offheapstore.storage.SplitStorageEngine;
import com.terracottatech.offheapstore.util.Factory;

import org.junit.Ignore;

/**
 *
 * @author cdennis
 */
public class PseudoEnormousCacheIT {

  /**
   * Ensure that we aren't throwing away hash bits and causing clumping in the
   * tables.
   */
  @Ignore @Test
  public void testPseudoEnormousCache() throws InterruptedException, ExecutionException {
    Map<Integer, Integer> map = new PseudoConcurrentMap(8192);
    for (long i = Integer.MIN_VALUE; i <= Integer.MAX_VALUE; i++) {
      Assert.assertThat(map.put((int)i, (int)i), IsNull.nullValue());
    }
    MapInternals stats = ((ConcurrentMapInternals) map).getSegmentInternals().get(0);
    Assert.assertEquals(32, stats.getReprobeLength());
  }


  static class PseudoConcurrentMap extends AbstractConcurrentOffHeapMap<Integer, Integer> {

    PseudoConcurrentMap(int concurrency) {
      super(new DementedSegmentFactory(), concurrency);
    }
  }

  static class DementedSegmentFactory implements Factory<Segment<Integer, Integer>> {

    private static final Segment<Integer, Integer> DEMENTED_SEGMENT = (Segment<Integer, Integer>) Proxy.newProxyInstance(Segment.class.getClassLoader(), new Class<?>[]{Segment.class}, new InvocationHandler() {
      @Override
      public Object invoke(Object o, Method method, Object[] os) throws Throwable {
        return null;
      }
    });
    private volatile boolean done;

    @Override
    public Segment<Integer, Integer> newInstance() {
      if (!done) {
        done = true;
        return new ReadWriteLockedOffHeapHashMap<Integer, Integer>(new UnlimitedPageSource(new HeapBufferSource()), new SplitStorageEngine<Integer, Integer>(new IntegerStorageEngine(), new IntegerStorageEngine()));
      } else {
        return DEMENTED_SEGMENT;
      }
    }
  }
}
