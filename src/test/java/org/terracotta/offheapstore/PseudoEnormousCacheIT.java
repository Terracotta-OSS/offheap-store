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
package org.terracotta.offheapstore;

import org.terracotta.offheapstore.ReadWriteLockedOffHeapHashMap;
import org.terracotta.offheapstore.MapInternals;
import org.terracotta.offheapstore.Segment;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.hamcrest.core.IsNull;

import org.junit.Assert;
import org.junit.Test;

import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.concurrent.AbstractConcurrentOffHeapMap;
import org.terracotta.offheapstore.concurrent.ConcurrentMapInternals;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.storage.IntegerStorageEngine;
import org.terracotta.offheapstore.storage.SplitStorageEngine;
import org.terracotta.offheapstore.util.Factory;

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
        return new ReadWriteLockedOffHeapHashMap<>(new UnlimitedPageSource(new HeapBufferSource()), new SplitStorageEngine<>(new IntegerStorageEngine(), new IntegerStorageEngine()));
      } else {
        return DEMENTED_SEGMENT;
      }
    }
  }
}
