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

import java.io.Serializable;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

import org.junit.Assert;
import org.junit.AssumptionViolatedException;
import org.junit.Test;

import org.terracotta.offheapstore.AbstractConcurrentOffHeapMapIT;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.concurrent.ConcurrentOffHeapClockCache;
import org.terracotta.offheapstore.storage.OffHeapBufferStorageEngine;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.portability.SerializablePortability;
import org.terracotta.offheapstore.util.Factory;
import org.terracotta.offheapstore.util.Generator;
import static org.terracotta.offheapstore.util.Generator.GOOD_GENERATOR;
import org.terracotta.offheapstore.util.Generator.SpecialInteger;
import org.terracotta.offheapstore.util.ParallelParameterized;
import org.terracotta.offheapstore.util.PointerSizeParameterizedTest;

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
    super(GOOD_GENERATOR);
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
  protected ConcurrentMap<SpecialInteger, SpecialInteger> createMap(Generator generator) {
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
