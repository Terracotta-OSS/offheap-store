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

import org.terracotta.offheapstore.OffHeapHashMap;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Test;

import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.IntegerStorageEngine;
import org.terracotta.offheapstore.storage.OffHeapBufferHalfStorageEngine;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.SplitStorageEngine;
import org.terracotta.offheapstore.storage.StringStorageEngine;
import org.terracotta.offheapstore.storage.listener.RuntimeStorageEngineListener;
import org.terracotta.offheapstore.storage.portability.ByteArrayPortability;
import org.terracotta.offheapstore.util.Generator;
import static org.terracotta.offheapstore.util.Generator.BAD_GENERATOR;
import static org.terracotta.offheapstore.util.Generator.GOOD_GENERATOR;
import org.terracotta.offheapstore.util.Generator.SpecialInteger;
import org.terracotta.offheapstore.util.ParallelParameterized;
import java.util.Arrays;
import java.util.Collection;
import static org.hamcrest.core.Is.is;
import static org.junit.Assume.assumeThat;
import org.junit.runner.RunWith;

@RunWith(ParallelParameterized.class)
public class OffHeapHashMapIT extends AbstractOffHeapMapIT {

  @ParallelParameterized.Parameters(name = "generator={0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[] {GOOD_GENERATOR}, new Object[] {BAD_GENERATOR});
  }

  public OffHeapHashMapIT(Generator generator) {
    super(generator);
  }
  
  @Test
  public void testDoublePut() {
    Map<SpecialInteger, SpecialInteger> map = new OffHeapHashMap<SpecialInteger, SpecialInteger>(new UnlimitedPageSource(new OffHeapBufferSource()), generator.engine(), 1);
    map.put(generator.generate(1), generator.generate(1));
    Assert.assertEquals(1, map.size());
    map.put(generator.generate(2), generator.generate(2));
    Assert.assertEquals(2, map.size());
    map.remove(generator.generate(1));
    Assert.assertEquals(1, map.size());
    map.put(generator.generate(2), generator.generate(2));
    Assert.assertEquals(1, map.size());
  }

  @Test
  public void testTableAllocationOomeOutput() {
    try {
      new OffHeapHashMap<Integer, Integer>(new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 96, 96), new SplitStorageEngine<Integer, Integer>(new IntegerStorageEngine(), new IntegerStorageEngine()), 8);
    } catch (IllegalArgumentException e) {
      System.err.println(e);
    }
  }

  @Test
  public void testEncodingSet() {
    final Set<Long> encodings = new HashSet<Long>();
    PageSource pageSource = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 10485760, 8192);
    StringStorageEngine storageEngine = new StringStorageEngine(PointerSize.INT, pageSource, 64);
    storageEngine.registerListener(new RuntimeStorageEngineListener<String, String>() {
      @Override public void cleared() { }
      @Override public void copied(int hash, long oldEncoding, long newEncoding, int metadata) { }
      @Override public void freed(long encoding, int hash, ByteBuffer key, boolean removed) { encodings.remove(encoding); }
      @Override public void written(String key, String value, ByteBuffer binaryKey, ByteBuffer binaryValue, int hash, int metadata, long encoding) { encodings.add(encoding); };
    });

    OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(pageSource, storageEngine);
    Assert.assertEquals(0, map.encodingSet().size());

    for (int i = 0; i < 10; i++) {
      map.put(String.valueOf(i), "");
    }

    Assert.assertEquals("encodings="+encodings.toString() + ", mapEncodings=" + map.encodingSet(), new TreeSet<Long>(map.encodingSet()), new TreeSet<Long>(encodings));

    for (int i = 0; i < 5; i++) {
      map.remove(String.valueOf(i));
    }

    Assert.assertEquals("encodings="+encodings.toString() + ", mapEncodings=" + map.encodingSet(), new TreeSet<Long>(map.encodingSet()), new TreeSet<Long>(encodings));

    map.clear();

    Assert.assertEquals(0, map.encodingSet().size());

    try {
      map.encodingSet().contains(4);
      fail();
    } catch (UnsupportedOperationException uoe) {
      // expected for now. If this changes the above assertions can do plain equals() on the sets to compare
      // Also maybe worth adding some explicit contains() tests once it works
    }
  }

  @Test
  public void testTableResizeOomeOutput() {
    Map<Integer, Integer> map = new OffHeapHashMap<Integer, Integer>(new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 96, 96), new SplitStorageEngine<Integer, Integer>(new IntegerStorageEngine(), new IntegerStorageEngine()), 4);

    map.put(1, 1);
    map.put(2, 2);
    map.put(3, 3);
    map.put(4, 4);

    try {
      map.put(5, 5);
    } catch (OversizeMappingException e) {
      System.err.println(e);
    }
  }

  @Test
  public void testDataAllocationOomeOutput() {
    try {
      new OffHeapHashMap<String, String>(new UnlimitedPageSource(new OffHeapBufferSource()), new StringStorageEngine(PointerSize.INT, new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 256, 128, 32), 256), 4);
    } catch (IllegalArgumentException e) {
      System.err.println(e);
    }
  }

  @Test
  public void testDataExpansionOomeOutput() {
    Map<String, String> map = new OffHeapHashMap<String, String>(new UnlimitedPageSource(new OffHeapBufferSource()), new StringStorageEngine(PointerSize.INT, new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 256, 128, 32), 32), 4);

    for (int i = 0; i < 100; i++) {
      try {
        map.put(String.valueOf(i), "Hello");
      } catch (OversizeMappingException e) {
        System.err.println(e);
        break;
      }
    }
  }

  @Override
  protected Map<SpecialInteger, SpecialInteger> createMap(Generator generator) {
    return new OffHeapHashMap<SpecialInteger, SpecialInteger>(new UnlimitedPageSource(new OffHeapBufferSource()), generator.engine(), 1);
  }

  @Override
  protected Map<Integer, byte[]> createOffHeapBufferMap(PageSource source) {
    assumeThat(generator, is(GOOD_GENERATOR));
    return new OffHeapHashMap<Integer, byte[]>(source, new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 1024, ByteArrayPortability.INSTANCE)));
  }
}
