package com.terracottatech.offheapstore;

import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Test;

import com.terracottatech.offheapstore.buffersource.OffHeapBufferSource;
import com.terracottatech.offheapstore.exceptions.OversizeMappingException;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.paging.UnlimitedPageSource;
import com.terracottatech.offheapstore.paging.UpfrontAllocatingPageSource;
import com.terracottatech.offheapstore.storage.IntegerStorageEngine;
import com.terracottatech.offheapstore.storage.OffHeapBufferHalfStorageEngine;
import com.terracottatech.offheapstore.storage.PointerSize;
import com.terracottatech.offheapstore.storage.SplitStorageEngine;
import com.terracottatech.offheapstore.storage.StringStorageEngine;
import com.terracottatech.offheapstore.storage.listener.RuntimeStorageEngineListener;
import com.terracottatech.offheapstore.storage.portability.ByteArrayPortability;
import com.terracottatech.offheapstore.util.Generator;
import com.terracottatech.offheapstore.util.Generator.SpecialInteger;

public class OffHeapHashMapIT extends AbstractOffHeapMapIT {

  @Test
  public void testDoublePut() {
    Map<SpecialInteger, SpecialInteger> map = new OffHeapHashMap<SpecialInteger, SpecialInteger>(new UnlimitedPageSource(new OffHeapBufferSource()), Generator.BAD_ENGINE, 1);
    map.put(Generator.BAD_GENERATOR.generate(1), Generator.BAD_GENERATOR.generate(1));
    Assert.assertEquals(1, map.size());
    map.put(Generator.BAD_GENERATOR.generate(2), Generator.BAD_GENERATOR.generate(2));
    Assert.assertEquals(2, map.size());
    map.remove(Generator.BAD_GENERATOR.generate(1));
    Assert.assertEquals(1, map.size());
    map.put(Generator.BAD_GENERATOR.generate(2), Generator.BAD_GENERATOR.generate(2));
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
  protected Map<SpecialInteger, SpecialInteger> createGoodMap() {
    return new OffHeapHashMap<SpecialInteger, SpecialInteger>(new UnlimitedPageSource(new OffHeapBufferSource()), Generator.GOOD_ENGINE, 1);
  }

  @Override
  protected Map<SpecialInteger, SpecialInteger> createBadMap() {
    return new OffHeapHashMap<SpecialInteger, SpecialInteger>(new UnlimitedPageSource(new OffHeapBufferSource()), Generator.BAD_ENGINE, 1);
  }

  @Override
  protected Map<Integer, byte[]> createOffHeapBufferMap(PageSource source) {
    return new OffHeapHashMap<Integer, byte[]>(source, new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 1024, ByteArrayPortability.INSTANCE)));
  }
}
