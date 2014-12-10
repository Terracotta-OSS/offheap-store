/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore;

import static com.terracottatech.offheapstore.util.MemoryUnit.KILOBYTES;
import static com.terracottatech.offheapstore.util.Generator.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.terracottatech.offheapstore.buffersource.HeapBufferSource;
import com.terracottatech.offheapstore.buffersource.OffHeapBufferSource;
import com.terracottatech.offheapstore.concurrent.AbstractConcurrentOffHeapMap;
import com.terracottatech.offheapstore.exceptions.OversizeMappingException;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.paging.UpfrontAllocatingPageSource;

import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentMap;

import org.hamcrest.core.Is;
import org.hamcrest.core.IsNull;
import org.hamcrest.core.StringContains;

/**
 *
 * @author Chris Dennis
 */
public abstract class AbstractOffHeapMapIT {

  protected abstract Map<SpecialInteger, SpecialInteger> createGoodMap();

  protected abstract Map<SpecialInteger, SpecialInteger> createBadMap();

  protected abstract Map<Integer, byte[]> createOffHeapBufferMap(PageSource source);

  @Test
  public final void testResizingGood() {
    MapTestRoutines.testResizing(GOOD_GENERATOR, createGoodMap());
  }

  @Test
  public final void testResizingBad() {
    MapTestRoutines.testResizing(BAD_GENERATOR, createBadMap());
  }

  @Test
  public final void testEmptyMap() {
    MapTestRoutines.testEmptyMapFunctionalInvariants(createGoodMap());
  }

  @Test
  public final void testPopulatedMapGood() {
    MapTestRoutines.testPopulatedMapFunctionalInvariants(GOOD_GENERATOR, createGoodMap());
  }

  @Test
  public final void testPopulatedMapBad() {
    MapTestRoutines.testPopulatedMapFunctionalInvariants(BAD_GENERATOR, createBadMap());
  }

  @Test
  public final void testTerminalEntryIteratorGood() {
    MapTestRoutines.testTerminalEntryIterator(GOOD_GENERATOR, createGoodMap());
  }

  @Test
  public final void testTerminalEntryIteratorBad() {
    MapTestRoutines.testTerminalEntryIterator(BAD_GENERATOR, createBadMap());
  }

  @Test
  public final void testTerminalEncodingIteratorGood() {
    MapTestRoutines.testTerminalEncodingIterator(GOOD_GENERATOR, createGoodMap());
  }

  @Test
  public final void testTerminalEncodingIteratorBad() {
    MapTestRoutines.testTerminalEncodingIterator(BAD_GENERATOR, createBadMap());
  }

  @Test
  public final void testTerminalValueIteratorGood() {
    MapTestRoutines.testTerminalValueIterator(GOOD_GENERATOR, createGoodMap());
  }

  @Test
  public final void testTerminalValueIteratorBad() {
    MapTestRoutines.testTerminalValueIterator(BAD_GENERATOR, createBadMap());
  }

  @Test
  public final void testTerminalKeyIteratorGood() {
    MapTestRoutines.testTerminalEntryIterator(GOOD_GENERATOR, createGoodMap());
  }

  @Test
  public final void testTerminalKeyIteratorBad() {
    MapTestRoutines.testTerminalEntryIterator(BAD_GENERATOR, createBadMap());
  }

  @Test
  public final void testGetBehaviorGood() {
    MapTestRoutines.testGetBehavior(GOOD_GENERATOR, createGoodMap());
  }

  @Test
  public final void testGetBehaviorBad() {
    MapTestRoutines.testGetBehavior(BAD_GENERATOR, createBadMap());
  }

  @Test
  public final void testDestroyFreesResources() {
    UpfrontAllocatingPageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 4 * 1024 * 1024, 4 * 1024 * 1024);

    Collection<Map<Integer, byte[]>> maps = new ArrayList<Map<Integer, byte[]>>();

    int count = 0;
    while (true) {
      try {
        Map<Integer, byte[]> m = createOffHeapBufferMap(source);
        maps.add(m);
        for (int i = 0; i < 100; i++) {
          m.put(i, new byte[1024]);
        }
        count++;
      } catch (OversizeMappingException e) {
        break;
      } catch (IllegalArgumentException e) {
        break;
      }
    }

    Assert.assertTrue(count != 0);

    for (Map<?, ?> m : maps) {
      if (m instanceof OffHeapHashMap<?, ?>) {
        ((OffHeapHashMap<?, ?>) m).destroy();
      } else if (m instanceof AbstractConcurrentOffHeapMap<?, ?>) {
        ((AbstractConcurrentOffHeapMap<?, ?>) m).destroy();
      } else if (m instanceof Segment<?, ?>) {
        ((Segment<?, ?>) m).destroy();
      } else {
        Assert.fail();
      }
    }

    Assert.assertEquals(0, source.getAllocatedSize());

    for (Map<Integer, byte[]> m : maps) {
      try {
        m.put(1, new byte[1024]);
        Assert.fail("Expected NullPointerException");
      } catch (IllegalStateException e) {
        Assert.assertThat(e.getMessage(), StringContains.containsString("destroyed"));
      }
    }

    for (int c = 0; c < count; c++) {
      Map<Integer, byte[]> m = createOffHeapBufferMap(source);
      for (int i = 0; i < 100; i++) {
        Assert.assertNull(m.put(i, new byte[1024]));
      }
    }
  }

  @Test
  public final void testDestroyedMapIsEmpty() {
    Map<SpecialInteger, SpecialInteger> m = createGoodMap();
    
    for (int i = 0; i < 100; i++) {
      m.put(GOOD_GENERATOR.generate(i), GOOD_GENERATOR.generate(i));
    }

    if (m instanceof OffHeapHashMap<?, ?>) {
      ((OffHeapHashMap<?, ?>) m).destroy();
    } else if (m instanceof AbstractConcurrentOffHeapMap<?, ?>) {
      ((AbstractConcurrentOffHeapMap<?, ?>) m).destroy();
    } else if (m instanceof Segment<?, ?>) {
      ((Segment<?, ?>) m).destroy();
    } else {
      Assert.fail();
    }

    MapTestRoutines.testEmptyMapFunctionalInvariants(m);
    Assert.assertThat(m.remove(GOOD_GENERATOR.generate(0)), IsNull.nullValue());
    Assert.assertThat(m.get(GOOD_GENERATOR.generate(0)), IsNull.nullValue());
    Assert.assertFalse(m.containsKey(GOOD_GENERATOR.generate(0)));
    Assert.assertFalse(m.equals(Collections.singletonMap(1, new byte[10])));
    Assert.assertThat(m.toString(), Is.is("{}"));
    Assert.assertThat(m.hashCode(), Is.is(0));
    Assert.assertFalse(m.containsValue(GOOD_GENERATOR.generate(0)));
  }

  @Test
  public final void testDestroyedMapIsImmutable() {
    Map<SpecialInteger, SpecialInteger> m = createGoodMap();
    
    for (int i = 0; i < 100; i++) {
      m.put(GOOD_GENERATOR.generate(i), GOOD_GENERATOR.generate(i));
    }

    if (m instanceof OffHeapHashMap<?, ?>) {
      ((OffHeapHashMap<?, ?>) m).destroy();
    } else if (m instanceof AbstractConcurrentOffHeapMap<?, ?>) {
      ((AbstractConcurrentOffHeapMap<?, ?>) m).destroy();
    } else if (m instanceof Segment<?, ?>) {
      ((Segment<?, ?>) m).destroy();
    } else {
      Assert.fail();
    }

    m.clear();
    m.putAll(Collections.<SpecialInteger, SpecialInteger>emptyMap());

    try {
      m.put(GOOD_GENERATOR.generate(1), GOOD_GENERATOR.generate(1));
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      Assert.assertThat(e.getMessage(), StringContains.containsString("destroyed"));
    }
    try {
      m.putAll(Collections.singletonMap(GOOD_GENERATOR.generate(1), GOOD_GENERATOR.generate(1)));
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      Assert.assertThat(e.getMessage(), StringContains.containsString("destroyed"));
    }
  }

  @Test
  public final void testDestroyWithLiveKeySetIterator() {
    UpfrontAllocatingPageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 4 * 1024 * 1024, 4 * 1024 * 1024);

    Map<Integer, byte[]> m = createOffHeapBufferMap(source);

    for (int i = 0; i < 10; i++) {
      m.put(i, new byte[128]);
    }
    
    Iterator<Integer> it = m.keySet().iterator();
    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(it.hasNext());
      Assert.assertNotNull(it.next());
    }
    
    Assert.assertTrue(it.hasNext());

    m.clear();
    
    if (m instanceof OffHeapHashMap<?, ?>) {
      ((OffHeapHashMap<?, ?>) m).destroy();
    } else if (m instanceof AbstractConcurrentOffHeapMap<?, ?>) {
      ((AbstractConcurrentOffHeapMap<?, ?>) m).destroy();
    } else if (m instanceof Segment<?, ?>) {
      ((Segment<?, ?>) m).destroy();
    } else {
      Assert.fail();
    }

    Assert.assertEquals(0, source.getAllocatedSize());

    try {
      m.put(1, new byte[1024]);
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      //expected
    }

    if (m instanceof ConcurrentMap<?, ?>) {
      Assert.assertTrue(it.hasNext());
      Assert.assertNotNull(it.next());
      Assert.assertFalse(it.hasNext());
      try {
        it.next();
        Assert.fail("Expected NoSuchElementException");
      } catch (NoSuchElementException e) {
        //expected
      }
    } else {
      try {
        it.next();
        Assert.fail("Expected ConcurrentModificationException");
      } catch (ConcurrentModificationException e) {
        //expected
      }
    }
  }
  
  @Test
  public void testFillBehavior() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), KILOBYTES.toBytes(32), KILOBYTES.toBytes(32)); 
    Map<Integer, byte[]> m = createOffHeapBufferMap(source);
    
    for (int i = 0; i < 100000; i++) {
      Set<Integer> keySetCopy = new HashSet<Integer>(m.keySet());
      MapTestRoutines.doFill(m, i, new byte[i % 1024]);
      if (m.containsKey(i)) {
        Assert.assertNotNull(m.get(i));
        Assert.assertEquals(i % 1024, m.get(i).length);
        Assert.assertTrue(keySetCopy.add(i));
        Assert.assertEquals(keySetCopy, m.keySet());
      } else {
        Assert.assertNull(m.get(i));
        Assert.assertEquals(keySetCopy, m.keySet());
        break;
      }
    }
  }
}
