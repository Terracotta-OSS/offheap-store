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

import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.terracotta.offheapstore.util.MemoryUnit.KILOBYTES;
import static org.terracotta.offheapstore.util.Generator.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.concurrent.AbstractConcurrentOffHeapMap;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.jdk8.BiFunction;
import org.terracotta.offheapstore.jdk8.Function;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.util.Generator;
import java.util.Arrays;

import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNull.nullValue;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.terracotta.offheapstore.MetadataTuple.metadataTuple;

/**
 *
 * @author Chris Dennis
 */
public abstract class AbstractOffHeapMapIT {

  public final Generator generator;

  public AbstractOffHeapMapIT(Generator generator) {
    this.generator = generator;
  }

  protected abstract Map<SpecialInteger, SpecialInteger> createMap(Generator generator);

  protected abstract Map<Integer, byte[]> createOffHeapBufferMap(PageSource source);

  @Test
  public void testRemoveByHash() throws Exception {
    final int ENTRIES_COUNT = 1013;
    Map<SpecialInteger, SpecialInteger> map = createMap(generator);
    HashingMap<SpecialInteger, SpecialInteger> hashingMap = (HashingMap<SpecialInteger, SpecialInteger>) map;

    for (int i = 0; i < ENTRIES_COUNT; i++) {
      SpecialInteger generated = generator.generate(i);
      hashingMap.put(generated, generated);
    }

    Map<SpecialInteger, SpecialInteger> removed = hashingMap.removeAllWithHash(BadInteger.HASHCODE);
    assertThat(removed.size(), greaterThan(0));
    assertThat(hashingMap.size(), is(ENTRIES_COUNT - removed.size()));

    for (int i = 0; i < ENTRIES_COUNT; i++) {
      SpecialInteger generated = generator.generate(i);
      if (removed.containsKey(generated)) {
        assertThat(hashingMap.get(generated), nullValue());
      } else {
        assertThat(hashingMap.get(generated), is(generated));
      }
    }
  }

  @Test
  public void testNpeBehaviors() {
    Map<SpecialInteger, SpecialInteger> map = createMap(generator);
    try {
      map.containsKey(null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
    try {
      map.remove(null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
    try {
      map.get(null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
    try {
      map.put(null, generator.generate(1));
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
    try {
      map.putAll(Collections.<SpecialInteger, SpecialInteger>singletonMap(null, generator.generate(1)));
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
  }

  @Test
  public final void testResizing() {
    Map<SpecialInteger, SpecialInteger> map = createMap(generator);
    for (int i = 0; i < 100; i++) {
      map.put(generator.generate(i), generator.generate(i));
      for (int j = 0; j <= i; j++) {
        Assert.assertEquals(generator.generate(j), map.get(generator.generate(j)));
      }
    }
    Assert.assertEquals(100, map.size());
    Assert.assertFalse(map.isEmpty());
    for (int i1 = 0; i1 < 100; i1++) {
      Assert.assertEquals(generator.generate(i1), map.remove(generator.generate(i1)));
      for (int j1 = i1 + 1; j1 < 100; j1++) {
        Assert.assertEquals(generator.generate(j1), map.get(generator.generate(j1)));
      }
    }
    Assert.assertTrue(map.isEmpty());
  }

  @Test
  public final void testEmptyMap() {
    testEmptyMap(generator, createMap(generator));
  }

  @Test
  public final void testPopulatedMap() {
    long randomSeed = System.nanoTime();
    System.err.println(this.getClass() + ".testPopulatedMap random seed = " + randomSeed);
    Random rndm = new Random(randomSeed);

    Map<SpecialInteger, SpecialInteger> map = createMap(generator);
    SpecialInteger keyA = generator.generate(rndm.nextInt());
    SpecialInteger valueA = generator.generate(rndm.nextInt());
    assertThat(map.put(keyA, valueA), nullValue());
    assertThat(map.get(keyA), is(valueA));
    assertTrue(map.containsKey(keyA));
    assertTrue(map.keySet().contains(keyA));
    assertTrue(map.containsValue(valueA));
    assertTrue(map.values().contains(valueA));
    assertFalse(map.isEmpty());

    SpecialInteger keyB;
    do {
      keyB = generator.generate(rndm.nextInt());
    } while (keyB.equals(keyA));
    SpecialInteger valueB1 = generator.generate(rndm.nextInt());
    assertThat(map.put(keyB, valueB1), nullValue());
    assertThat(map.get(keyB), is(valueB1));
    assertTrue(map.containsKey(keyB));
    assertTrue(map.keySet().contains(keyB));
    assertTrue(map.containsValue(valueB1));
    assertTrue(map.values().contains(valueB1));
    assertFalse(map.isEmpty());

    SpecialInteger valueB2 = generator.generate(rndm.nextInt());
    assertThat(map.put(keyB, valueB2), is(valueB1));
    assertThat(map.get(keyB), is(valueB2));
    assertTrue(map.containsKey(keyB));
    assertTrue(map.keySet().contains(keyB));
    assertTrue(map.containsValue(valueB2));
    assertTrue(map.values().contains(valueB2));
    assertFalse(map.isEmpty());

    assertThat(map.size(), is(2));
    assertThat(map.entrySet().size(), is(2));
    assertThat(map.values().size(), is(2));

    assertFalse(map.isEmpty());
    assertFalse(map.entrySet().isEmpty());
    assertFalse(map.values().isEmpty());

    Set<SpecialInteger> keySet = map.keySet();
    //keySet
    assertFalse(keySet.isEmpty());
    assertThat(keySet.size(), is(2));
    assertTrue(keySet.equals(keySet));
    assertTrue(keySet.containsAll(keySet));
    assertTrue(keySet.containsAll(Arrays.asList(keySet.toArray())));
    assertTrue(keySet.containsAll(Arrays.asList(keySet.toArray(new SpecialInteger[0]))));
    assertTrue(keySet.containsAll(new ArrayList<SpecialInteger>()));

    //keySet::toArray
    assertThat(keySet.toArray().length, is(2));
    assertSame(keySet.toArray().getClass(), Object[].class);
    {
      SpecialInteger[] a = keySet.toArray(new SpecialInteger[0]);
      assertThat(a.length, is(2));
      int i = 0;
      for (SpecialInteger j : keySet) {
        Assert.assertEquals(j, a[i++]);
      }
      assertSame(a.getClass(), SpecialInteger[].class);
    }
    {
      SpecialInteger[] a = keySet.toArray(new SpecialInteger[1]);
      assertThat(a.length, is(2));
      int i = 0;
      for (SpecialInteger j : keySet) {
        Assert.assertEquals(j, a[i++]);
      }
      assertSame(a.getClass(), SpecialInteger[].class);
    }
    {
      SpecialInteger[] a = keySet.toArray(new SpecialInteger[2]);
      assertThat(a.length, is(2));
      int i = 0;
      for (SpecialInteger j : keySet) {
        Assert.assertEquals(j, a[i++]);
      }
      assertSame(a.getClass(), SpecialInteger[].class);
    }
    {
      SpecialInteger[] a = keySet.toArray(new SpecialInteger[3]);
      assertThat(a.length, is(3));
      int i = 0;
      for (SpecialInteger j : keySet) {
        Assert.assertEquals(j, a[i++]);
      }
      assertThat(a[2], nullValue());
      assertSame(a.getClass(), SpecialInteger[].class);
    }

    Set<Map.Entry<SpecialInteger, SpecialInteger>> entrySet = map.entrySet();
    //entrySet
    assertFalse(entrySet.isEmpty());
    assertThat(entrySet.size(), is(2));
    assertTrue(entrySet.equals(entrySet));
    assertTrue(entrySet.containsAll(entrySet));
    assertTrue(entrySet.containsAll(Arrays.asList(entrySet.toArray())));
    assertTrue(entrySet.containsAll(Arrays.asList(entrySet.toArray(new Map.Entry[0]))));
    assertTrue(entrySet.containsAll(new ArrayList<Map.Entry>()));

    //entrySet::toArray
    assertThat(entrySet.toArray().length, is(2));
    assertSame(entrySet.toArray().getClass(), Object[].class);
    {
      Map.Entry[] a = entrySet.toArray(new Map.Entry[0]);
      assertThat(a.length, is(2));
      int i = 0;
      for (Map.Entry<SpecialInteger, SpecialInteger> j : entrySet) {
        Assert.assertEquals(j, a[i++]);
      }
      assertSame(a.getClass(), Map.Entry[].class);
    }
    {
      Map.Entry[] a = entrySet.toArray(new Map.Entry[1]);
      assertThat(a.length, is(2));
      int i = 0;
      for (Map.Entry<SpecialInteger, SpecialInteger> j : entrySet) {
        Assert.assertEquals(j, a[i++]);
      }
      assertSame(a.getClass(), Map.Entry[].class);
    }
    {
      Map.Entry[] a = entrySet.toArray(new Map.Entry[2]);
      assertThat(a.length, is(2));
      int i = 0;
      for (Map.Entry<SpecialInteger, SpecialInteger> j : entrySet) {
        Assert.assertEquals(j, a[i++]);
      }
      assertSame(a.getClass(), Map.Entry[].class);
    }
    {
      Map.Entry[] a = entrySet.toArray(new Map.Entry[3]);
      assertThat(a.length, is(3));
      int i = 0;
      for (Map.Entry<SpecialInteger, SpecialInteger> j : entrySet) {
        Assert.assertEquals(j, a[i++]);
      }
      assertThat(a[2], nullValue());
      assertSame(a.getClass(), Map.Entry[].class);
    }

    Collection<SpecialInteger> values = map.values();
    //values
    assertFalse(values.isEmpty());
    assertThat(values.size(), is(2));
    assertTrue(values.containsAll(values));
    assertTrue(values.containsAll(Arrays.asList(values.toArray())));
    assertTrue(values.containsAll(Arrays.asList(values.toArray(new SpecialInteger[0]))));
    assertTrue(values.containsAll(new ArrayList<SpecialInteger>()));

    //values::toArray
    assertThat(values.toArray().length, is(2));
    assertSame(values.toArray().getClass(), Object[].class);
    {
      SpecialInteger[] a = values.toArray(new SpecialInteger[0]);
      assertThat(a.length, is(2));
      int i = 0;
      for (SpecialInteger j : values) {
        Assert.assertEquals(j, a[i++]);
      }
      assertSame(a.getClass(), SpecialInteger[].class);
    }
    {
      SpecialInteger[] a = values.toArray(new SpecialInteger[1]);
      assertThat(a.length, is(2));
      int i = 0;
      for (SpecialInteger j : values) {
        Assert.assertEquals(j, a[i++]);
      }
      assertSame(a.getClass(), SpecialInteger[].class);
    }
    {
      SpecialInteger[] a = values.toArray(new SpecialInteger[2]);
      assertThat(a.length, is(2));
      int i = 0;
      for (SpecialInteger j : values) {
        Assert.assertEquals(j, a[i++]);
      }
      assertSame(a.getClass(), SpecialInteger[].class);
    }
    {
      SpecialInteger[] a = values.toArray(new SpecialInteger[3]);
      assertThat(a.length, is(3));
      int i = 0;
      for (SpecialInteger j : values) {
        Assert.assertEquals(j, a[i++]);
      }
      assertThat(a[2], nullValue());
      assertSame(a.getClass(), SpecialInteger[].class);
    }
  }

  @Test
  public final void testTerminalEntryIterator() {
    Map<SpecialInteger, SpecialInteger> map = createMap(generator);
    for (int i = 0; i < 20; i++) {
      map.put(generator.generate(i), generator.generate(i));
    }
    final Iterator<?> it = map.entrySet().iterator();
    try {
      while (true) {
        it.next();
      }
    } catch (NoSuchElementException e) {
    }
  }

  @Test
  public final void testTerminalEncodingIterator() {
    Map<SpecialInteger, SpecialInteger> map = createMap(generator);
    assumeThat(map, instanceOf(OffHeapHashMap.class));

    OffHeapHashMap<SpecialInteger, SpecialInteger> offheapMap = (OffHeapHashMap<SpecialInteger, SpecialInteger>) map;
    for (int i = 0; i < 20; i++) {
      offheapMap.put(generator.generate(i), generator.generate(i));
    }
    Iterator<?> it = offheapMap.encodingSet().iterator();
    try {
      while (true) {
        it.next();
      }
    } catch (NoSuchElementException e) {
    }
  }

  @Test
  public final void testTerminalValueIterator() {
    Map<SpecialInteger, SpecialInteger> map = createMap(generator);
    for (int i = 0; i < 20; i++) {
      map.put(generator.generate(i), generator.generate(i));
    }
    final Iterator<?> it = map.values().iterator();
    try {
      while (true) {
        it.next();
      }
    } catch (NoSuchElementException e) {
    }
  }

  @Test
  public final void testTerminalKeyIterator() {
    Map<SpecialInteger, SpecialInteger> map = createMap(generator);
    for (int i = 0; i < 20; i++) {
      map.put(generator.generate(i), generator.generate(i));
    }
    final Iterator<?> it = map.keySet().iterator();
    try {
      while (true) {
        it.next();
      }
    } catch (NoSuchElementException e) {
    }
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
        Assert.assertThat(e.getMessage(), containsString("destroyed"));
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
    Map<SpecialInteger, SpecialInteger> m = createMap(generator);

    for (int i = 0; i < 100; i++) {
      m.put(generator.generate(i), generator.generate(i));
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

    testEmptyMap(generator, m);
  }

  @Test
  public final void testDestroyedMapIsImmutable() {
    Map<SpecialInteger, SpecialInteger> m = createMap(generator);

    for (int i = 0; i < 100; i++) {
      m.put(generator.generate(i), generator.generate(i));
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
      m.put(generator.generate(1), generator.generate(1));
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      Assert.assertThat(e.getMessage(), containsString("destroyed"));
    }
    try {
      m.putAll(Collections.singletonMap(generator.generate(1), generator.generate(1)));
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      Assert.assertThat(e.getMessage(), containsString("destroyed"));
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
      doFill(m, i, new byte[i % 1024]);
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

  @Test
  public final void testSimpleMutationUsingComputes() {
    long randomSeed = System.nanoTime();
    System.err.println(this.getClass() + ".testSimpleMutationUsingCompute random seed = " + randomSeed);
    Random rndm = new Random(randomSeed);

    Map<SpecialInteger, SpecialInteger> map = createMap(generator);
    SpecialInteger keyA = generator.generate(rndm.nextInt());
    final SpecialInteger valueA1 = generator.generate(rndm.nextInt());
    final SpecialInteger valueA2 = generator.generate(valueA1.value() + 1);

    assertThat(doComputeIfPresentWithMetadata(map, keyA, new BiFunction<SpecialInteger, MetadataTuple<SpecialInteger>, MetadataTuple<SpecialInteger>>() {
      @Override
      public MetadataTuple<SpecialInteger> apply(SpecialInteger t, MetadataTuple<SpecialInteger> u) {
        throw new AssertionError("Unexpected function invocation");
      }
    }), nullValue());
    assertThat(doComputeIfAbsentWithMetadata(map, keyA, new Function<SpecialInteger, MetadataTuple<SpecialInteger>>() {
      @Override
      public MetadataTuple<SpecialInteger> apply(SpecialInteger t) {
        return metadataTuple(valueA1, 0);
      }
    }), is(metadataTuple(valueA1, 0)));

    assertThat(doComputeIfPresentWithMetadata(map, keyA, new BiFunction<SpecialInteger, MetadataTuple<SpecialInteger>, MetadataTuple<SpecialInteger>>() {
      @Override
      public MetadataTuple<SpecialInteger> apply(SpecialInteger t, MetadataTuple<SpecialInteger> u) {
        return metadataTuple(generator.generate(u.value().value() + 1), 16);
      }
    }), is(metadataTuple(valueA2, 16)));
    assertThat(map.get(keyA), is(valueA2));
    assertThat(map.size(), is(1));

    SpecialInteger keyB;
    do {
      keyB = generator.generate(rndm.nextInt());
    } while (keyB.equals(keyA));
    final SpecialInteger valueB1 = generator.generate(rndm.nextInt());
    assertThat(doComputeWithMetadata(map, keyB, new BiFunction<SpecialInteger, MetadataTuple<SpecialInteger>, MetadataTuple<SpecialInteger>>() {
      @Override
      public MetadataTuple<SpecialInteger> apply(SpecialInteger t, MetadataTuple<SpecialInteger> u) {
        assertThat(u, nullValue());
        return metadataTuple(valueB1, 0);
      }
    }), is(metadataTuple(valueB1, 0)));
    assertThat(map.get(keyB), is(valueB1));
    assertThat(map.get(keyA), is(valueA2));
    assertThat(map.size(), is(2));

    final SpecialInteger valueB2 = generator.generate(rndm.nextInt());
    assertThat(doComputeWithMetadata(map, keyB, new BiFunction<SpecialInteger, MetadataTuple<SpecialInteger>, MetadataTuple<SpecialInteger>>() {
      @Override
      public MetadataTuple<SpecialInteger> apply(SpecialInteger t, MetadataTuple<SpecialInteger> u) {
        return metadataTuple(valueB2, 32);
      }
    }), is(metadataTuple(valueB2, 32)));
    assertThat(map.get(keyB), is(valueB2));
    assertThat(map.get(keyA), is(valueA2));
    assertThat(map.size(), is(2));

    assertThat(doComputeWithMetadata(map, keyA, new BiFunction<SpecialInteger, MetadataTuple<SpecialInteger>, MetadataTuple<SpecialInteger>>() {
      @Override
      public MetadataTuple<SpecialInteger> apply(SpecialInteger t, MetadataTuple<SpecialInteger> u) {
        return null;
      }
    }), nullValue());
    assertThat(map.get(keyB), is(valueB2));
    assertThat(map.get(keyA), nullValue());
    assertThat(map.size(), is(1));

    assertThat(doComputeIfPresentWithMetadata(map, keyB, new BiFunction<SpecialInteger, MetadataTuple<SpecialInteger>, MetadataTuple<SpecialInteger>>() {
      @Override
      public MetadataTuple<SpecialInteger> apply(SpecialInteger t, MetadataTuple<SpecialInteger> u) {
        return null;
      }
    }), nullValue());
    assertThat(map.get(keyB), nullValue());
    assertThat(map.get(keyA), nullValue());
    assertThat(map.size(), is(0));

    assertThat(doComputeIfPresentWithMetadata(map, keyA, new BiFunction<SpecialInteger, MetadataTuple<SpecialInteger>, MetadataTuple<SpecialInteger>>() {
      @Override
      public MetadataTuple<SpecialInteger> apply(SpecialInteger t, MetadataTuple<SpecialInteger> u) {
        return null;
      }
    }), nullValue());
    assertThat(map.get(keyB), nullValue());
    assertThat(map.get(keyA), nullValue());
    assertThat(map.size(), is(0));
  }

  private static void testEmptyMap(Generator g, Map<SpecialInteger, SpecialInteger> m) {
    assertThat(m.size(), is(0));
    assertThat(m.entrySet().size(), is(0));
    assertThat(m.values().size(), is(0));

    assertTrue(m.isEmpty());
    assertTrue(m.entrySet().isEmpty());
    assertTrue(m.values().isEmpty());

    assertThat(m.toString(), is("{}"));
    assertFalse(m.containsValue(g.generate(1)));
    assertFalse(m.containsKey(g.generate(1)));

    Set<SpecialInteger> keySet = m.keySet();
    //keySet
    assertTrue(keySet.isEmpty());
    assertThat(keySet.size(), is(0));
    assertThat(keySet.toString(), is("[]"));
    assertThat(keySet.hashCode(), is(0));
    assertTrue(keySet.containsAll(keySet));
    assertTrue(keySet.equals(Collections.emptySet()));
    assertTrue(Collections.emptySet().equals(keySet));
    assertTrue(keySet.equals(keySet));

    //keySet::iterator
    assertFalse(keySet.iterator().hasNext());
    try {
      keySet.iterator().next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }

    //keySet::toArray
    assertThat(keySet.toArray().length, is(0));
    assertSame(keySet.toArray().getClass(), Object[].class);
    SpecialInteger[] emptyKeyArray = keySet.toArray(new SpecialInteger[0]);
    assertSame(emptyKeyArray.getClass(), SpecialInteger[].class);
    assertThat(emptyKeyArray.length, is(0));
    SpecialInteger[] nullKeyArray = keySet.toArray(new SpecialInteger[1]);
    assertSame(nullKeyArray.getClass(), SpecialInteger[].class);
    assertThat(nullKeyArray.length, is(1));
    assertThat(nullKeyArray[0], nullValue());

    Set<Map.Entry<SpecialInteger, SpecialInteger>> entrySet = m.entrySet();
    //entrySet
    assertTrue(entrySet.isEmpty());
    assertThat(entrySet.size(), is(0));
    assertThat(entrySet.toString(), is("[]"));
    assertThat(entrySet.hashCode(), is(0));
    assertTrue(entrySet.containsAll(entrySet));
    assertTrue(entrySet.equals(Collections.emptySet()));
    assertTrue(Collections.emptySet().equals(entrySet));
    assertTrue(entrySet.equals(entrySet));

    //entrySet::iterator
    assertFalse(entrySet.iterator().hasNext());
    try {
      entrySet.iterator().next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }

    //entrySet::toArray
    assertThat(entrySet.toArray().length, is(0));
    assertSame(entrySet.toArray().getClass(), Object[].class);
    Map.Entry[] emptyEntryArray = entrySet.toArray(new Map.Entry[0]);
    assertSame(emptyEntryArray.getClass(), Map.Entry[].class);
    assertThat(emptyEntryArray.length, is(0));
    Map.Entry[] nullEntryArray = entrySet.toArray(new Map.Entry[1]);
    assertSame(nullEntryArray.getClass(), Map.Entry[].class);
    assertThat(nullEntryArray.length, is(1));
    assertThat(nullEntryArray[0], nullValue());

    Collection<SpecialInteger> values = m.values();
    //values
    assertTrue(values.isEmpty());
    assertThat(values.size(), is(0));
    assertThat(values.toString(), is("[]"));
    assertTrue(values.containsAll(values));

    //values::iterator
    assertFalse(values.iterator().hasNext());
    try {
      values.iterator().next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) {
      //expected
    }

    //values::toArray
    assertThat(values.toArray().length, is(0));
    assertSame(values.toArray().getClass(), Object[].class);
    SpecialInteger[] emptyValuesArray = values.toArray(new SpecialInteger[0]);
    assertSame(emptyValuesArray.getClass(), SpecialInteger[].class);
    assertThat(emptyValuesArray.length, is(0));
    SpecialInteger[] nullValuesArray = values.toArray(new SpecialInteger[1]);
    assertSame(nullValuesArray.getClass(), SpecialInteger[].class);
    assertThat(nullValuesArray.length, is(1));
    assertThat(nullValuesArray[0], nullValue());
  }

  public static <K, V> V doFill(Map<K, V> map, K key, V value) {
    if (map instanceof OffHeapHashMap<?, ?>) {
      return ((OffHeapHashMap<K, V>) map).fill(key, value);
    } else if (map instanceof Segment<?, ?>) {
      return ((Segment<K, V>) map).fill(key, value);
    } else if (map instanceof AbstractConcurrentOffHeapMap<?, ?>) {
      return ((AbstractConcurrentOffHeapMap<K, V>) map).fill(key, value);
    } else {
      throw new AssertionError("Unexpected type : " + map.getClass());
    }
  }

  public static <K, V> MetadataTuple<V> doComputeWithMetadata(Map<K, V> map, K key, BiFunction<? super K, ? super MetadataTuple<V>, ? extends MetadataTuple<V>> function) {
    if (map instanceof OffHeapHashMap<?, ?>) {
      return ((OffHeapHashMap<K, V>) map).computeWithMetadata(key, function);
    } else if (map instanceof Segment<?, ?>) {
      return ((Segment<K, V>) map).computeWithMetadata(key, function);
    } else if (map instanceof AbstractConcurrentOffHeapMap<?, ?>) {
      return ((AbstractConcurrentOffHeapMap<K, V>) map).computeWithMetadata(key, function);
    } else {
      throw new AssertionError("Unexpected type : " + map.getClass());
    }
  }

  public static <K, V> MetadataTuple<V> doComputeIfAbsentWithMetadata(Map<K, V> map, K key, Function<? super K, ? extends MetadataTuple<V>> function) {
    if (map instanceof OffHeapHashMap<?, ?>) {
      return ((OffHeapHashMap<K, V>) map).computeIfAbsentWithMetadata(key, function);
    } else if (map instanceof Segment<?, ?>) {
      return ((Segment<K, V>) map).computeIfAbsentWithMetadata(key, function);
    } else if (map instanceof AbstractConcurrentOffHeapMap<?, ?>) {
      return ((AbstractConcurrentOffHeapMap<K, V>) map).computeIfAbsentWithMetadata(key, function);
    } else {
      throw new AssertionError("Unexpected type : " + map.getClass());
    }
  }

  public static <K, V> MetadataTuple<V> doComputeIfPresentWithMetadata(Map<K, V> map, K key, BiFunction<? super K, ? super MetadataTuple<V>, ? extends MetadataTuple<V>> function) {
    if (map instanceof OffHeapHashMap<?, ?>) {
      return ((OffHeapHashMap<K, V>) map).computeIfPresentWithMetadata(key, function);
    } else if (map instanceof Segment<?, ?>) {
      return ((Segment<K, V>) map).computeIfPresentWithMetadata(key, function);
    } else if (map instanceof AbstractConcurrentOffHeapMap<?, ?>) {
      return ((AbstractConcurrentOffHeapMap<K, V>) map).computeIfPresentWithMetadata(key, function);
    } else {
      throw new AssertionError("Unexpected type : " + map.getClass());
    }
  }
}
