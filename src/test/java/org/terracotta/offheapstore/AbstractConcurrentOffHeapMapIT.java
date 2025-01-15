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
package org.terracotta.offheapstore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.concurrent.AbstractConcurrentOffHeapCache;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.util.Generator;
import org.terracotta.offheapstore.util.Generator.SpecialInteger;
import java.util.Iterator;

import java.util.concurrent.atomic.AtomicReference;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.CombinableMatcher.either;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

/**
 *
 * @author cdennis
 */
public abstract class AbstractConcurrentOffHeapMapIT extends AbstractOffHeapMapIT {

  public AbstractConcurrentOffHeapMapIT(Generator generator) {
    super(generator);
  }

  @Override
  protected abstract ConcurrentMap<SpecialInteger, SpecialInteger> createMap(Generator generator);

  @Test
  public void testConcurrentMethodNpeBehaviors() {
    ConcurrentMap<SpecialInteger, SpecialInteger> map = createMap(generator);
    try {
      map.remove(null, null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
    try {
      map.replace(null, generator.generate(1));
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
    try {
      map.replace(null, generator.generate(1), generator.generate(1));
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
    try {
      map.putIfAbsent(null, generator.generate(1));
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
  }

  @Test
  public final void testConcurrentMap() {
    long randomSeed = System.nanoTime();
    System.err.println(this.getClass() + ".testConcurrentMap random seed = " + randomSeed);
    Random rndm = new Random(randomSeed);

    ConcurrentMap<SpecialInteger, SpecialInteger> m = createMap(generator);

    SpecialInteger keyOne = generator.generate(rndm.nextInt());
    SpecialInteger valueOne = generator.generate(rndm.nextInt());

    assertThat(m.putIfAbsent(keyOne, valueOne), nullValue());
    assertTrue(m.containsKey(keyOne));
    assertThat(m.putIfAbsent(keyOne, generator.generate(rndm.nextInt())), is(valueOne));
    assertTrue(m.containsKey(keyOne));

    assertFalse(m.replace(keyOne, generator.generate(rndm.nextInt()), generator.generate(rndm.nextInt())));
    assertTrue(m.containsKey(keyOne));
    assertTrue(m.containsKey(keyOne));
    assertThat(m.get(keyOne), is(valueOne));

    SpecialInteger valueTwo = generator.generate(5555);
    assertTrue(m.replace(keyOne, valueOne, valueTwo));
    assertTrue(m.replace(keyOne, valueTwo, valueOne));
    assertThat(m.get(keyOne), is(valueOne));

    SpecialInteger keyTwo;
    do {
      keyTwo = generator.generate(rndm.nextInt());
    } while (keyOne.equals(keyTwo));

    assertThat(m.replace(keyTwo, generator.generate(rndm.nextInt())), nullValue());
    assertThat(m.replace(keyOne, valueOne), is(valueOne));
    assertThat(m.replace(keyOne, valueTwo), is(valueOne));
    assertThat(m.get(keyOne), is(valueTwo));
    assertThat(m.replace(keyOne, valueOne), is(valueTwo));
    assertThat(m.get(keyOne), is(valueOne));

    SpecialInteger valueThree;
    do {
      valueThree = generator.generate(rndm.nextInt());
    } while (valueOne.equals(valueThree));

    assertFalse(m.remove(keyOne, valueThree));
    assertThat(m.get(keyOne), is(valueOne));
    assertTrue(m.containsKey(keyOne));
    assertFalse(m.remove(keyOne, null));
    assertThat(m.get(keyOne), is(valueOne));
    assertTrue(m.remove(keyOne, valueOne));
    assertThat(m.get(keyOne), nullValue());
    assertFalse(m.containsKey(keyOne));
    assertTrue(m.isEmpty());

    SpecialInteger keyThree = generator.generate(rndm.nextInt());
    SpecialInteger valueFour = generator.generate(rndm.nextInt());
    m.putIfAbsent(keyThree, valueFour);
    assertThat(m.size(), is(1));
    assertFalse(m.remove(keyThree, null));
    assertFalse(m.remove(keyThree, null));
    assertFalse(m.remove(keyThree, keyThree));
    assertTrue(m.remove(keyThree, valueFour));
    assertTrue(m.isEmpty());
  }

  @Test
  public final void testSubCollections() throws InterruptedException {
    final Map<SpecialInteger, SpecialInteger> m = createMap(generator);
    final Throwable[] failure = new Throwable[1];
    final int maximumSize = 1000;

    Thread loader = new Thread(() -> {
      for (int i = 0; i < maximumSize; i++) {
        m.put(generator.generate(i), generator.generate(i));
      }
    });

    Thread tester = new Thread(new SubCollectionTester(m, maximumSize, failure));

    tester.start();
    loader.start();

    loader.join();
    tester.join();

    if (failure[0] != null) {
      throw new AssertionError(failure[0]);
    }
  }

  @Test
  public final void testConcurrentSubCollections() throws InterruptedException {
    final Map<SpecialInteger, SpecialInteger> m = createMap(generator);
    final Throwable[] failure = new Throwable[1];
    final int maximumSize = 1000;

    Thread loader = new Thread(() -> {
      for (int i = 0; i < maximumSize; i++) {
        m.put(generator.generate(i), generator.generate(i));
      }
    });

    Thread tester1 = new Thread(new SubCollectionTester(m, maximumSize, failure));
    Thread tester2 = new Thread(new SubCollectionTester(m, maximumSize, failure));

    tester1.start();
    tester2.start();
    loader.start();

    loader.join();
    tester1.join();
    tester2.join();

    if (failure[0] != null) {
      throw new AssertionError(failure[0]);
    }
  }

  @Test
  public final void testConcurrentRemoval() {
    ConcurrentMap<SpecialInteger, SpecialInteger> m = createMap(generator);
    assertTrue(m.isEmpty());
    m.put(generator.generate(1), generator.generate(2));
    Iterator<Map.Entry<SpecialInteger, SpecialInteger>> it = m.entrySet().iterator();
    assertTrue(it.hasNext());
    assertThat(m.remove(generator.generate(1)), is(generator.generate(2)));
    Map.Entry<SpecialInteger, SpecialInteger> e = it.next();
    assertTrue(m.isEmpty());
    assertThat(e.getKey(), is(generator.generate(1)));
    assertThat(e.getValue(), is(generator.generate(2)));
  }

  @Test
  public final void testConcurrentAddition() {
    ConcurrentMap<SpecialInteger, SpecialInteger> m = createMap(generator);
    m.put(generator.generate(1), generator.generate(2));
    Iterator<Map.Entry<SpecialInteger, SpecialInteger>> it = m.entrySet().iterator();
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(generator.generate(2), m.put(generator.generate(1), generator.generate(3)));
    Map.Entry<SpecialInteger, SpecialInteger> e = it.next();
    Assert.assertEquals(generator.generate(1), e.getKey());

    Assert.assertThat(e.getValue(), anyOf(equalTo(generator.generate(2)), equalTo(generator.generate(3))));
  }

  @Test
  public final void testConcurrentIterationAndMutation() {
    final ConcurrentMap<SpecialInteger, SpecialInteger> m = createMap(generator);
    final AtomicBoolean stopped = new AtomicBoolean(false);
    Thread mutator = new Thread(() -> {
      int counter = 0;
      Random rndm = new Random();
      while (!stopped.get()) {
        if (++counter == 25) {
          counter = 0;
          // Fixing reader starvation on some JVMs / Hardware combinations
          Thread.yield();
        }
        int v = rndm.nextInt(8192);
        if (rndm.nextBoolean()) {
          m.remove(generator.generate(v));
        } else {
          m.put(generator.generate(v), generator.generate(v));
        }
      }
    });

    mutator.start();

    boolean interrupted = Thread.interrupted();
    try {
      for (int i = 0; i < 10; i++) {
        int checked = 0;
        for (Map.Entry<SpecialInteger, SpecialInteger> e : m.entrySet()) {
          Assert.assertEquals(e.getKey(), e.getValue());
          checked++;
        }
        System.out.println("Finished Read Pass " + i + " : Checked " + checked);
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
  }

  @Test
  public final void testConcurrentIterationAndMutationWithHalfOffHeapStorageEngine() {
    final Map<Integer, byte[]> m = createOffHeapBufferMap(new UnlimitedPageSource(new HeapBufferSource()));

    final AtomicReference<Throwable> mutatorExceptionRef = new AtomicReference<>();
    final AtomicBoolean stopped = new AtomicBoolean(false);
    Thread mutator = new Thread(() -> {
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
    });

    mutator.start();

    boolean interrupted = false;
    try {
      for (int i = 0; i < 10; i++) {
        int checked = 0;
        for (Map.Entry<Integer, byte[]> e : m.entrySet()) {
          Integer key = e.getKey();
          byte[] value = e.getValue();
          Assert.assertEquals(key >>> 3, value.length);
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

    final AtomicReference<Throwable> mutatorExceptionRef = new AtomicReference<>();
    final AtomicBoolean stopped = new AtomicBoolean(false);
    Thread mutator = new Thread(() -> {
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
    });

    mutator.start();

    boolean interrupted = Thread.interrupted();
    try {
      for (int i = 0; i < 10; i++) {
        int checked = 0;
        for (Map.Entry<Integer, byte[]> e : m.entrySet()) {
          Integer key = e.getKey();
          byte[] value = e.getValue();
          Assert.assertEquals(key >>> 3, value.length);
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
  public void testConcurrentUpdateOfKeyWhileIterating() {
    final Map<SpecialInteger, SpecialInteger> m = createMap(generator);

    assumeThat(m, not(either(instanceOf(AbstractConcurrentOffHeapCache.class))
                    .or(instanceOf(AbstractOffHeapClockCache.class))));

    m.put(generator.generate(1024), generator.generate(1024));
    m.put(generator.generate(1), generator.generate(1));

    List<Iterator<SpecialInteger>> iterators = new ArrayList<>(1024);
    for (int i = 0; i < 1024; i++) {
      iterators.add(m.keySet().iterator());
      m.put(generator.generate(i), generator.generate(i));
      m.put(generator.generate(1024), generator.generate(i));
    }

    for (Iterator<SpecialInteger> it : iterators) {
      boolean foundMagicKey = false;
      while (it.hasNext()) {
        if (it.next().equals(generator.generate(1024))) {
          foundMagicKey = true;
        }
      }
      assertTrue(foundMagicKey);
    }
  }

  static class SubCollectionTester implements Runnable {

    private final Throwable[] failure;
    private final Map<?, ?> map;
    private final int maximumSize;

    private int previousSize = 0;

    SubCollectionTester(Map<?, ?> map, int max, Throwable[] failure) {
      this.failure = failure;
      this.map = map;
      this.maximumSize = max;
    }

    private boolean check(int size) {
      if (size < previousSize) throw new AssertionError("Map has shrunk?");
      if (size > maximumSize) throw new AssertionError("Max bigger than possible?");
      if (size == maximumSize) return true;
      previousSize = size;
      return false;
    }

    @Override
    public void run() {
      try {
        while (true) {
          if (check(map.keySet().size())) return;
          if (check(map.entrySet().size())) return;
          if (check(map.values().size())) return;
          if (check(map.keySet().toArray().length)) return;
          if (check(map.entrySet().toArray().length)) return;
          if (check(map.values().toArray().length)) return;
        }
      } catch (Throwable t) {
        failure[0] = t;
      }
    }

  }
}
