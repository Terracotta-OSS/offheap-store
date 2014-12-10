package com.terracottatech.offheapstore;

import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.IsEqual.equalTo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.RandomAccess;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;

import com.terracottatech.offheapstore.concurrent.AbstractConcurrentOffHeapMap;
import com.terracottatech.offheapstore.util.Generator;
import com.terracottatech.offheapstore.util.Generator.SpecialInteger;

/**
 *
 * @author Chris Dennis
 */
public final class MapTestRoutines {

  private MapTestRoutines() {
    //
  }

  public static void testResizing(Generator g, Map<SpecialInteger, SpecialInteger> m) {
    for (int i = 0; i < 100; i++) {
      m.put(g.generate(i), g.generate(i));
      
      for (int j = 0; j <= i; j++) {
        Assert.assertEquals(g.generate(j), m.get(g.generate(j)));
      }
    }

    Assert.assertEquals(100, m.size());
    Assert.assertFalse(m.isEmpty());
    
    for (int i = 0; i < 100; i++) {
      Assert.assertEquals(g.generate(i), m.remove(g.generate(i)));
      
      for (int j = i + 1; j < 100; j++) {
        Assert.assertEquals(g.generate(j), m.get(g.generate(j)));
      }
    }
    
    Assert.assertTrue(m.isEmpty());
  }

  public static void testEmptyMapFunctionalInvariants(Map<SpecialInteger, SpecialInteger> m) {
    checkFunctionalInvariants(m);
  }

  public static void testPopulatedMapFunctionalInvariants(Generator g, Map<SpecialInteger, SpecialInteger> m) {
    Assert.assertNull(m.put(g.generate(3333), g.generate(77777)));
    Assert.assertNull(m.put(g.generate(9134), g.generate(74982)));
    Assert.assertEquals(m.get(g.generate(9134)), g.generate(74982));
    Assert.assertEquals(m.put(g.generate(9134), g.generate(1382)), g.generate(74982));
    Assert.assertEquals(m.get(g.generate(9134)), g.generate(1382));
    Assert.assertEquals(m.size(), 2);
    checkFunctionalInvariants(m);
    checkNPEConsistency(g, m);
  }

    private static final int SIZE = 6;

  public static void testTerminalValueIterator(Generator g, Map<SpecialInteger, SpecialInteger> m) {
    for (int i = 0; i < 3*SIZE; i++) {
        m.put(g.generate(i), g.generate(i));
    }
    testTerminalIterator(m.values());
  }

  public static void testTerminalKeyIterator(Generator g, Map<SpecialInteger, SpecialInteger> m) {
    for (int i = 0; i < 3*SIZE; i++) {
        m.put(g.generate(i), g.generate(i));
    }
    testTerminalIterator(m.keySet());
  }

  public static void testTerminalEntryIterator(Generator g, Map<SpecialInteger, SpecialInteger> m) {
    for (int i = 0; i < 3*SIZE; i++) {
        m.put(g.generate(i), g.generate(i));
    }
    testTerminalIterator(m.entrySet());
  }
  
  public static void testTerminalEncodingIterator(Generator g, Map<SpecialInteger, SpecialInteger> m) {
    if (! (m instanceof OffHeapHashMap)) {
      return;
    }

    OffHeapHashMap<SpecialInteger, SpecialInteger> offheapMap = (OffHeapHashMap<SpecialInteger, SpecialInteger>) m;

    for (int i = 0; i < 3*SIZE; i++) {
      offheapMap.put(g.generate(i), g.generate(i));
    }
    testTerminalIterator(offheapMap.encodingSet());
  }

  private static void testTerminalIterator(Collection<?> c) {
    final Iterator<?> it = c.iterator();
    THROWS(NoSuchElementException.class, new Fun() {
      @Override
      void f() {
        while (true) {
          it.next();
        }
      }
    });
    try {
      it.remove();
    } catch (UnsupportedOperationException e) {
      return;
    }
  }

  public static void testGetBehavior(Generator g, Map<SpecialInteger, SpecialInteger> m) {
    // We verify following assertions in get(Object) method javadocs
    checkPut(m, g.generate(1), g.generate(2), null);
    checkPut(m, g.generate(1), g.generate(1), g.generate(2));
    checkPut(m, g.generate(2), g.generate(2), null);
    checkPut(m, g.generate(1), g.generate(1), g.generate(1));

    try {
      m.get(null);
      Assert.fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }

    try {
      m.put(null, g.generate(2));
      Assert.fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }

    try {
      m.put(g.generate(1), null);
      Assert.fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }

    try {
      m.put(g.generate(3), null);
      Assert.fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
  }

  private static void checkPut(Map<SpecialInteger, SpecialInteger> m, SpecialInteger key, SpecialInteger value, SpecialInteger oldValue) {
    if (oldValue != null) {
      Assert.assertTrue(m.containsValue(oldValue));
      Assert.assertTrue(m.values().contains(oldValue));
    }
    Assert.assertEquals(oldValue, m.put(key, value));
    Assert.assertEquals(value, m.get(key));
    Assert.assertTrue(m.containsKey(key));
    Assert.assertTrue(m.keySet().contains(key));
    Assert.assertTrue(m.containsValue(value));
    Assert.assertTrue(m.values().contains(value));
    Assert.assertTrue(!m.isEmpty());
  }

  public static void testSubCollections(final Generator g, final Map<SpecialInteger, SpecialInteger> m) throws InterruptedException {
    final Throwable[] failure = new Throwable[1];
    final int maximumSize = 1000;

    Thread loader = new Thread() {
      @Override
      public void run() {
        for (int i = 0; i < maximumSize; i++) {
          m.put(g.generate(i), g.generate(i));
        }
      }
    };

    Thread tester = new Thread(new SubCollectionTester(m, maximumSize, failure));

    tester.start();
    loader.start();

    loader.join();
    tester.join();

    if (failure[0] != null) {
      throw new AssertionError(failure[0]);
    }
  }

  public static void testConcurrentSubCollections(final Generator g, final Map<SpecialInteger, SpecialInteger> m) throws InterruptedException {
    final Throwable[] failure = new Throwable[1];
    final int maximumSize = 1000;

    Thread loader = new Thread() {
      @Override
      public void run() {
        for (int i = 0; i < maximumSize; i++) {
          m.put(g.generate(i), g.generate(i));
        }
      }
    };

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
  
  public static void testConcurrentRemoval(Generator g, Map<SpecialInteger, SpecialInteger> m) {
    m.clear();
    Assert.assertTrue(m.isEmpty());
    m.put(g.generate(1), g.generate(2));
    Iterator<Entry<SpecialInteger, SpecialInteger>> it = m.entrySet().iterator();
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(g.generate(2), m.remove(g.generate(1)));
    Entry<SpecialInteger, SpecialInteger> e = it.next();
    Assert.assertTrue(m.isEmpty());
    Assert.assertEquals(g.generate(1), e.getKey());
    Assert.assertEquals(g.generate(2), e.getValue());
  }

  @SuppressWarnings("unchecked")
  public static void testConcurrentAddition(Generator g, Map<SpecialInteger, SpecialInteger> m) {
    m.clear();
    Assert.assertTrue(m.isEmpty());
    m.put(g.generate(1), g.generate(2));
    Iterator<Entry<SpecialInteger, SpecialInteger>> it = m.entrySet().iterator();
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(g.generate(2), m.put(g.generate(1), g.generate(3)));
    Entry<SpecialInteger, SpecialInteger> e = it.next();
    Assert.assertEquals(g.generate(1), e.getKey());

    Assert.assertThat(e.getValue(), anyOf(equalTo(g.generate(2)), equalTo(g.generate(3))));
  }

  private static void checkContainsSelf(Collection<SpecialInteger> c) {
    Assert.assertTrue(c.containsAll(c));
    Assert.assertTrue(c.containsAll(c));
    Assert.assertTrue(c.containsAll(Arrays.asList(c.toArray())));
    Assert.assertTrue(c.containsAll(Arrays.asList(c.toArray(new SpecialInteger[0]))));
  }

  private static void checkContainsEmpty(Collection<SpecialInteger> c) {
    Assert.assertTrue(c.containsAll(new ArrayList<SpecialInteger>()));
  }

  private static <T> void testEmptyCollection(Collection<T> c) {
    Assert.assertTrue(c.isEmpty());
    Assert.assertEquals(c.size(), 0);
    Assert.assertEquals(c.toString(), "[]");
    Assert.assertEquals(c.toArray().length, 0);
    Assert.assertEquals(c.toArray(new Object[0]).length, 0);
    Assert.assertNull(c.toArray(new Object[]{42})[0]);

    Object[] a = new Object[1];
    a[0] = Boolean.TRUE;
    Assert.assertSame(c.toArray(a), a);
    Assert.assertNull(a[0]);
    testEmptyIterator(c.iterator());
  }

  private static <T> void testEmptyIterator(final Iterator<T> it) {
    if (rnd.nextBoolean()) {
      Assert.assertFalse(it.hasNext());
    }

    THROWS(NoSuchElementException.class,
            new Fun() {

              @Override
              void f() {
                it.next();
              }
            });

    try {
      it.remove();
    } catch (IllegalStateException _) {
    } catch (UnsupportedOperationException _) {
    }

    if (rnd.nextBoolean()) {
      Assert.assertFalse(it.hasNext());
    }
  }

  private static <T> void testEmptySet(Set<T> c) {
    testEmptyCollection(c);
    Assert.assertEquals(c.hashCode(), 0);
    Assert.assertEquals(c, Collections.<Integer>emptySet());
    Assert.assertEquals(Collections.<Integer>emptySet(), c);
  }

  private static <K, V> void testEmptyMap(final Map<K, V> m) {
    Assert.assertTrue(m.isEmpty());
    Assert.assertEquals(0, m.size());
    Assert.assertEquals("{}", m.toString());
    testEmptySet(m.keySet());
    testEmptySet(m.entrySet());
    testEmptyCollection(m.values());

    try {
      Assert.assertFalse(m.containsValue(null));
    } catch (NullPointerException _) { /* OK */ }
    try {
      Assert.assertFalse(m.containsKey(null));
    } catch (NullPointerException _) { /* OK */ }
    Assert.assertFalse(m.containsValue(1));
    Assert.assertFalse(m.containsKey(1));
  }

  @SuppressWarnings("unused")
  private static void checkFunctionalInvariants(Collection<SpecialInteger> c) {
    checkContainsSelf(c);
    checkContainsEmpty(c);
    Assert.assertTrue(c.size() != 0 ^ c.isEmpty());

    {
      int size = 0;
      for (SpecialInteger i : c) {
        size++;
      }
      Assert.assertEquals(c.size(), size);
    }

    Assert.assertEquals(c.size(), c.toArray().length);
    Assert.assertSame(c.toArray().getClass(), Object[].class);
    for (int size : new int[]{0, 1, c.size(), c.size() + 1}) {
      SpecialInteger[] a = c.toArray(new SpecialInteger[size]);
      Assert.assertTrue((size > c.size()) || (a.length == c.size()));
      int i = 0;
      for (SpecialInteger j : c) {
        Assert.assertEquals(j, a[i++]);
      }
      Assert.assertTrue((size <= c.size()) || (a[c.size()] == null));
      Assert.assertSame(a.getClass(), SpecialInteger[].class);
    }

    Assert.assertEquals(c, c);;
    if (c instanceof Serializable) {
      try {
        Object clone = serialClone(c);
        Assert.assertEquals(c instanceof Serializable, clone instanceof Serializable);
        Assert.assertEquals(c instanceof RandomAccess, clone instanceof RandomAccess);
        if ((c instanceof List<?>) || (c instanceof Set<?>)) {
          Assert.assertEquals(c, clone);
        } else {
          Assert.assertEquals(new HashSet<SpecialInteger>(c), new HashSet<SpecialInteger>(serialClone(c)));
        }
      } catch (Error e) {
        if (!(e.getCause() instanceof NotSerializableException)) {
          throw e;
        }
      }
    }
  }

  //----------------------------------------------------------------
  // Map
  //----------------------------------------------------------------
  private static void checkFunctionalInvariants(Map<SpecialInteger, SpecialInteger> m) {
    Assert.assertEquals(m.keySet().size(), m.entrySet().size());
    Assert.assertEquals(m.size(), m.keySet().size());
    checkFunctionalInvariants(m.keySet());
    checkFunctionalInvariants(m.values());
    Assert.assertTrue(m.size() != 0 ^ m.isEmpty());
  }

  //----------------------------------------------------------------
  // ConcurrentMap
  //----------------------------------------------------------------
  public static void testConcurrentMap(Generator g, ConcurrentMap<SpecialInteger, SpecialInteger> m) {
    Assert.assertNull(m.putIfAbsent(g.generate(18357), g.generate(7346)));
    Assert.assertTrue(m.containsKey(g.generate(18357)));
    Assert.assertEquals(g.generate(7346), m.putIfAbsent(g.generate(18357), g.generate(8263)));
    try {
      m.putIfAbsent(g.generate(18357), null);
      Assert.fail("NPE");
    } catch (NullPointerException t) {
    }
    Assert.assertTrue(m.containsKey(g.generate(18357)));

    Assert.assertFalse(m.replace(g.generate(18357), g.generate(8888), g.generate(7777)));
    Assert.assertTrue(m.containsKey(g.generate(18357)));
    try {
      m.replace(g.generate(18357), null, g.generate(7777));
      Assert.fail("NPE");
    } catch (NullPointerException t) {
    }
    Assert.assertTrue(m.containsKey(g.generate(18357)));
    Assert.assertEquals(g.generate(7346), m.get(g.generate(18357)));
    Assert.assertTrue(m.replace(g.generate(18357), g.generate(7346), g.generate(5555)));
    Assert.assertTrue(m.replace(g.generate(18357), g.generate(5555), g.generate(7346)));
    Assert.assertEquals(g.generate(7346), m.get(g.generate(18357)));

    Assert.assertNull(m.replace(g.generate(92347), g.generate(7834)));
    try {
      m.replace(g.generate(18357), null);
      Assert.fail("NPE");
    } catch (NullPointerException t) {
    }
    Assert.assertEquals(g.generate(7346), m.replace(g.generate(18357), g.generate(7346)));
    Assert.assertEquals(g.generate(7346), m.replace(g.generate(18357), g.generate(5555)));
    Assert.assertEquals(g.generate(5555), m.get(g.generate(18357)));
    Assert.assertEquals(g.generate(5555), m.replace(g.generate(18357), g.generate(7346)));
    Assert.assertEquals(g.generate(7346), m.get(g.generate(18357)));

    Assert.assertFalse(m.remove(g.generate(18357), g.generate(9999)));
    Assert.assertEquals(g.generate(7346), m.get(g.generate(18357)));
    Assert.assertTrue(m.containsKey(g.generate(18357)));
    Assert.assertFalse(m.remove(g.generate(18357), null)); // 6272521
    Assert.assertEquals(g.generate(7346), m.get(g.generate(18357)));
    Assert.assertTrue(m.remove(g.generate(18357), g.generate(7346)));
    Assert.assertNull(m.get(g.generate(18357)));
    Assert.assertFalse(m.containsKey(g.generate(18357)));
    Assert.assertTrue(m.isEmpty());

    m.putIfAbsent(g.generate(1), g.generate(2));
    Assert.assertEquals(1, m.size());
    Assert.assertFalse(m.remove(g.generate(1), null));
    Assert.assertFalse(m.remove(g.generate(1), null));
    Assert.assertFalse(m.remove(g.generate(1), g.generate(1)));
    Assert.assertTrue(m.remove(g.generate(1), g.generate(2)));
    Assert.assertTrue(m.isEmpty());

    testEmptyMap(m);
  }

  public static void testConcurrentIterationAndMutation(final Generator g, final ConcurrentMap<SpecialInteger, SpecialInteger> m) {
    final AtomicBoolean stopped = new AtomicBoolean(false);
    Thread mutator = new Thread() {

      @Override
      public void run() {
        Random rndm = new Random();
        while (!stopped.get()) {
          int v = rndm.nextInt(8192);
          if (rndm.nextBoolean()) {
            m.remove(g.generate(v));
          } else {
            m.put(g.generate(v), g.generate(v));
          }
        }
      }
    };
    
    mutator.start();
    
    boolean interrupted = false;
    try {
      for (int i = 0; i < 10; i++) {
        int checked = 0;
        for (Map.Entry<SpecialInteger, SpecialInteger> e : m.entrySet()) {
          Assert.assertEquals(e.getKey(), e.getValue());
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
  }
  
  private static void throwsConsistently(Class<? extends Throwable> k,
          Iterable<Fun> fs) {
    List<Class<? extends Throwable>> threw = new ArrayList<Class<? extends Throwable>>();
    for (Fun f : fs) {
      try {
        f.f();
        threw.add(null);
      } catch (Throwable t) {
        Assert.assertTrue(k.isAssignableFrom(t.getClass()));
        threw.add(t.getClass());
      }
    }
    if (new HashSet<Object>(threw).size() != 1) {
      Assert.fail(threw.toString());
    }
  }

  private static <T> void checkNPEConsistency(final Generator g, final Map<T, SpecialInteger> m) {
    m.clear();
    final ConcurrentMap<T, SpecialInteger> cm = (m instanceof ConcurrentMap<?, ?>)
            ? (ConcurrentMap<T, SpecialInteger>) m
            : null;
    List<Fun> fs = new ArrayList<Fun>();
    fs.add(new Fun() {

      @Override
      void f() {
        Assert.assertFalse(m.containsKey(null));
      }
    });
    fs.add(new Fun() {

      @Override
      void f() {
        Assert.assertNull(m.remove(null));
      }
    });
    fs.add(new Fun() {

      @Override
      void f() {
        Assert.assertNull(m.get(null));
      }
    });
    if (cm != null) {
      fs.add(new Fun() {

        @Override
        void f() {
          Assert.assertFalse(cm.remove(null, null));
        }
      });
    }
    throwsConsistently(NullPointerException.class, fs);

    fs.clear();
    final Map<T, SpecialInteger> sm = Collections.singletonMap(null, g.generate(1));
    fs.add(new Fun() {

      @Override
      void f() {
        Assert.assertNull(m.put(null, g.generate(1)));
        m.clear();
      }
    });
    fs.add(new Fun() {

      @Override
      void f() {
        m.putAll(sm);
        m.clear();
      }
    });
    if (cm != null) {
      fs.add(new Fun() {

        @Override
        void f() {
          Assert.assertFalse(cm.remove(null, null));
        }
      });
      fs.add(new Fun() {

        @Override
        void f() {
          Assert.assertEquals(1, cm.putIfAbsent(null, g.generate(1)));
        }
      });
      fs.add(new Fun() {

        @Override
        void f() {
          Assert.assertNull(cm.replace(null, g.generate(1)));
        }
      });
      fs.add(new Fun() {

        @Override
        void f() {
          Assert.assertEquals(1, cm.replace(null, g.generate(1), g.generate(1)));
        }
      });
    }
    throwsConsistently(NullPointerException.class, fs);
  }

  private static final Random rnd = new Random();

  //--------------------- Infrastructure ---------------------------
  static abstract class Fun {

    abstract void f() throws Throwable;
  }

  private static void THROWS(Class<? extends Throwable> k, Fun... fs) {
    for (Fun f : fs) {
      try {
        f.f();
        Assert.fail("Expected " + k.getName() + " not thrown");
      } catch (Throwable t) {
        if (!k.isAssignableFrom(t.getClass())) {
          throw new AssertionError(t);
        }
      }
    }
  }

  private static byte[] serializedForm(Object obj) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      new ObjectOutputStream(baos).writeObject(obj);
      return baos.toByteArray();
    } catch (IOException e) {
      throw new Error(e);
    }
  }

  private static Object readObject(byte[] bytes)
          throws IOException, ClassNotFoundException {
    InputStream is = new ByteArrayInputStream(bytes);
    return new ObjectInputStream(is).readObject();
  }

  @SuppressWarnings("unchecked")
  private static <T> T serialClone(T obj) {
    try {
      return (T) readObject(serializedForm(obj));
    } catch (Exception e) {
      throw new Error(e);
    }
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
}
