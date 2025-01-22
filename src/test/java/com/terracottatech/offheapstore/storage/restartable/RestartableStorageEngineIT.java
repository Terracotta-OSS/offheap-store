package com.terracottatech.offheapstore.storage.restartable;

import static com.terracottatech.offheapstore.storage.restartable.RestartableStorageEngine.NULL_ENCODING;
import static com.terracottatech.offheapstore.storage.restartable.RestartableStorageEngine.extractHashcode;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Test;

import com.terracottatech.frs.object.ObjectManagerEntry;
import org.terracotta.offheapstore.storage.StorageEngine;
import org.terracotta.offheapstore.util.NoOpLock;
import com.terracottatech.offheapstore.util.PointerSizeEngineTypeParameterizedTest;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;

public abstract class RestartableStorageEngineIT extends PointerSizeEngineTypeParameterizedTest {

  @Test
  public void testSingleElement() {
    RestartableStorageEngine<?, ?, String, String> engine = createEngine();
    engine.bind(new TrackingOwner());
    try {
      Long encoding = engine.writeMapping("foo", "bar", "foo".hashCode(), 0);
      assertThat(encoding, notNullValue());
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      engine.assignLsn(encoding, 1);
      
      assertThat(engine.firstEncoding(), is(encoding));
      assertThat(engine.lastEncoding(), is(encoding));
      
      engine.freeMapping(encoding, 0, true);
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
    } finally {
      engine.destroy();
    }
  }
  
  @Test
  public void testOrderedTwoElementChain() {
    RestartableStorageEngine<?, ?, String, String> engine = createEngine();
    engine.bind(new TrackingOwner());
    try {
      Long encoding1 = engine.writeMapping("foo", "bar", "foo".hashCode(), 0);
      assertThat(encoding1, notNullValue());

      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      Long encoding2 = engine.writeMapping("alice", "bob", "alice".hashCode(), 0);
      assertThat(encoding2, notNullValue());
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      engine.assignLsn(encoding1, 1);
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding1));
      
      engine.assignLsn(encoding2, 2);
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding2));
      
      engine.freeMapping(encoding1, 0, true);
      
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding2));
      
      engine.freeMapping(encoding2, 0, true);
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
    } finally {
      engine.destroy();
    }
  }
  
  @Test
  public void testUnorderedTwoElementChain() {
    RestartableStorageEngine<?, ?, String, String> engine = createEngine();
    engine.bind(new TrackingOwner());
    try {
      Long encoding1 = engine.writeMapping("foo", "bar", "foo".hashCode(), 0);
      assertThat(encoding1, notNullValue());

      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      Long encoding2 = engine.writeMapping("alice", "bob", "alice".hashCode(), 0);
      assertThat(encoding2, notNullValue());
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      engine.assignLsn(encoding1, 2);
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding1));
      
      engine.assignLsn(encoding2, 1);
      
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding1));
      
      engine.freeMapping(encoding1, 0, true);
      
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding2));
      
      engine.freeMapping(encoding2, 0, true);
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
    } finally {
      engine.destroy();
    }
  }

  @Test
  public void testReorderedTwoElementChain() {
    RestartableStorageEngine<?, ?, String, String> engine = createEngine();
    engine.bind(new TrackingOwner());
    try {
      Long encoding1 = engine.writeMapping("foo", "bar", "foo".hashCode(), 0);
      assertThat(encoding1, notNullValue());

      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      Long encoding2 = engine.writeMapping("alice", "bob", "alice".hashCode(), 0);
      assertThat(encoding2, notNullValue());
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      engine.assignLsn(encoding1, 1);
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding1));
      
      engine.assignLsn(encoding2, 2);
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding2));
      
      engine.assignLsn(encoding1, 3);
      
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding1));
      
      engine.freeMapping(encoding1, 0, true);
      
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding2));
      
      engine.freeMapping(encoding2, 0, true);
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
    } finally {
      engine.destroy();
    }
  }
  

  @Test
  public void testOrderedThreeElementChain() {
    RestartableStorageEngine<?, ?, String, String> engine = createEngine();
    TrackingOwner owner = new TrackingOwner();
    engine.bind(owner);
    try {
      Long encoding1 = engine.writeMapping("foo", "bar", "foo".hashCode(), 0);
      assertThat(encoding1, notNullValue());
      owner.addEncoding("foo", encoding1);
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      Long encoding2 = engine.writeMapping("alice", "bob", "alice".hashCode(), 0);
      assertThat(encoding2, notNullValue());
      owner.addEncoding("alice", encoding2);
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      Long encoding3 = engine.writeMapping("han", "yoda", "han".hashCode(), 0);
      assertThat(encoding3, notNullValue());
      owner.addEncoding("han", encoding3);
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      engine.assignLsn(encoding1, 1);
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding1));
      
      engine.assignLsn(encoding2, 2);
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding2));
      
      engine.assignLsn(encoding3, 3);
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding3));
      
      engine.freeMapping(encoding1, 0, true);
      
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding3));
      
      engine.freeMapping(encoding2, 0, true);
      
      assertThat(engine.firstEncoding(), is(owner.getEncoding("han")));
      assertThat(engine.lastEncoding(), is(owner.getEncoding("han")));
      
      engine.freeMapping(owner.getEncoding("han"), 0, true);
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
    } finally {
      engine.destroy();
    }
  }
    
  @Test
  public void testUnorderedThreeElementChain() {
    RestartableStorageEngine<?, ?, String, String> engine = createEngine();
    TrackingOwner owner = new TrackingOwner();
    engine.bind(owner);
    try {
      Long encoding1 = engine.writeMapping("foo", "bar", "foo".hashCode(), 0);
      assertThat(encoding1, notNullValue());
      owner.addEncoding("foo", encoding1);
              
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      Long encoding2 = engine.writeMapping("alice", "bob", "alice".hashCode(), 0);
      assertThat(encoding2, notNullValue());
      owner.addEncoding("alice", encoding2);
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      Long encoding3 = engine.writeMapping("han", "yoda", "han".hashCode(), 0);
      assertThat(encoding3, notNullValue());
      owner.addEncoding("han", encoding3);
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      engine.assignLsn(encoding1, 3);
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding1));
      
      engine.assignLsn(encoding2, 1);
      
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding1));
      
      engine.assignLsn(encoding3, 2);
      
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding1));
      
      engine.freeMapping(encoding1, 0, true);
      
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding3));
      
      engine.freeMapping(encoding2, 0, true);
      
      assertThat(engine.firstEncoding(), is(owner.getEncoding("han")));
      assertThat(engine.lastEncoding(), is(owner.getEncoding("han")));
      
      engine.freeMapping(owner.getEncoding("han"), 0, true);
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
    } finally {
      engine.destroy();
    }
  }
  
  @Test
  public void testReorderedThreeElementChain() {
    RestartableStorageEngine<?, ?, String, String> engine = createEngine();
    TrackingOwner owner = new TrackingOwner();
    engine.bind(owner);
    try {
      Long encoding1 = engine.writeMapping("foo", "bar", "foo".hashCode(), 0);
      assertThat(encoding1, notNullValue());
      owner.addEncoding("foo", encoding1);

      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      Long encoding2 = engine.writeMapping("alice", "bob", "alice".hashCode(), 0);
      assertThat(encoding2, notNullValue());
      owner.addEncoding("alice", encoding2);
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      Long encoding3 = engine.writeMapping("han", "yoda", "han".hashCode(), 0);
      assertThat(encoding3, notNullValue());
      owner.addEncoding("han", encoding3);
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      engine.assignLsn(encoding1, 1);
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding1));
      
      engine.assignLsn(encoding2, 2);
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding2));
      
      engine.assignLsn(encoding3, 3);
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding3));

      engine.assignLsn(encoding1, 4);
      
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding1));
      
      engine.assignLsn(encoding3, 6);
      
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding3));
      
      engine.assignLsn(encoding2, 5);
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding3));
      
      engine.freeMapping(encoding1, 0, true);
      
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding3));
      
      engine.freeMapping(encoding2, 0, true);
      
      assertThat(engine.firstEncoding(), is(owner.getEncoding("han")));
      assertThat(engine.lastEncoding(), is(owner.getEncoding("han")));
      
      engine.freeMapping(owner.getEncoding("han"), 0, true);
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
    } finally {
      engine.destroy();
    }
  }

  @Test
  public void testAcquireAndUpdateLsn() throws Exception {
    RestartableStorageEngine<?, String, String, String> engine = createEngine();
    engine.bind(new TrackingOwner());
    try {
      Long encoding1 = engine.writeMapping("foo", "bar", "foo".hashCode(), 0);
      engine.assignLsn(encoding1, 1);
      Long encoding2 = engine.writeMapping("baz", "boo", "baz".hashCode(), 0);
      engine.assignLsn(encoding2, 2);

      assertThat(engine.getLowestLsn(), is(1L));

      ObjectManagerEntry<String, ByteBuffer, ByteBuffer>
              entry = engine.acquireCompactionEntry(Long.MAX_VALUE);
      assertThat(entry.getLsn(), is(1L));
      engine.updateLsn(extractHashcode(entry.getKey()), entry, 3L);
      engine.releaseCompactionEntry(entry);

      assertThat(engine.getLowestLsn(), is(2L));

      try {
        engine.updateLsn(extractHashcode(entry.getKey()), entry, 4L);
        fail();
      } catch (IllegalArgumentException e) {
        // expected
      }

      try {
        engine.releaseCompactionEntry(entry);
        fail();
      } catch (IllegalArgumentException e) {
        // expected
      }

      engine.freeMapping(encoding2, 0, true);
      engine.freeMapping(encoding1, 0, true);

      entry = engine.acquireCompactionEntry(Long.MAX_VALUE);
      assertThat(entry, nullValue());

      Long encoding3 = engine.writeMapping("a", "b", "a".hashCode(), 0);
      engine.assignLsn(encoding3, 3L);

      entry = engine.acquireCompactionEntry(3L);
      assertThat(entry, nullValue());

    } finally {
      engine.destroy();
    }
  }

  @Test
  public void testRandomMutations() {
    long seed = 1357148530430361000L;System.nanoTime();
    System.err.println(getClass().getSimpleName() + ".testRandomMutations : seed=" + seed);
    Random rndm = new Random(seed);

    final SortedMap<Long, Long> lsnReference = new TreeMap<Long, Long>();
    final Set<Long> unassignedEncodings = new HashSet<Long>();
    
    RestartableStorageEngine<?, ?, String, String> engine = createEngine();
    engine.bind(new StorageEngine.Owner() {

      @Override
      public Long getEncodingForHashAndBinary(int hash, ByteBuffer offHeapBinaryKey) {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public long getSize() {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public long installMappingForHashAndEncoding(int pojoHash, ByteBuffer offheapBinaryKey, ByteBuffer offheapBinaryValue, int metadata) {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public Iterable<Long> encodingSet() {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public boolean updateEncoding(int hashCode, long lastAddress, long compressed, long mask) {
        if (unassignedEncodings.remove(lastAddress)) {
          unassignedEncodings.add(compressed);
          return true;
        } else {
          for (Iterator<Entry<Long, Long>> it = lsnReference.entrySet().iterator(); it.hasNext(); ) {
            Entry<Long, Long> e = it.next();
            if (e.getValue() == lastAddress) {
              e.setValue(compressed);
              return true;
            }
          }
          return false;
        }
      }

      @Override
      public Integer getSlotForHashAndEncoding(int hash, long address, long mask) {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public boolean evict(int slot, boolean b) {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public boolean isThiefForTableAllocations() {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public Lock readLock() {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public Lock writeLock() {
        throw new UnsupportedOperationException("Not supported yet.");
      }
    });
    
    for (int i = 0; i < 100000; i++) {
      switch (rndm.nextInt(3)) {
        case 0:
          storeRandom(rndm, engine, unassignedEncodings, lsnReference);
          break;
        case 1:
          assignRandom(rndm, engine, unassignedEncodings, lsnReference);
          break;
        case 2:
          freeRandom(rndm, engine, unassignedEncodings, lsnReference);
          break;
      }
      
      if (lsnReference.isEmpty()) {
        assertThat(engine.firstEncoding(), is(NULL_ENCODING));
        assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      } else {
        assertThat(engine.firstEncoding(), is(lsnReference.get(lsnReference.firstKey())));
        assertThat(engine.lastEncoding(), is(lsnReference.get(lsnReference.lastKey())));
      }
    }
  }

  private static void storeRandom(Random rndm, RestartableStorageEngine<?, ?, String, String> engine, Set<Long> unassignedEncodings, SortedMap<Long, Long> lsnReference) {
    Long written = engine.writeMapping("foo", "bar", "foo".hashCode(), 0);
    if (written == null) {
      if (lsnReference.isEmpty()) {
        assignRandom(rndm, engine, unassignedEncodings, lsnReference);
      } else {
        freeRandom(rndm, engine, unassignedEncodings, lsnReference);
      }
    } else {
      unassignedEncodings.add(written);
    }
  }
  
  private static void freeRandom(Random rndm, RestartableStorageEngine<?, ?, String, String> engine, Set<Long> unassignedEncodings, SortedMap<Long, Long> lsnReference) {
    if (lsnReference.isEmpty()) {
      assignRandom(rndm, engine, unassignedEncodings, lsnReference);
    } else {
      Entry<Long, Long> assigned = randomEntry(rndm, lsnReference);
      engine.freeMapping(assigned.getValue(), 0, true);
      lsnReference.remove(assigned.getKey());
    }
  }
  
  private static void assignRandom(Random rndm, RestartableStorageEngine<?, ?, String, String> engine, Set<Long> unassignedEncodings, SortedMap<Long, Long> lsnReference) {
    if (rndm.nextBoolean() && !unassignedEncodings.isEmpty()) {
      Iterator<Long> it = unassignedEncodings.iterator();
      long unassignedEncoding = it.next();
      it.remove();
      long newLsn = randomUniqueLong(rndm, lsnReference.keySet());
      engine.assignLsn(unassignedEncoding, newLsn);
      lsnReference.put(newLsn, unassignedEncoding);
    } else if (!lsnReference.isEmpty()) {
      Entry<Long, Long> assigned = randomEntry(rndm, lsnReference);
      long newLsn = randomUniqueLong(rndm, lsnReference.keySet());
      engine.assignLsn(assigned.getValue(), newLsn);
      lsnReference.remove(assigned.getKey());
      lsnReference.put(newLsn, assigned.getValue());
    } else {
      storeRandom(rndm, engine, unassignedEncodings, lsnReference);
    }
  }
  
  private static Entry<Long, Long> randomEntry(Random rndm, SortedMap<Long, Long> map) {
    long target = Math.abs(rndm.nextLong() % (map.lastKey() + 1));
    return new AbstractMap.SimpleEntry<Long, Long>(map.tailMap(target).entrySet().iterator().next());
  }
  
  private static long randomUniqueLong(Random rndm, Collection<Long> not) {
    while (true) {
      long candidate = Math.abs(rndm.nextLong());
      if (candidate != Long.MIN_VALUE && !not.contains(candidate)) {
        return candidate;
      }
    }
  }
  
  protected abstract RestartableStorageEngine<?, String, String, String> createEngine();
  
  static class TrackingOwner<K> implements StorageEngine.Owner {

    private final Map<K, Long> encodings = new HashMap<K, Long>();
    
    public void addEncoding(K key, long encoding) {
      encodings.put(key, encoding);
    }
    
    public long getEncoding(K key) {
      return encodings.get(key);
    }

    @Override
    public Long getEncodingForHashAndBinary(int hash, ByteBuffer offHeapBinaryKey) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long getSize() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long installMappingForHashAndEncoding(int pojoHash, ByteBuffer offheapBinaryKey, ByteBuffer offheapBinaryValue, int metadata) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Iterable<Long> encodingSet() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean updateEncoding(int hashCode, long lastAddress, long compressed, long mask) {
      for (Iterator<Entry<K, Long>> it = encodings.entrySet().iterator(); it.hasNext();) {
        Entry<K, Long> e = it.next();
        if (e.getValue() == lastAddress) {
          e.setValue(compressed);
          return true;
        }
      }
      throw new AssertionError();
    }

    @Override
    public Integer getSlotForHashAndEncoding(int hash, long address, long mask) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean evict(int slot, boolean b) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isThiefForTableAllocations() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Lock readLock() {
      return NoOpLock.INSTANCE;
    }

    @Override
    public Lock writeLock() {
      return NoOpLock.INSTANCE;
    }
    
  }
}
