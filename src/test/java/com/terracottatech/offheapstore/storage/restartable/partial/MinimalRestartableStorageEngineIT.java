package com.terracottatech.offheapstore.storage.restartable.partial;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.Tuple;

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
import com.terracottatech.offheapstore.buffersource.OffHeapBufferSource;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.paging.UpfrontAllocatingPageSource;
import com.terracottatech.offheapstore.storage.StorageEngine;
import com.terracottatech.offheapstore.storage.portability.StringPortability;
import com.terracottatech.offheapstore.storage.restartable.NoOpRestartStore;
import com.terracottatech.offheapstore.storage.restartable.RestartableStorageEngine;
import com.terracottatech.offheapstore.util.MemoryUnit;
import com.terracottatech.offheapstore.util.NoOpLock;
import com.terracottatech.offheapstore.util.PointerSizeParameterizedTest;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;

import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Ignore;

import static com.terracottatech.offheapstore.storage.restartable.RestartableStorageEngine.NULL_ENCODING;
import static com.terracottatech.offheapstore.storage.restartable.RestartableStorageEngine.extractHashcode;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MinimalRestartableStorageEngineIT extends PointerSizeParameterizedTest {

  @Test
  public void testSingleElement() {
    RestartStore<String, ByteBuffer, ByteBuffer> frs = mock(RestartStore.class);
    when(frs.beginTransaction(true)).thenReturn(new NoOpRestartStore.NoOpTransaction<String, ByteBuffer, ByteBuffer>());
      
    RestartableMinimalStorageEngine<?, String, String> engine = createEngine(frs);
    engine.bind(new TrackingOwner());
    try {
      Long encoding = engine.writeMapping("foo", "bar", "foo".hashCode(), 0);
      when(frs.get(1)).thenReturn(tupleOf(null, "foo", "bar", encoding));
      
      assertThat(encoding, notNullValue());
      assertThat(engine.getDataSize(), is(12L));
              
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      engine.assignLsn(encoding, 1);
      
      assertThat(engine.firstEncoding(), is(encoding));
      assertThat(engine.lastEncoding(), is(encoding));
      
      engine.freeMapping(encoding, 0, true);
      assertThat(engine.getDataSize(), is(0L));
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
    } finally {
      engine.destroy();
    }
  }
  
  @Test
  public void testOrderedTwoElementChain() {
    RestartStore<String, ByteBuffer, ByteBuffer> frs = mock(RestartStore.class);
    when(frs.beginTransaction(true)).thenReturn(new NoOpRestartStore.NoOpTransaction<String, ByteBuffer, ByteBuffer>());
    
    RestartableMinimalStorageEngine<?, String, String> engine = createEngine(frs);
    engine.bind(new TrackingOwner());
    try {
      Long encoding1 = engine.writeMapping("foo", "bar", "foo".hashCode(), 0);
      assertThat(encoding1, notNullValue());
      assertThat(engine.getDataSize(), is(12L));

      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      Long encoding2 = engine.writeMapping("alice", "bob", "alice".hashCode(), 0);
      assertThat(encoding2, notNullValue());
      assertThat(engine.getDataSize(), is(28L));
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      engine.assignLsn(encoding1, 1);
      when(frs.get(1)).thenReturn(tupleOf(null, "foo", "bar", encoding1));
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding1));
      
      engine.assignLsn(encoding2, 2);
      when(frs.get(2)).thenReturn(tupleOf(null, "alice", "bob", encoding2));
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding2));
      
      engine.freeMapping(encoding1, 0, true);
      
      assertThat(engine.getDataSize(), is(16L));
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding2));
      
      engine.freeMapping(encoding2, 0, true);

      assertThat(engine.getDataSize(), is(0L));
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
    } finally {
      engine.destroy();
    }
  }
  
  @Test
  public void testUnorderedTwoElementChain() {
    RestartStore<String, ByteBuffer, ByteBuffer> frs = mock(RestartStore.class);
    when(frs.beginTransaction(true)).thenReturn(new NoOpRestartStore.NoOpTransaction<String, ByteBuffer, ByteBuffer>());
    
    RestartableMinimalStorageEngine<?, String, String> engine = createEngine(frs);
    engine.bind(new TrackingOwner());
    try {
      Long encoding1 = engine.writeMapping("foo", "bar", "foo".hashCode(), 0);
      assertThat(encoding1, notNullValue());
      assertThat(engine.getDataSize(), is(12L));

      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      Long encoding2 = engine.writeMapping("alice", "bob", "alice".hashCode(), 0);
      assertThat(encoding2, notNullValue());
      assertThat(engine.getDataSize(), is(28L));
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      engine.assignLsn(encoding1, 2);
      when(frs.get(2)).thenReturn(tupleOf(null, "foo", "bar", encoding1));
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding1));
      
      engine.assignLsn(encoding2, 1);
      when(frs.get(1)).thenReturn(tupleOf(null, "alice", "bob", encoding2));
      
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding1));
      
      engine.freeMapping(encoding1, 0, true);
      
      assertThat(engine.getDataSize(), is(16L));
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding2));
      
      engine.freeMapping(encoding2, 0, true);
      
      assertThat(engine.getDataSize(), is(0L));
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
    } finally {
      engine.destroy();
    }
  }

  @Test
  public void testReorderedTwoElementChain() {
    RestartStore<String, ByteBuffer, ByteBuffer> frs = mock(RestartStore.class);
    when(frs.beginTransaction(true)).thenReturn(new NoOpRestartStore.NoOpTransaction<String, ByteBuffer, ByteBuffer>());
    
    RestartableMinimalStorageEngine<?, String, String> engine = createEngine(frs);
    engine.bind(new TrackingOwner());
    try {
      Long encoding1 = engine.writeMapping("foo", "bar", "foo".hashCode(), 0);
      assertThat(encoding1, notNullValue());
      assertThat(engine.getDataSize(), is(12L));

      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      Long encoding2 = engine.writeMapping("alice", "bob", "alice".hashCode(), 0);
      assertThat(encoding2, notNullValue());
      assertThat(engine.getDataSize(), is(28L));
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      engine.assignLsn(encoding1, 1);
      when(frs.get(1)).thenReturn(tupleOf(null, "foo", "bar", encoding1));
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding1));
      
      engine.assignLsn(encoding2, 2);
      when(frs.get(2)).thenReturn(tupleOf(null, "alice", "bar", encoding2));
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding2));
      
      engine.assignLsn(encoding1, 3);
      when(frs.get(3)).thenReturn(tupleOf(null, "baz", "boo", encoding2));
      
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding1));
      
      engine.freeMapping(encoding1, 0, true);

      assertThat(engine.getDataSize(), is(16L));
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding2));
      
      engine.freeMapping(encoding2, 0, true);
      
      assertThat(engine.getDataSize(), is(0L));
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
    } finally {
      engine.destroy();
    }
  }
  

  @Test
  public void testOrderedThreeElementChain() {
    RestartStore<String, ByteBuffer, ByteBuffer> frs = mock(RestartStore.class);
    when(frs.beginTransaction(true)).thenReturn(new NoOpRestartStore.NoOpTransaction<String, ByteBuffer, ByteBuffer>());
    
    RestartableMinimalStorageEngine<?, String, String> engine = createEngine(frs);
    TrackingOwner owner = new TrackingOwner();
    engine.bind(owner);
    try {
      Long encoding1 = engine.writeMapping("foo", "bar", "foo".hashCode(), 0);
      assertThat(encoding1, notNullValue());
      assertThat(engine.getDataSize(), is(12L));
      owner.addEncoding("foo", encoding1);
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      Long encoding2 = engine.writeMapping("alice", "bob", "alice".hashCode(), 0);
      assertThat(encoding2, notNullValue());
      assertThat(engine.getDataSize(), is(28L));
      owner.addEncoding("alice", encoding2);
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      Long encoding3 = engine.writeMapping("han", "yoda", "han".hashCode(), 0);
      assertThat(encoding3, notNullValue());
      assertThat(engine.getDataSize(), is(42L));
      owner.addEncoding("han", encoding3);
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      engine.assignLsn(encoding1, 1);
      when(frs.get(1)).thenReturn(tupleOf(null, "foo", "bar", encoding1));
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding1));
      
      engine.assignLsn(encoding2, 2);
      when(frs.get(2)).thenReturn(tupleOf(null, "alice", "bob", encoding2));
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding2));
      
      engine.assignLsn(encoding3, 3);
      when(frs.get(3)).thenReturn(tupleOf(null, "han", "yoda", encoding3));
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding3));
      
      engine.freeMapping(encoding1, 0, true);
      assertThat(engine.getDataSize(), is(30L));
      
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding3));
      
      engine.freeMapping(encoding2, 0, true);
      
      assertThat(engine.getDataSize(), is(14L));
      assertThat(engine.firstEncoding(), is(owner.getEncoding("han")));
      assertThat(engine.lastEncoding(), is(owner.getEncoding("han")));
      
      engine.freeMapping(owner.getEncoding("han"), 0, true);
      
      assertThat(engine.getDataSize(), is(0L));
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
    } finally {
      engine.destroy();
    }
  }
    
  @Test
  public void testUnorderedThreeElementChain() {
    RestartStore<String, ByteBuffer, ByteBuffer> frs = mock(RestartStore.class);
    when(frs.beginTransaction(true)).thenReturn(new NoOpRestartStore.NoOpTransaction<String, ByteBuffer, ByteBuffer>());
    
    RestartableMinimalStorageEngine<?, String, String> engine = createEngine(frs);
    TrackingOwner owner = new TrackingOwner();
    engine.bind(owner);
    try {
      Long encoding1 = engine.writeMapping("foo", "bar", "foo".hashCode(), 0);
      assertThat(encoding1, notNullValue());
      assertThat(engine.getDataSize(), is(12L));
      owner.addEncoding("foo", encoding1);
              
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      Long encoding2 = engine.writeMapping("alice", "bob", "alice".hashCode(), 0);
      assertThat(encoding2, notNullValue());
      assertThat(engine.getDataSize(), is(28L));
      owner.addEncoding("alice", encoding2);
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      Long encoding3 = engine.writeMapping("han", "yoda", "han".hashCode(), 0);
      assertThat(encoding3, notNullValue());
      assertThat(engine.getDataSize(), is(42L));
      owner.addEncoding("han", encoding3);
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      engine.assignLsn(encoding1, 3);
      when(frs.get(3)).thenReturn(tupleOf(null, "foo", "bar", encoding1));
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding1));
      
      engine.assignLsn(encoding2, 1);
      when(frs.get(1)).thenReturn(tupleOf(null, "alice", "bob", encoding2));
      
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding1));
      
      engine.assignLsn(encoding3, 2);
      when(frs.get(2)).thenReturn(tupleOf(null, "han", "yoda", encoding3));
      
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding1));
      
      engine.freeMapping(encoding1, 0, true);
      
      assertThat(engine.getDataSize(), is(30L));
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding3));
      
      engine.freeMapping(encoding2, 0, true);
      
      assertThat(engine.getDataSize(), is(14L));
      assertThat(engine.firstEncoding(), is(owner.getEncoding("han")));
      assertThat(engine.lastEncoding(), is(owner.getEncoding("han")));
      
      engine.freeMapping(owner.getEncoding("han"), 0, true);
      
      assertThat(engine.getDataSize(), is(0L));
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
    } finally {
      engine.destroy();
    }
  }
  
  @Test
  public void testReorderedThreeElementChain() {
    RestartStore<String, ByteBuffer, ByteBuffer> frs = mock(RestartStore.class);
    when(frs.beginTransaction(true)).thenReturn(new NoOpRestartStore.NoOpTransaction<String, ByteBuffer, ByteBuffer>());
    
    RestartableMinimalStorageEngine<?, String, String> engine = createEngine(frs);
    TrackingOwner owner = new TrackingOwner();
    engine.bind(owner);
    try {
      Long encoding1 = engine.writeMapping("foo", "bar", "foo".hashCode(), 0);
      assertThat(encoding1, notNullValue());
      assertThat(engine.getDataSize(), is(12L));
      owner.addEncoding("foo", encoding1);

      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      Long encoding2 = engine.writeMapping("alice", "bob", "alice".hashCode(), 0);
      assertThat(encoding2, notNullValue());
      assertThat(engine.getDataSize(), is(28L));
      owner.addEncoding("alice", encoding2);
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      Long encoding3 = engine.writeMapping("han", "yoda", "han".hashCode(), 0);
      assertThat(encoding3, notNullValue());
      assertThat(engine.getDataSize(), is(42L));
      owner.addEncoding("han", encoding3);
      
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
      
      engine.assignLsn(encoding1, 1);
      when(frs.get(1)).thenReturn(tupleOf(null, "foo", "bar", encoding1));
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding1));
      
      engine.assignLsn(encoding2, 2);
      when(frs.get(2)).thenReturn(tupleOf(null, "alice", "bob", encoding2));
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding2));
      
      engine.assignLsn(encoding3, 3);
      when(frs.get(3)).thenReturn(tupleOf(null, "han", "yoda", encoding3));
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding3));

      engine.assignLsn(encoding1, 4);
      when(frs.get(4)).thenReturn(tupleOf(null, "foo", "bar", encoding1));
      
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding1));
      
      engine.assignLsn(encoding3, 6);
      when(frs.get(6)).thenReturn(tupleOf(null, "han", "yoda", encoding3));
      
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding3));
      
      engine.assignLsn(encoding2, 5);
      when(frs.get(5)).thenReturn(tupleOf(null, "alice", "bob", encoding2));
      
      assertThat(engine.firstEncoding(), is(encoding1));
      assertThat(engine.lastEncoding(), is(encoding3));
      
      engine.freeMapping(encoding1, 0, true);
      
      assertThat(engine.getDataSize(), is(30L));
      assertThat(engine.firstEncoding(), is(encoding2));
      assertThat(engine.lastEncoding(), is(encoding3));
      
      engine.freeMapping(encoding2, 0, true);
      
      assertThat(engine.getDataSize(), is(14L));
      assertThat(engine.firstEncoding(), is(owner.getEncoding("han")));
      assertThat(engine.lastEncoding(), is(owner.getEncoding("han")));
      
      engine.freeMapping(owner.getEncoding("han"), 0, true);
      
      assertThat(engine.getDataSize(), is(0L));
      assertThat(engine.firstEncoding(), is(NULL_ENCODING));
      assertThat(engine.lastEncoding(), is(NULL_ENCODING));
    } finally {
      engine.destroy();
    }
  }
  
  @Test
  public void testAcquireAndUpdateLsn() throws Exception {
    RestartStore<String, ByteBuffer, ByteBuffer> frs = mock(RestartStore.class);
    when(frs.beginTransaction(true)).thenReturn(new NoOpRestartStore.NoOpTransaction<String, ByteBuffer, ByteBuffer>());
    
    RestartableMinimalStorageEngine<String, String, String> engine = createEngine(frs);
    engine.bind(new TrackingOwner());
    try {
      Long encoding1 = engine.writeMapping("foo", "bar", "foo".hashCode(), 0);
      assertThat(engine.getDataSize(), is(12L));
      engine.assignLsn(encoding1, 1);
      when(frs.get(1)).thenReturn(tupleOf(null, "foo", "bar", encoding1));
      Long encoding2 = engine.writeMapping("baz", "boo", "baz".hashCode(), 0);
      assertThat(engine.getDataSize(), is(24L));
      engine.assignLsn(encoding2, 2);
      when(frs.get(2)).thenReturn(tupleOf(null, "baz", "boo", encoding2));

      assertThat(engine.getLowestLsn(), is(1L));

      ObjectManagerEntry<String, ByteBuffer, ByteBuffer>
              entry = engine.acquireCompactionEntry(Long.MAX_VALUE);
      assertThat(entry.getLsn(), is(1L));
      engine.updateLsn(extractHashcode(entry.getKey()), entry, 3L);
      when(frs.get(3)).thenReturn(tupleOf(null, "foo", "bar", encoding1));
      when(frs.get(1)).thenReturn(null);
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
      assertThat(engine.getDataSize(), is(12L));
      engine.freeMapping(encoding1, 0, true);
      assertThat(engine.getDataSize(), is(0L));

      entry = engine.acquireCompactionEntry(Long.MAX_VALUE);
      assertThat(entry, nullValue());

      Long encoding3 = engine.writeMapping("a", "b", "a".hashCode(), 0);
      assertThat(engine.getDataSize(), is(4L));
      engine.assignLsn(encoding3, 3L);
      when(frs.get(3)).thenReturn(tupleOf(null, "a", "b", encoding3));

      entry = engine.acquireCompactionEntry(3L);
      assertThat(entry, nullValue());

    } finally {
      engine.destroy();
    }
  }

  @Test
  public void testFlushFaultBehavior() throws Exception {
    final int KEY_COUNT = 2048;
    
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < KEY_COUNT; i++) {
      sb.append(Integer.toString(i));
    }
    final String value = sb.toString();

    RestartStore<String, ByteBuffer, ByteBuffer> frs = mock(RestartStore.class);
    when(frs.beginTransaction(true)).thenReturn(new NoOpRestartStore.NoOpTransaction<String, ByteBuffer, ByteBuffer>());
    
    RestartableMinimalStorageEngine<String, String, String> engine = createEngine(frs);
    TrackingOwner<String> owner = new TrackingOwner<String>();
    engine.bind(owner);
    try {
      
      for (int i = 0; i < KEY_COUNT; i++) {
        String key = Integer.toString(i);
        long encoding = engine.writeMapping(key, value.substring(i), key.hashCode(), 0);
        engine.attachedMapping(encoding, key.hashCode(), 0);
        engine.assignLsn(encoding, i);
        owner.addEncoding(key, encoding);
        when(frs.get(i)).thenReturn(tupleOf("id", key, value.substring(i), encoding));
      }
      Assert.assertThat(engine.getDataSize(), greaterThan(engine.getOccupiedMemory()));
      
      long seed = System.currentTimeMillis();
      System.out.println("seed = " + seed);
      Random rndm = new Random(seed);
      for (int i = 0; i < 2 * KEY_COUNT; i++) {
        int index = rndm.nextInt(KEY_COUNT);
        String key = Integer.toString(index);
        String readValue = engine.readValue(owner.getEncoding(key));
        Assert.assertThat(readValue, IsEqual.equalTo(value.substring(index)));
      }
    } finally {
      engine.destroy();
    }
  }
  
  @Test
  public void testMultiThreadFlushFaultBehavior() throws Exception {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < MemoryUnit.KILOBYTES.toBytes(768 / 2); i++) {
      sb.append('1');
    }
    final String value = sb.toString();

    RestartStore<String, ByteBuffer, ByteBuffer> frs = mock(RestartStore.class);
    when(frs.beginTransaction(true)).thenReturn(new NoOpRestartStore.NoOpTransaction<String, ByteBuffer, ByteBuffer>());
    final RestartableMinimalStorageEngine<String, String, String> engine = createEngine(frs);
    final TrackingOwner<String> owner = new TrackingOwner<String>();
    engine.bind(owner);
    try {
      for (int i = 0; i < 5; i++) {
        String key = Integer.toString(i);
        long encoding = engine.writeMapping(key, value, key.hashCode(), 0);
        engine.attachedMapping(encoding, key.hashCode(), 0);
        engine.assignLsn(encoding, i);
        owner.addEncoding(key, encoding);
        when(frs.get(i)).thenReturn(tupleOf("id", key, value, encoding));
      }
      Assert.assertThat(engine.getDataSize(), greaterThan(engine.getOccupiedMemory()));

      ExecutorService executor = Executors.newFixedThreadPool(4);
      try {
        for (Future f : executor.invokeAll(Collections.nCopies(4, new Callable<Void>() {

          @Override
          public Void call() throws Exception {
            for (int i = 0; i < 20; i++) {
              for (int j = 0; j < 5; j++) {
                String key = Integer.toString(j);
                String readValue = engine.readValue(owner.getEncoding(key));
                Assert.assertThat(readValue, IsEqual.equalTo(value));
              }
            }
            return null;
          }
        }))) {
          f.get();
        }
      } finally {
        executor.shutdown();
      }
    } finally {
      engine.destroy();
    }
  }
  
  @Test
  @Ignore
  public void testRandomMutations() {
    long seed = 1357148530430361000L;System.nanoTime();
    System.err.println(getClass().getSimpleName() + ".testRandomMutations : seed=" + seed);
    Random rndm = new Random(seed);

    final SortedMap<Long, Long> lsnReference = new TreeMap<Long, Long>();
    final Set<Long> unassignedEncodings = new HashSet<Long>();
    
    RestartableMinimalStorageEngine<?, String, String> engine = createEngine(new NoOpRestartStore());
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

  private static void storeRandom(Random rndm, RestartableMinimalStorageEngine<?, String, String> engine, Set<Long> unassignedEncodings, SortedMap<Long, Long> lsnReference) {
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
  
  private static void freeRandom(Random rndm, RestartableMinimalStorageEngine<?, String, String> engine, Set<Long> unassignedEncodings, SortedMap<Long, Long> lsnReference) {
    if (lsnReference.isEmpty()) {
      assignRandom(rndm, engine, unassignedEncodings, lsnReference);
    } else {
      Entry<Long, Long> assigned = randomEntry(rndm, lsnReference);
      engine.freeMapping(assigned.getValue(), 0, true);
      lsnReference.remove(assigned.getKey());
    }
  }
  
  private static void assignRandom(Random rndm, RestartableMinimalStorageEngine<?, String, String> engine, Set<Long> unassignedEncodings, SortedMap<Long, Long> lsnReference) {
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
  
  protected RestartableMinimalStorageEngine<String, String, String> createEngine(RestartStore<String, ByteBuffer, ByteBuffer> frs) {
    PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), MemoryUnit.MEGABYTES.toBytes(1), MemoryUnit.MEGABYTES.toBytes(1));
    return new RestartableMinimalStorageEngine<String, String, String>("id", frs, true, getPointerSize(), source, MemoryUnit.KILOBYTES.toBytes(1), StringPortability.INSTANCE, StringPortability.INSTANCE, 0.0f);
  }

  private static Tuple<String, ByteBuffer, ByteBuffer> tupleOf(final String identifier, final String key, final String value, final long encoding) {
    return new Tuple<String, ByteBuffer, ByteBuffer>() {

      @Override
      public String getIdentifier() {
        return identifier;
      }

      @Override
      public ByteBuffer getKey() {
        return RestartableStorageEngine.encodeKey(StringPortability.INSTANCE.encode(key), key.hashCode());
      }

      @Override
      public ByteBuffer getValue() {
        return RestartableStorageEngine.encodeValue(StringPortability.INSTANCE.encode(value), encoding, 0);
      }
    };
  }

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
      return true;
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
