/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.eviction;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;

import org.junit.Assert;
import org.junit.Test;

import com.terracottatech.offheapstore.buffersource.OffHeapBufferSource;
import com.terracottatech.offheapstore.concurrent.ConcurrentOffHeapClockCache;
import com.terracottatech.offheapstore.concurrent.ConcurrentOffHeapHashMap;
import com.terracottatech.offheapstore.concurrent.ConcurrentWriteLockedOffHeapClockCache;
import com.terracottatech.offheapstore.exceptions.OversizeMappingException;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.paging.UpfrontAllocatingPageSource;
import com.terracottatech.offheapstore.storage.LongStorageEngine;
import com.terracottatech.offheapstore.storage.OffHeapBufferHalfStorageEngine;
import com.terracottatech.offheapstore.storage.portability.StringPortability;

/**
 *
 * @author Chris Dennis
 */
public class EvictionListenerIT {

  @Test
  public void testEvictionListenerReadWriteLocked() {
    MonitoringEvictionListener listener = new MonitoringEvictionListener();
    PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 16 * 4096, 2048);
    Map<Long, String> map = new ConcurrentOffHeapClockCache<Long, String>(source, LongStorageEngine.createFactory(OffHeapBufferHalfStorageEngine.createFactory(source, 128, StringPortability.INSTANCE)), listener);

    for (long i = 0; i < 2000L; i++) {
      map.put(Long.valueOf(i), Long.toString(i));
    }

    Set<Long> evictedKeys = listener.evictedKeys();
    Set<Long> presentKeys = map.keySet();

    for (long i = 0; i < 2000L; i++) {
      Assert.assertEquals("Key: " + i + " map:" + presentKeys.contains(i) + " evicted:" + evictedKeys.contains(i), presentKeys.contains(i), !evictedKeys.contains(i));
    }
  }

  @Test
  public void testEvictionListenerWriteLocked() {
    MonitoringEvictionListener listener = new MonitoringEvictionListener();
    PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 16 * 4096, 2048);
    Map<Long, String> map = new ConcurrentWriteLockedOffHeapClockCache<Long, String>(source, LongStorageEngine.createFactory(OffHeapBufferHalfStorageEngine.createFactory(source, 128, StringPortability.INSTANCE)), listener);

    for (long i = 0; i < 2000L; i++) {
      map.put(Long.valueOf(i), Long.toString(i));
    }

    Set<Long> evictedKeys = listener.evictedKeys();
    Set<Long> presentKeys = map.keySet();

    for (long i = 0; i < 2000L; i++) {
      Assert.assertEquals("Key: " + i + " map:" + presentKeys.contains(i) + " evicted:" + evictedKeys.contains(i), presentKeys.contains(i), !evictedKeys.contains(i));
    }
  }

  @Test
  public void testEvictionListenerThatThrowsReadWriteLocked() {
    ThrowingEvictionListener listener = new ThrowingEvictionListener();
    PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 16 * 4096, 2048);
    Map<Long, String> map = new ConcurrentOffHeapClockCache<Long, String>(source, LongStorageEngine.createFactory(OffHeapBufferHalfStorageEngine.createFactory(source, 128, StringPortability.INSTANCE)), listener);

    for (long i = 0; i < 2000L; i++) {
      try {
        map.put(Long.valueOf(i), Long.toString(i));
      } catch (NullPointerException e) {
        //ignore
      }
    }

    Set<Long> evictedKeys = listener.evictedKeys();

    for (Long l : evictedKeys) {
      Assert.assertFalse(map.containsKey(l));
    }
  }

  @Test
  public void testEvictionListenerThatThrowsWriteLocked() {
    ThrowingEvictionListener listener = new ThrowingEvictionListener();
    PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 16 * 4096, 2048);
    Map<Long, String> map = new ConcurrentWriteLockedOffHeapClockCache<Long, String>(source, LongStorageEngine.createFactory(OffHeapBufferHalfStorageEngine.createFactory(source, 128, StringPortability.INSTANCE)), listener);

    for (long i = 0; i < 2000L; i++) {
      try {
        map.put(Long.valueOf(i), Long.toString(i));
      } catch (NullPointerException e) {
        //ignore
      }
    }

    Set<Long> evictedKeys = listener.evictedKeys();

    for (Long l : evictedKeys) {
      Assert.assertFalse(map.containsKey(l));
    }
  }

  @Test
  public void testEvictionListenerSeesStealingEventsReadWriteLocked() {
    MonitoringEvictionListener listener = new MonitoringEvictionListener();
    PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 16 * 4096, 16 * 4096);
    Map<Long, String> victim = new ConcurrentOffHeapClockCache<Long, String>(source, LongStorageEngine.createFactory(OffHeapBufferHalfStorageEngine.createFactory(source, 128, StringPortability.INSTANCE, false, true)), listener);

    long i = 0;
    while (listener.evictedKeys().isEmpty()) {
      victim.put(Long.valueOf(i), Long.toString(i));
      i++;
    }
    listener.evictedKeys().clear();
    long victimSize = victim.size();

    Map<Long, String> thief = new ConcurrentOffHeapHashMap<Long, String>(source, true, LongStorageEngine.createFactory(OffHeapBufferHalfStorageEngine.createFactory(source, 128, StringPortability.INSTANCE, true, false)));

    try {
      i = 0;
      while (true) {
        thief.put(Long.valueOf(i), Long.toString(i));
        i++;
      }
    } catch (OversizeMappingException e) {
      //ignore
    }

    Assert.assertFalse(listener.evictedKeys().isEmpty());

    Assert.assertEquals(victimSize, victim.size() + listener.evictedKeys().size());
  }

  @Test
  public void testEvictionListenerSeesStealingEventsWriteLocked() {
    MonitoringEvictionListener listener = new MonitoringEvictionListener();
    PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 16 * 4096, 16 * 4096);
    Map<Long, String> victim = new ConcurrentWriteLockedOffHeapClockCache<Long, String>(source, LongStorageEngine.createFactory(OffHeapBufferHalfStorageEngine.createFactory(source, 128, StringPortability.INSTANCE, false, true)), listener);

    long i = 0;
    while (listener.evictedKeys().isEmpty()) {
      victim.put(Long.valueOf(i), Long.toString(i));
      i++;
    }
    listener.evictedKeys().clear();
    long victimSize = victim.size();

    Map<Long, String> thief = new ConcurrentOffHeapHashMap<Long, String>(source, true, LongStorageEngine.createFactory(OffHeapBufferHalfStorageEngine.createFactory(source, 128, StringPortability.INSTANCE, true, false)));

    try {
      i = 0;
      while (true) {
        thief.put(Long.valueOf(i++), Long.toString(i));
      }
    } catch (OversizeMappingException e) {
      //ignore
    }

    Assert.assertFalse(listener.evictedKeys().isEmpty());

    Assert.assertEquals(victimSize, victim.size() + listener.evictedKeys().size());
  }

  static class MonitoringEvictionListener implements EvictionListener<Long, String> {

    private final Set<Long> evictedKeys = new HashSet<Long>();

    @Override
    public void evicting(Callable<Entry<Long, String>> evictee) {
      try {
        Entry<Long, String> evicted = evictee.call();
        Assert.assertEquals(evicted.getKey().longValue(), Long.parseLong(evicted.getValue()));
        Assert.assertTrue(evictedKeys.add(evicted.getKey()));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public Set<Long> evictedKeys() {
      return evictedKeys;
    }
  }

  static class ThrowingEvictionListener implements EvictionListener<Long, String> {

    private final Set<Long> evictedKeys = new HashSet<Long>();

    @Override
    public void evicting(Callable<Entry<Long, String>> evictee) {
      try {
        Entry<Long, String> evicted = evictee.call();
        Assert.assertEquals(evicted.getKey().longValue(), Long.parseLong(evicted.getValue()));
        Assert.assertTrue(evictedKeys.add(evicted.getKey()));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      throw new NullPointerException();
    }

    public Set<Long> evictedKeys() {
      return evictedKeys;
    }
  }
}
