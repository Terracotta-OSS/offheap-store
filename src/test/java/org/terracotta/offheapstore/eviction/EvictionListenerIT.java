/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
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
package org.terracotta.offheapstore.eviction;

import org.terracotta.offheapstore.eviction.EvictionListener;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;

import org.junit.Assert;
import org.junit.Test;

import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.concurrent.ConcurrentOffHeapClockCache;
import org.terracotta.offheapstore.concurrent.ConcurrentOffHeapHashMap;
import org.terracotta.offheapstore.concurrent.ConcurrentWriteLockedOffHeapClockCache;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.LongStorageEngine;
import org.terracotta.offheapstore.storage.OffHeapBufferHalfStorageEngine;
import org.terracotta.offheapstore.storage.portability.StringPortability;

/**
 *
 * @author Chris Dennis
 */
public class EvictionListenerIT {

  @Test
  public void testEvictionListenerReadWriteLocked() {
    MonitoringEvictionListener listener = new MonitoringEvictionListener();
    PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 16 * 4096, 2048);
    Map<Long, String> map = new ConcurrentOffHeapClockCache<>(source, LongStorageEngine.createFactory(OffHeapBufferHalfStorageEngine
      .createFactory(source, 128, StringPortability.INSTANCE)), listener);

    for (long i = 0; i < 2000L; i++) {
      map.put(i, Long.toString(i));
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
    Map<Long, String> map = new ConcurrentWriteLockedOffHeapClockCache<>(source, LongStorageEngine.createFactory(OffHeapBufferHalfStorageEngine
      .createFactory(source, 128, StringPortability.INSTANCE)), listener);

    for (long i = 0; i < 2000L; i++) {
      map.put(i, Long.toString(i));
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
    Map<Long, String> map = new ConcurrentOffHeapClockCache<>(source, LongStorageEngine.createFactory(OffHeapBufferHalfStorageEngine
      .createFactory(source, 128, StringPortability.INSTANCE)), listener);

    for (long i = 0; i < 2000L; i++) {
      try {
        map.put(i, Long.toString(i));
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
    Map<Long, String> map = new ConcurrentWriteLockedOffHeapClockCache<>(source, LongStorageEngine.createFactory(OffHeapBufferHalfStorageEngine
      .createFactory(source, 128, StringPortability.INSTANCE)), listener);

    for (long i = 0; i < 2000L; i++) {
      try {
        map.put(i, Long.toString(i));
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
    Map<Long, String> victim = new ConcurrentOffHeapClockCache<>(source, LongStorageEngine.createFactory(OffHeapBufferHalfStorageEngine
      .createFactory(source, 128, StringPortability.INSTANCE, false, true)), listener);

    long i = 0;
    while (listener.evictedKeys().isEmpty()) {
      victim.put(i, Long.toString(i));
      i++;
    }
    listener.evictedKeys().clear();
    long victimSize = victim.size();

    Map<Long, String> thief = new ConcurrentOffHeapHashMap<>(source, true, LongStorageEngine.createFactory(OffHeapBufferHalfStorageEngine
      .createFactory(source, 128, StringPortability.INSTANCE, true, false)));

    try {
      i = 0;
      while (true) {
        thief.put(i, Long.toString(i));
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
    Map<Long, String> victim = new ConcurrentWriteLockedOffHeapClockCache<>(source, LongStorageEngine.createFactory(OffHeapBufferHalfStorageEngine
      .createFactory(source, 128, StringPortability.INSTANCE, false, true)), listener);

    long i = 0;
    while (listener.evictedKeys().isEmpty()) {
      victim.put(i, Long.toString(i));
      i++;
    }
    listener.evictedKeys().clear();
    long victimSize = victim.size();

    Map<Long, String> thief = new ConcurrentOffHeapHashMap<>(source, true, LongStorageEngine.createFactory(OffHeapBufferHalfStorageEngine
      .createFactory(source, 128, StringPortability.INSTANCE, true, false)));

    try {
      i = 0;
      while (true) {
        thief.put(i++, Long.toString(i));
      }
    } catch (OversizeMappingException e) {
      //ignore
    }

    Assert.assertFalse(listener.evictedKeys().isEmpty());

    Assert.assertEquals(victimSize, victim.size() + listener.evictedKeys().size());
  }

  static class MonitoringEvictionListener implements EvictionListener<Long, String> {

    private final Set<Long> evictedKeys = new HashSet<>();

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

    private final Set<Long> evictedKeys = new HashSet<>();

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
