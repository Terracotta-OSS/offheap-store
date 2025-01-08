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

import java.nio.ByteBuffer;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.StorageEngine;

/**
 * An abstract locked off-heap map.
 * <p>
 * Subclasses must implement the {@code readLock()} and {@code writeLock()}
 * methods such that they return the correct locks under which read and write
 * operations must occur.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 *
 * @author Chris Dennis
 */
public abstract class AbstractLockedOffHeapHashMap<K, V> extends OffHeapHashMap<K, V> implements Segment<K, V> {

  public AbstractLockedOffHeapHashMap(PageSource source, StorageEngine<? super K, ? super V> storageEngine) {
    super(source, storageEngine);
  }

  public AbstractLockedOffHeapHashMap(PageSource source, boolean tableAllocationsSteal, StorageEngine<? super K, ? super V> storageEngine) {
    super(source, tableAllocationsSteal, storageEngine);
  }

  public AbstractLockedOffHeapHashMap(PageSource source, StorageEngine<? super K, ? super V> storageEngine, boolean bootstrap) {
    super(source, storageEngine, bootstrap);
  }

  public AbstractLockedOffHeapHashMap(PageSource source, StorageEngine<? super K, ? super V> storageEngine, int tableSize) {
    super(source, storageEngine, tableSize);
  }

  public AbstractLockedOffHeapHashMap(PageSource source, boolean tableAllocationsSteal, StorageEngine<? super K, ? super V> storageEngine, int tableSize) {
    super(source, tableAllocationsSteal, storageEngine, tableSize);
  }

  public AbstractLockedOffHeapHashMap(PageSource source, StorageEngine<? super K, ? super V> storageEngine, int tableSize, boolean bootstrap) {
    super(source, false, storageEngine, tableSize, bootstrap);
  }

  @Override
  public int size() {
    Lock l = readLock();
    l.lock();
    try {
      return super.size();
    } finally {
      l.unlock();
    }
  }

  @Override
  public boolean containsKey(Object key) {
    Lock l = readLock();
    l.lock();
    try {
      return super.containsKey(key);
    } finally {
      l.unlock();
    }
  }

  @Override
  public V get(Object key) {
    Lock l = readLock();
    l.lock();
    try {
      return super.get(key);
    } finally {
      l.unlock();
    }
  }

  @Override
  public Long getEncodingForHashAndBinary(int hash, ByteBuffer binaryKey) {
    Lock l = readLock();
    l.lock();
    try {
      return super.getEncodingForHashAndBinary(hash, binaryKey);
    } finally {
      l.unlock();
    }
  }

  @Override
  public long installMappingForHashAndEncoding(int pojoHash, ByteBuffer offheapBinaryKey, ByteBuffer offheapBinaryValue, int metadata) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.installMappingForHashAndEncoding(pojoHash, offheapBinaryKey, offheapBinaryValue, metadata);
    } finally {
      l.unlock();
    }
  }

  @Override
  public V put(K key, V value) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.put(key, value);
    } finally {
      l.unlock();
    }
  }

  @Override
  public V put(K key, V value, int metadata) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.put(key, value, metadata);
    } finally {
      l.unlock();
    }
  }

  @Override
  public V fill(K key, V value) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.fill(key, value);
    } finally {
      l.unlock();
    }
  }

  @Override
  public V fill(K key, V value, int metadata) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.fill(key, value, metadata);
    } finally {
      l.unlock();
    }
  }

  @Override
  public V remove(Object key) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.remove(key);
    } finally {
      l.unlock();
    }
  }

  @Override
  public boolean removeNoReturn(Object key) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.removeNoReturn(key);
    } finally {
      l.unlock();
    }
  }

  @Override
  public void clear() {
    Lock l = writeLock();
    l.lock();
    try {
      super.clear();
    } finally {
      l.unlock();
    }
  }

  @Override
  public V putIfAbsent(K key, V value) {
    Lock l = writeLock();
    l.lock();
    try {
      if (key == null || value == null) {
        throw new NullPointerException();
      }
      V existing = get(key);
      if (existing == null) {
        put(key, value);
      }
      return existing;
    } finally {
      l.unlock();
    }
  }

  @Override
  public boolean remove(Object key, Object value) {
    Lock l = writeLock();
    l.lock();
    try {
      if (key == null) {
        throw new NullPointerException();
      }
      if (value == null) {
        return false;
      }
      V existing = get(key);
      if (value.equals(existing)) {
        remove(key);
        return true;
      } else {
        return false;
      }
    } finally {
      l.unlock();
    }
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    Lock l = writeLock();
    l.lock();
    try {
      V existing = get(key);
      if (oldValue.equals(existing)) {
        put(key, newValue);
        return true;
      } else {
        return false;
      }
    } finally {
      l.unlock();
    }
  }

  @Override
  public V replace(K key, V value) {
    Lock l = writeLock();
    l.lock();
    try {
      if (value == null || key == null) {
        throw new NullPointerException();
      }
      V existing = get(key);
      if (existing != null) {
        put(key, value);
      }
      return existing;
    } finally {
      l.unlock();
    }
  }

  @Override
  public Integer getMetadata(Object key, int mask) {
    Lock l = readLock();
    l.lock();
    try {
      return super.getMetadata(key, mask);
    } finally {
      l.unlock();
    }
  }

  @Override
  public Integer getAndSetMetadata(Object key, int mask, int values) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.getAndSetMetadata(key, mask, values);
    } finally {
      l.unlock();
    }
  }

  @Override
  public V getValueAndSetMetadata(Object key, int mask, int values) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.getValueAndSetMetadata(key, mask, values);
    } finally {
      l.unlock();
    }
  }

  /*
   * remove used by EntrySet
   */
  @Override
  protected boolean removeMapping(Object o) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.removeMapping(o);
    } finally {
      l.unlock();
    }
  }

  @Override
  public boolean evict(int index, boolean shrink) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.evict(index, shrink);
    } finally {
      l.unlock();
    }
  }

  @Override
  protected Set<Entry<K, V>> createEntrySet() {
    return new LockedEntrySet();
  }

  protected class LockedEntrySet extends AbstractSet<Entry<K, V>> {

    @Override
    public Iterator<Entry<K, V>> iterator() {
      Lock l = readLock();
      l.lock();
      try {
        return new LockedEntryIterator();
      } finally {
        l.unlock();
      }
    }

    @Override
    public boolean contains(Object o) {
      if (!(o instanceof Entry<?, ?>)) {
        return false;
      }
      @SuppressWarnings("unchecked")
      Entry<K, V> e = (Entry<K, V>) o;
      Lock l = readLock();
      l.lock();
      try {
        V value = AbstractLockedOffHeapHashMap.this.get(e.getKey());
        return value != null && value.equals(e.getValue());
      } finally {
        l.unlock();
      }
    }

    @Override
    public boolean remove(Object o) {
      return AbstractLockedOffHeapHashMap.this.removeMapping(o);
    }

    @Override
    public int size() {
      return AbstractLockedOffHeapHashMap.this.size();
    }

    @Override
    public void clear() {
      AbstractLockedOffHeapHashMap.this.clear();
    }
  }

  protected class LockedEntryIterator extends EntryIterator {

    @Override
    public Entry<K, V> next() {
      Lock l = readLock();
      l.lock();
      try {
        return super.next();
      } finally {
        l.unlock();
      }
    }

    @Override
    public void remove() {
      Lock l = writeLock();
      l.lock();
      try {
        super.remove();
      } finally {
        l.unlock();
      }
    }

    @Override
    protected void checkForConcurrentModification() {
      //no-op
    }
  }

  @Override
  protected Set<K> createKeySet() {
    return new LockedKeySet();
  }

  protected class LockedKeySet extends AbstractSet<K> {

    @Override
    public Iterator<K> iterator() {
      Lock l = readLock();
      l.lock();
      try {
        return new LockedKeyIterator();
      } finally {
        l.unlock();
      }
    }

    @Override
    public boolean contains(Object o) {
      return AbstractLockedOffHeapHashMap.this.containsKey(o);
    }

    @Override
    public boolean remove(Object o) {
      return AbstractLockedOffHeapHashMap.this.remove(o) != null;
    }

    @Override
    public int size() {
      return AbstractLockedOffHeapHashMap.this.size();
    }

    @Override
    public void clear() {
      AbstractLockedOffHeapHashMap.this.clear();
    }
  }

  protected class LockedKeyIterator extends KeyIterator {

    @Override
    public K next() {
      Lock l = readLock();
      l.lock();
      try {
        return super.next();
      } finally {
        l.unlock();
      }
    }

    @Override
    public void remove() {
      Lock l = writeLock();
      l.lock();
      try {
        super.remove();
      } finally {
        l.unlock();
      }
    }

    @Override
    protected void checkForConcurrentModification() {
      //no-op
    }
  }

  @Override
  public void destroy() {
    Lock l = writeLock();
    l.lock();
    try {
      super.destroy();
    } finally {
      l.unlock();
    }
  }

  @Override
  public boolean shrink() {
    Lock l = writeLock();
    l.lock();
    try {
      return storageEngine.shrink();
    } finally {
      l.unlock();
    }
  }

  @Override
  public abstract Lock readLock();

  @Override
  public abstract Lock writeLock();

  /*
   * JDK-8-alike metadata methods
   */
  @Override
  public MetadataTuple<V> computeWithMetadata(K key, BiFunction<? super K, ? super MetadataTuple<V>, ? extends MetadataTuple<V>> remappingFunction) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.computeWithMetadata(key, remappingFunction);
    } finally {
      l.unlock();
    }
  }

  @Override
  public MetadataTuple<V> computeIfAbsentWithMetadata(K key, Function<? super K,? extends MetadataTuple<V>> mappingFunction) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.computeIfAbsentWithMetadata(key, mappingFunction);
    } finally {
      l.unlock();
    }
  }

  @Override
  public MetadataTuple<V> computeIfPresentWithMetadata(K key, BiFunction<? super K,? super MetadataTuple<V>,? extends MetadataTuple<V>> remappingFunction) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.computeIfPresentWithMetadata(key, remappingFunction);
    } finally {
      l.unlock();
    }
  }

  @Override
  public Map<K, V> removeAllWithHash(int hash) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.removeAllWithHash(hash);
    } finally {
      l.unlock();
    }
  }
}
