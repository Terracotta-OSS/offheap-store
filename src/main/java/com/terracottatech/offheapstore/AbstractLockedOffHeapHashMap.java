/*
 * All content copyright (c) 2010-2011 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore;

import java.nio.ByteBuffer;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.storage.StorageEngine;

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

  private Set<Entry<K, V>> entrySet;
  private Set<K> keySet;
  private Collection<V> values;

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
  public boolean setMetadata(K key, int writeMask, int metadata) {
    Lock l = writeLock();
    l.lock();
    try {
      return super.updateMetadata(key, writeMask, metadata);
    } finally {
      l.unlock();
    }
  }

  @Override
  // TODO Make this a real compound op (avoiding multiple lookups) in super class
  public V getAndSetMetadata(final K key, final int mask, final int metadata) {
    Lock l = writeLock();
    l.lock();
    try {
      final V v = get(key);
      if(v != null) {
        if(!updateMetadata(key, mask, metadata)) {
          return null;
        }
      }
      return v;
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
  public Set<Entry<K, V>> entrySet() {
    Set<Entry<K, V>> es = entrySet;
    return es == null ? (entrySet = new LockedEntrySet()) : es;
  }

  class LockedEntrySet extends AbstractSet<Entry<K, V>> {

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

  class LockedEntryIterator extends EntryIterator {

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
  public Set<K> keySet() {
    if (keySet == null) {
      keySet = new LockedKeySet();
    }
    return keySet;
  }

  class LockedKeySet extends AbstractSet<K> {

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

  class LockedKeyIterator extends KeyIterator {

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
  public Collection<V> values() {
    if (values == null) {
      values = new AbstractCollection<V>() {

        @Override
        public Iterator<V> iterator() {
          return new Iterator<V>() {

            private final Iterator<Entry<K, V>> i = entrySet().iterator();

            @Override
            public boolean hasNext() {
              return i.hasNext();
            }

            @Override
            public V next() {
              return i.next().getValue();
            }

            @Override
            public void remove() {
              i.remove();
            }
          };
        }

        @Override
        public int size() {
          return AbstractLockedOffHeapHashMap.this.size();
        }

        @Override
        public boolean isEmpty() {
          return AbstractLockedOffHeapHashMap.this.isEmpty();
        }

        @Override
        public void clear() {
          AbstractLockedOffHeapHashMap.this.clear();
        }

        @Override
        public boolean contains(Object v) {
          return AbstractLockedOffHeapHashMap.this.containsValue(v);
        }
      };
    }
    return values;
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
}
