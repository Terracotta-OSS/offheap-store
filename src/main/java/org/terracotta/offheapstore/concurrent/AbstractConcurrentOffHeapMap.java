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
package org.terracotta.offheapstore.concurrent;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.terracotta.offheapstore.HashingMap;
import org.terracotta.offheapstore.OffHeapHashMap;
import org.terracotta.offheapstore.Segment;
import org.terracotta.offheapstore.MapInternals;
import org.terracotta.offheapstore.MetadataTuple;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.jdk8.BiFunction;
import org.terracotta.offheapstore.jdk8.Function;
import org.terracotta.offheapstore.util.Factory;

/**
 * An abstract concurrent (striped) off-heap map.
 * <p>
 * This is an n-way hashcode striped map implementation.  Subclasses must
 * provide a {@link Factory} instance at construction time from which
 * the required number of segments are created.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 *
 * @author Chris Dennis
 */
public abstract class AbstractConcurrentOffHeapMap<K, V> extends AbstractMap<K, V> implements ConcurrentMap<K, V>, ConcurrentMapInternals, HashingMap<K, V> {

  private static final int MAX_SEGMENTS = 1 << 16; // slightly conservative
  private static final int DEFAULT_CONCURRENCY = 16;

  protected final Segment<K, V>[] segments;
  private final int segmentShift;
  private final int segmentMask;

  private Set<K> keySet;
  private Set<Entry<K, V>> entrySet;
  private Collection<V> values;

  /**
   * Create a concurrent map using a default number of segments.
   *
   * @param segmentFactory factory used to create the map segments
   */
  public AbstractConcurrentOffHeapMap(Factory<? extends Segment<K, V>> segmentFactory) {
    this(segmentFactory, DEFAULT_CONCURRENCY);
  }

  /**
   * Create a concurrent map with a defined number of segments.
   *
   * @param segmentFactory factory used to create the map segments
   * @param concurrency number of segments in the map
   * @throws IllegalArgumentException if the supplied number of segments is
   * negative
   */
  @SuppressWarnings("unchecked")
  public AbstractConcurrentOffHeapMap(Factory<? extends Segment<K, V>> segmentFactory, int concurrency) {
    if (concurrency <= 0) {
      throw new IllegalArgumentException("Concurrency must be positive [was: " + concurrency + "]");
    }

    if (concurrency > MAX_SEGMENTS) {
      concurrency = MAX_SEGMENTS;
    }

    // Find power-of-two sizes best matching arguments
    int sshift = 0;
    int ssize = 1;
    while (ssize < concurrency) {
      ++sshift;
      ssize <<= 1;
    }
    segmentShift = 32 - sshift;
    segmentMask = ssize - 1;
    this.segments = new Segment[ssize];

    try {
      for (int i = 0; i < this.segments.length; ++i) {
        this.segments[i] = segmentFactory.newInstance();
      }
    } catch (RuntimeException e) {
      for (Segment<?, ?> s : this.segments) {
        if (s != null) {
          s.destroy();
        }
      }
      throw e;
    }
  }

  protected Segment<K, V> segmentFor(Object key) {
    return segmentFor(key.hashCode());
  }

  protected Segment<K, V> segmentFor(int hash) {
    return segments[getIndexFor(hash)];
  }
  
  public int getIndexFor(int hash) {
    return (spread(hash) >>> segmentShift) & segmentMask;
  }
  
  public List<Segment<K, V>> getSegments() {
    return Collections.unmodifiableList(Arrays.asList(segments));
  }

  protected int getConcurrency() {
    return segments.length;
  }

  private static int spread(int hash) {
    int h = hash;
    h += (h << 15) ^ 0xffffcd7d;
    h ^= (h >>> 10);
    h += (h << 3);
    h ^= (h >>> 6);
    h += (h << 2) + (h << 14);
    return h ^ (h >>> 16);
  }

  @Override
  public int size() {
    long sum = 0;
    readLockAll();
    try {
      for (Map<?, ?> m : segments) {
        sum += m.size();
      }
    } finally {
      readUnlockAll();
    }

    if (sum > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else {
      return (int) sum;
    }
  }

  @Override
  public boolean containsKey(Object key) {
    return segmentFor(key).containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    // Resort to locking all segments
    readLockAll();
    try {
      for (Map<?, ?> m : segments) {
        if (m.containsValue(value)) {
          return true;
        }
      }
    } finally {
      readUnlockAll();
    }
    return false;
  }

  @Override
  public V get(Object key) {
    return segmentFor(key).get(key);
  }

  @Override
  public V put(K key, V value) {
    try {
      return segmentFor(key).put(key, value);
    } catch (OversizeMappingException e) {
      if (handleOversizeMappingException(key.hashCode())) {
        try {
          return segmentFor(key).put(key, value);
        } catch (OversizeMappingException ex) {
          //ignore
        }
      }
      
      writeLockAll();
      try {
        do {
          try {
            return segmentFor(key).put(key, value);
          } catch (OversizeMappingException ex) {
            e = ex;
          }
        } while (handleOversizeMappingException(key.hashCode()));
        throw e;
      } finally {
        writeUnlockAll();
      }
    }
  }

  public V put(K key, V value, int metadata) {
    try {
      return segmentFor(key).put(key, value, metadata);
    } catch (OversizeMappingException e) {
      if (handleOversizeMappingException(key.hashCode())) {
        try {
          return segmentFor(key).put(key, value, metadata);
        } catch (OversizeMappingException ex) {
          //ignore
        }
      }
      
      writeLockAll();
      try {
        do {
          try {
            return segmentFor(key).put(key, value, metadata);
          } catch (OversizeMappingException ex) {
            e = ex;
          }
        } while (handleOversizeMappingException(key.hashCode()));
        throw e;
      } finally {
        writeUnlockAll();
      }
    }
  }

  /**
   * See {@link OffHeapHashMap#fill(Object, Object)} for a detailed description.
   * 
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return the previous value associated with <tt>key</tt>, or
   *         <tt>null</tt> if there was no mapping for <tt>key</tt>
   *         (irrespective of whether the value was successfully installed).
   */
  public V fill(K key, V value) {
    return segmentFor(key).fill(key, value);
  }

  public V fill(K key, V value, int metadata) {
    return segmentFor(key).fill(key, value, metadata);
  }
  
  @Override
  public V remove(Object key) {
    return segmentFor(key).remove(key);
  }

  public boolean removeNoReturn(Object key) {
    return segmentFor(key).removeNoReturn(key);
  }

  public Integer getMetadata(K key, int mask) throws IllegalArgumentException {
    return segmentFor(key).getMetadata(key, mask);
  }
  
  public Integer getAndSetMetadata(K key, int mask, int values) throws IllegalArgumentException {
    return segmentFor(key).getAndSetMetadata(key, mask, values);
  }
  
  public V getValueAndSetMetadata(K key, int mask, int values) {
    return segmentFor(key).getValueAndSetMetadata(key, mask, values);
  }
  
  @Override
  public void clear() {
    writeLockAll();
    try {
      for (Map<?, ?> m : segments) {
        m.clear();
      }
    } finally {
      writeUnlockAll();
    }
  }

  public void destroy() {
    writeLockAll();
    try {
      for (Segment<?, ?> m : segments) {
        m.destroy();
      }
    } finally {
      writeUnlockAll();
    }
  }

  @Override
  public V putIfAbsent(K key, V value) {
    try {
      return segmentFor(key).putIfAbsent(key, value);
    } catch (OversizeMappingException e) {
      if (handleOversizeMappingException(key.hashCode())) {
        try {
          return segmentFor(key).putIfAbsent(key, value);
        } catch (OversizeMappingException ex) {
          e = ex;
        }
      }
      
      writeLockAll();
      try {
        do {
          try {
            return segmentFor(key).putIfAbsent(key, value);
          } catch (OversizeMappingException ex) {
            e = ex;
          }
        } while (handleOversizeMappingException(key.hashCode()));
        throw e;
      } finally {
        writeUnlockAll();
      }
    }
  }

  @Override
  public boolean remove(Object key, Object value) {
    return segmentFor(key).remove(key, value);
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    try {
      return segmentFor(key).replace(key, oldValue, newValue);
    } catch (OversizeMappingException e) {
      if (handleOversizeMappingException(key.hashCode())) {
        try {
          return segmentFor(key).replace(key, oldValue, newValue);
        } catch (OversizeMappingException ex) {
          e = ex;
        }
      }
      
      writeLockAll();
      try {
        do {
          try {
            return segmentFor(key).replace(key, oldValue, newValue);
          } catch (OversizeMappingException ex) {
            e = ex;
          }
        } while (handleOversizeMappingException(key.hashCode()));
        throw e;
      } finally {
        writeUnlockAll();
      }
    }
  }

  @Override
  public V replace(K key, V value) {
    try {
      return segmentFor(key).replace(key, value);
    } catch (OversizeMappingException e) {
      if (handleOversizeMappingException(key.hashCode())) {
        try {
          return segmentFor(key).replace(key, value);
        } catch (OversizeMappingException ex) {
          e = ex;
        }
      }
      
      writeLockAll();
      try {
        do {
          try {
            return segmentFor(key).replace(key, value);
          } catch (OversizeMappingException ex) {
            e = ex;
          }
        } while (handleOversizeMappingException(key.hashCode()));
        throw e;
      } finally {
        writeUnlockAll();
      }
    }
  }

  @Override
  public Set<K> keySet() {
    Set<K> ks = keySet;
    return ks != null ? ks : (keySet = new AggregateKeySet());
  }

  @Override
  public Collection<V> values() {
    Collection<V> vc = values;
    return vc != null ? vc : (values = new AggregatedValuesCollection());
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    Set<Entry<K, V>> es = entrySet;
    return es != null ? es : (entrySet = new AggregateEntrySet());
  }

  class AggregateKeySet extends BaseAggregateSet<K> {

    @Override
    public boolean contains(final Object o) {
      return AbstractConcurrentOffHeapMap.this.containsKey(o);
    }

    @Override
    public boolean remove(final Object o) {
      return AbstractConcurrentOffHeapMap.this.remove(o) != null;
    }

    @Override
    public Iterator<K> iterator() {
      return new AggregateIterator<K>() {

        @Override
        protected Iterator<K> getNextIterator() {
          return listIterator.next().keySet().iterator();
        }
      };
    }

  }

  class AggregateEntrySet extends BaseAggregateSet<Map.Entry<K, V>> {

    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
      return new AggregateIterator<Map.Entry<K, V>>() {

        @Override
        protected Iterator<java.util.Map.Entry<K, V>> getNextIterator() {
          return listIterator.next().entrySet().iterator();
        }
      };
    }

    @Override
    public boolean contains(final Object o) {
      if (!(o instanceof Entry<?, ?>)) { return false; }
      final Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
      final V value = AbstractConcurrentOffHeapMap.this.get(e.getKey());
      return value != null && value.equals(e.getValue());
    }

    @Override
    public boolean remove(final Object o) {
      if (!(o instanceof Entry<?, ?>)) { return false; }
      final Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
      final V value = AbstractConcurrentOffHeapMap.this.get(e.getKey());
      if (value != null && value.equals(e.getValue())) {
        return AbstractConcurrentOffHeapMap.this.remove(e.getKey()) != null;
      } else {
        return false;
      }
    }

  }

  class AggregatedValuesCollection extends AbstractCollection<V> {

    @Override
    public Iterator<V> iterator() {
      return new AggregateIterator<V>() {

        @Override
        protected Iterator<V> getNextIterator() {
          return listIterator.next().values().iterator();
        }
      };
    }

    @Override
    public int size() {
      return AbstractConcurrentOffHeapMap.this.size();
    }

  }

  private abstract class BaseAggregateSet<T> extends AbstractSet<T> {

    @Override
    public int size() {
      return AbstractConcurrentOffHeapMap.this.size();
    }

    @Override
    public void clear() {
      AbstractConcurrentOffHeapMap.this.clear();
    }
  }

  protected abstract class AggregateIterator<T> implements Iterator<T> {

    protected final Iterator<Map<K, V>> listIterator;
    protected Iterator<T>               currentIterator;

    protected abstract Iterator<T> getNextIterator();

    public AggregateIterator() {
      listIterator = Arrays.<Map<K, V>>asList(segments).iterator();
      while (listIterator.hasNext()) {
        currentIterator = getNextIterator();
        if (currentIterator.hasNext()) {
          return;
        }
      }
    }

    @Override
    public boolean hasNext() {
      if (currentIterator == null) { 
        return false; 
      }

      if (currentIterator.hasNext()) {
        return true;
      } else {
        while (listIterator.hasNext()) {
          currentIterator = getNextIterator();
          if (currentIterator.hasNext()) {
            return true;
          }
        }
        return false;
      }
    }

    @Override
    public T next() {
      if (currentIterator == null) {
        throw new NoSuchElementException();
      }

      if (currentIterator.hasNext()) {
        return currentIterator.next();
      } else {
        while (listIterator.hasNext()) {
          currentIterator = getNextIterator();

          if (currentIterator.hasNext()) {
            return currentIterator.next();
          }
        }
      }

      throw new NoSuchElementException();
    }

    @Override
    public void remove() {
      currentIterator.remove();
    }
  }

  protected void readLockAll() {
    for (Segment<?, ?> m : segments) {
      m.readLock().lock();
    }
  }

  protected void readUnlockAll() {
    for (Segment<?, ?> m : segments) {
      m.readLock().unlock();
    }
  }

  public final void writeLockAll() {
    for (Segment<?, ?> m : segments) {
      m.writeLock().lock();
    }
  }

  public final void writeUnlockAll() {
    for (Segment<?, ?> m : segments) {
      m.writeLock().unlock();
    }
  }

  @Override
  public List<MapInternals> getSegmentInternals() {
    return Collections.unmodifiableList(Arrays.<MapInternals>asList(segments));
  }

  @Override
  public long getSize() {
    long sum = 0;
    for (MapInternals m : segments) {
      sum += m.getSize();
    }
    return sum;
  }

  @Override
  public long getTableCapacity() {
    long sum = 0;
    for (MapInternals m : segments) {
      sum += m.getTableCapacity();
    }
    return sum;
  }

  @Override
  public long getUsedSlotCount() {
    long sum = 0;
    for (MapInternals m : segments) {
      sum += m.getUsedSlotCount();
    }
    return sum;
  }

  @Override
  public long getRemovedSlotCount() {
    long sum = 0;
    for (MapInternals m : segments) {
      sum += m.getRemovedSlotCount();
    }
    return sum;
  }

  @Override
  public int getReprobeLength() {
    throw new UnsupportedOperationException("Segmented maps do not have a reprobe length");
  }

  /* Memory usage oriented statistics */
  @Override
  public long getAllocatedMemory() {
    long sum = 0;
    for (MapInternals m : segments) {
      sum += m.getAllocatedMemory();
    }
    return sum;
  }

  @Override
  public long getOccupiedMemory() {
    long sum = 0;
    for (MapInternals m : segments) {
      sum += m.getOccupiedMemory();
    }
    return sum;
  }

  @Override
  public long getVitalMemory() {
    long sum = 0;
    for (MapInternals m : segments) {
      sum += m.getVitalMemory();
    }
    return sum;
  }
  
  @Override
  public long getDataAllocatedMemory() {
    long sum = 0;
    for (MapInternals m : segments) {
      sum += m.getDataAllocatedMemory();
    }
    return sum;
  }

  @Override
  public long getDataOccupiedMemory() {
    long sum = 0;
    for (MapInternals m : segments) {
      sum += m.getDataOccupiedMemory();
    }
    return sum;
  }

  @Override
  public long getDataVitalMemory() {
    long sum = 0;
    for (MapInternals m : segments) {
      sum += m.getDataVitalMemory();
    }
    return sum;
  }

  @Override
  public long getDataSize() {
    long sum = 0;
    for (MapInternals m : segments) {
      sum += m.getDataSize();
    }
    return sum;
  }

  public final boolean handleOversizeMappingException(int hash) {
    boolean evicted = false;

    Segment<?, ?> target = segmentFor(hash);
    for (Segment<?, ?> s : segments) {
      if (s != target) {
        evicted |= s.shrink();
      }
    }

    return evicted;
  }

  /*
   * JDK-8-alike metadata methods
   */
  public MetadataTuple<V> computeWithMetadata(K key, BiFunction<? super K, ? super MetadataTuple<V>, ? extends MetadataTuple<V>> remappingFunction) {
    try {
      return segmentFor(key).computeWithMetadata(key, remappingFunction);
    } catch (OversizeMappingException e) {
      if (handleOversizeMappingException(key.hashCode())) {
        try {
          return segmentFor(key).computeWithMetadata(key, remappingFunction);
        } catch (OversizeMappingException ex) {
          //ignore
        }
      }

      writeLockAll();
      try {
        do {
          try {
            return segmentFor(key).computeWithMetadata(key, remappingFunction);
          } catch (OversizeMappingException ex) {
            e = ex;
          }
        } while (handleOversizeMappingException(key.hashCode()));
        throw e;
      } finally {
        writeUnlockAll();
      }
    }
  }

  public MetadataTuple<V> computeIfAbsentWithMetadata(K key, Function<? super K,? extends MetadataTuple<V>> mappingFunction) {
    try {
      return segmentFor(key).computeIfAbsentWithMetadata(key, mappingFunction);
    } catch (OversizeMappingException e) {
      if (handleOversizeMappingException(key.hashCode())) {
        try {
          return segmentFor(key).computeIfAbsentWithMetadata(key, mappingFunction);
        } catch (OversizeMappingException ex) {
          e = ex;
        }
      }

      writeLockAll();
      try {
        do {
          try {
            return segmentFor(key).computeIfAbsentWithMetadata(key, mappingFunction);
          } catch (OversizeMappingException ex) {
            e = ex;
          }
        } while (handleOversizeMappingException(key.hashCode()));
        throw e;
      } finally {
        writeUnlockAll();
      }
    }
  }

  public MetadataTuple<V> computeIfPresentWithMetadata(K key, BiFunction<? super K,? super MetadataTuple<V>,? extends MetadataTuple<V>> remappingFunction) {
    try {
      return segmentFor(key).computeIfPresentWithMetadata(key, remappingFunction);
    } catch (OversizeMappingException e) {
      if (handleOversizeMappingException(key.hashCode())) {
        try {
          return segmentFor(key).computeIfPresentWithMetadata(key, remappingFunction);
        } catch (OversizeMappingException ex) {
          e = ex;
        }
      }

      writeLockAll();
      try {
        do {
          try {
            return segmentFor(key).computeIfPresentWithMetadata(key, remappingFunction);
          } catch (OversizeMappingException ex) {
            e = ex;
          }
        } while (handleOversizeMappingException(key.hashCode()));
        throw e;
      } finally {
        writeUnlockAll();
      }
    }
  }

  @Override
  public Map<K, V> removeAllWithHash(int keyHash) {
    return segmentFor(keyHash).removeAllWithHash(keyHash);
  }
}
