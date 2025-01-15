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
package org.terracotta.offheapstore.concurrent;

import org.terracotta.offheapstore.Metadata;
import org.terracotta.offheapstore.Segment;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.pinning.PinnableCache;
import org.terracotta.offheapstore.pinning.PinnableSegment;
import org.terracotta.offheapstore.util.Factory;

import java.util.Arrays;
import java.util.Comparator;

/**
 *
 * @author cdennis
 */
public abstract class AbstractConcurrentOffHeapCache<K, V> extends AbstractConcurrentOffHeapMap<K, V> implements PinnableCache<K, V> {

  private static final Comparator<Segment<?, ?>> SIZE_COMPARATOR = (o1, o2) -> (int) (o2.getSize() - o1.getSize());



  public AbstractConcurrentOffHeapCache(Factory<? extends PinnableSegment<K, V>> segmentFactory) {
    super(segmentFactory);
  }

  public AbstractConcurrentOffHeapCache(Factory<? extends Segment<K, V>> segmentFactory, int concurrency) {
    super(segmentFactory, concurrency);
  }

  @Override
  public V fill(K key, V value) {
    try {
      return super.fill(key, value);
    } catch (OversizeMappingException e) {
      return null;
    }
  }

  @Override
  public V getAndPin(final K key) {
    return segmentFor(key).getValueAndSetMetadata(key, Metadata.PINNED, Metadata.PINNED);
  }

  @Override
  public V putPinned(final K key, final V value) {
    try {
      return segmentFor(key).putPinned(key, value);
    } catch (OversizeMappingException e) {
      if (handleOversizeMappingException(key.hashCode())) {
        try {
          return segmentFor(key).putPinned(key, value);
        } catch (OversizeMappingException ex) {
          //ignore
        }
      }

      writeLockAll();
      try {
        do {
          try {
            return segmentFor(key).putPinned(key, value);
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
  public boolean isPinned(Object key) {
    return segmentFor(key).isPinned(key);
  }

  @Override
  public void setPinning(K key, boolean pinned) {
    segmentFor(key).setPinning(key, pinned);
  }

  @Override
  protected PinnableSegment<K, V> segmentFor(Object key) {
    return (PinnableSegment<K, V>) super.segmentFor(key);
  }

  public boolean shrink() {
    Segment<?, ?>[] sorted = segments.clone();
    Arrays.sort(sorted, SIZE_COMPARATOR);
    for (Segment<?, ?> s : sorted) {
      if (s.shrink()) {
        return true;
      }
    }
    return false;
  }

  public boolean shrinkOthers(final int excludedHash) {
    boolean evicted = false;

    Segment<?, ?> target = segmentFor(excludedHash);
    for (Segment<?, ?> s : segments) {
      if (s == target) {
        continue;
      }
      evicted |= s.shrink();
    }

    return evicted;
  }
}
