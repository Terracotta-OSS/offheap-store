/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.concurrent;

import com.terracottatech.offheapstore.Metadata;
import com.terracottatech.offheapstore.Segment;
import com.terracottatech.offheapstore.exceptions.OversizeMappingException;
import com.terracottatech.offheapstore.pinning.PinnableCache;
import com.terracottatech.offheapstore.pinning.PinnableSegment;
import com.terracottatech.offheapstore.util.Factory;

import java.util.Arrays;
import java.util.Comparator;

/**
 *
 * @author cdennis
 */
public abstract class AbstractConcurrentOffHeapCache<K, V> extends AbstractConcurrentOffHeapMap<K, V> implements PinnableCache<K, V> {

  private static final Comparator<Segment<?, ?>> SIZE_COMPARATOR = new Comparator<Segment<?, ?>>() {
    @Override
    public int compare(Segment<?, ?> o1, Segment<?, ?> o2) {
      return (int) (o2.getSize() - o1.getSize());
    }
  };



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
  
  public V getAndPin(final K key) {
    return segmentFor(key).getAndSetMetadata(key, Metadata.PINNED, Metadata.PINNED);
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
          e = ex;
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

  public boolean pin(final K key) {
    return setPinned(key, true);
  }

  public boolean unpin(final K key) {
    return setPinned(key, false);
  }

  private boolean setPinned(final K key, boolean pin) {
    return updateMetadata(key, Metadata.PINNED, pin ? Metadata.PINNED : 0);
  }

  @Override
  public boolean isPinned(Object key) {
    return segmentFor(key).isPinned(key);
  }

  @Override
  public void setPinning(K key, boolean pinned) {
    try {
      segmentFor(key).setPinning(key, pinned);
    } catch (OversizeMappingException e) {
      if (handleOversizeMappingException(key.hashCode())) {
        try {
          segmentFor(key).setPinning(key, pinned);
          return;
        } catch (OversizeMappingException ex) {
          e = ex;
        }
      }

      writeLockAll();
      try {
        do {
          try {
            segmentFor(key).setPinning(key, pinned);
            return;
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
