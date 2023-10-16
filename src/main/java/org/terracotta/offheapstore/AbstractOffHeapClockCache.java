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
package org.terracotta.offheapstore;

import java.util.Random;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.pinning.PinnableCache;
import org.terracotta.offheapstore.pinning.PinnableSegment;
import org.terracotta.offheapstore.storage.StorageEngine;
import org.terracotta.offheapstore.util.DebuggingUtils;

import java.nio.IntBuffer;

/**
 * An abstract off-heap cache implementation.
 * <p>
 * Subclasses must implement the two {@code getEvictionIndex(...)} methods to
 * instruct the cache regarding which mappings to remove.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 *
 * @author Chris Dennis
 * @author Manoj Govindassamy
 */
public abstract class AbstractOffHeapClockCache<K, V> extends AbstractLockedOffHeapHashMap<K, V> implements PinnableCache<K, V>, PinnableSegment<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractOffHeapClockCache.class);

  private static final int PRESENT_CLOCK = 1 << (Integer.SIZE - 1);

  private final Random rndm = new Random();

  private int clockHand;

  public AbstractOffHeapClockCache(PageSource source, StorageEngine<? super K, ? super V> storageEngine) {
    super(source, storageEngine);
  }

  public AbstractOffHeapClockCache(PageSource source, boolean tableAllocationsSteal, StorageEngine<? super K, ? super V> storageEngine) {
    super(source, tableAllocationsSteal, storageEngine);
  }

  public AbstractOffHeapClockCache(PageSource source, StorageEngine<? super K, ? super V> storageEngine, boolean bootstrap) {
    super(source, storageEngine, bootstrap);
  }

  public AbstractOffHeapClockCache(PageSource source, StorageEngine<? super K, ? super V> storageEngine, int tableSize) {
    super(source, storageEngine, tableSize);
  }

  public AbstractOffHeapClockCache(PageSource source, boolean tableAllocationsSteal, StorageEngine<? super K, ? super V> storageEngine, int tableSize) {
    super(source, tableAllocationsSteal, storageEngine, tableSize);
  }

  public AbstractOffHeapClockCache(PageSource source, StorageEngine<? super K, ? super V> storageEngine, int tableSize, boolean bootstrap) {
    super(source, storageEngine, tableSize, bootstrap);
  }

  @Override
  protected void storageEngineFailure(Object failure) {
    if (isEmpty()) {
      throw new OversizeMappingException("Storage Engine and Eviction Failed - Empty Map\nStorage Engine : " + storageEngine);
    } else {
      int evictionIndex = getEvictionIndex();
      if (evictionIndex < 0) {
        throw new OversizeMappingException("Storage Engine and Eviction Failed - Everything Pinned (" + getSize() + " mappings) \n" + "Storage Engine : " + storageEngine);
      } else {
        evict(evictionIndex, false);
      }
    }
  }

  @Override
  protected void tableExpansionFailure(int start, int length) {
    int evictionIndex = getEvictionIndex(start, length);
    if (evictionIndex < 0) {
      if (tryIncreaseReprobe()) {
        LOGGER.debug("Increased reprobe to {} slots for a {} slot table in a last ditch attempt to avoid storage failure.", getReprobeLength(), getTableCapacity());
      } else {
        String msg = "Table Expansion and Eviction Failed.\n" + "Current Table Size (slots) : " + getTableCapacity() + '\n' +
                    "Current Reprobe Length     : " + getReprobeLength() + '\n' +
                    "Resize Will Require        : " + DebuggingUtils.toBase2SuffixedString(getTableCapacity() * ENTRY_SIZE * (Integer.SIZE / Byte.SIZE) * 2) + "B\n" +
                    "Table Page Source          : " + tableSource;
        throw new OversizeMappingException(msg);
      }
    } else {
      evict(evictionIndex, false);
    }
  }

  @Override
  protected void hit(int position, IntBuffer entry) {
    entry.put(STATUS, PRESENT_CLOCK | entry.get(STATUS));
  }

  /**
   * Return the table offset of the to be evicted mapping.
   * <p>
   * The mapping to be evicted can occur anywhere in this cache's table.
   *
   * @return table offset of the mapping to be evicted
   */
  public int getEvictionIndex() {
    /*
     * If the table has been shrunk then it's possible for the clock-hand to
     * point past the end of the table.  We cannot allow the initial-hand to
     * be equal to this otherwise we may never be able to terminate this loop.
     */
    if (clockHand >= hashtable.capacity()) {
      clockHand = 0;
    }

    int initialHand = clockHand;
    int loops = 0;
    while (true) {
      if ((clockHand += ENTRY_SIZE) + STATUS >= hashtable.capacity()) {
        clockHand = 0;
      }

      int hand = hashtable.get(clockHand + STATUS);

      if (evictable(hand) && ((hand & PRESENT_CLOCK) == 0)) {
        return clockHand;
      } else if ((hand & PRESENT_CLOCK) == PRESENT_CLOCK) {
        hashtable.put(clockHand + STATUS, hand & ~PRESENT_CLOCK);
      }

      if (initialHand == clockHand && ++loops == 2) {
        return -1;
      }
    }
  }

  /**
   * Return the table offset of the to be evicted mapping within the given probe
   * sequence.
   * <p>
   * The mapping to be evicted must occur within the given probe sequence.
   *
   * @param start initial slot in probe sequence
   * @param length number of slots in probe sequence
   * @return table offset of the mapping to be evicted
   */
  private int getEvictionIndex(int start, int length) {
    /*
     * TODO This pseudo clock eviction may not be the best algorithm.  Currently
     * the algorithm is to do a regular eviction selection, which helps the clock
     * by advancing it, and also some clock-bit clearing.  If the resultant index
     * is in our probe sequence (which isn't the most simple of calculations - I
     * think I have it right here) then just return that as the eviction index,
     * and all is good.  If that index is not in our probe sequence, then we evict
     * that element anyway, we then go on to see if any of the entries in our
     * probe sequence have their clock bit cleared, if so we evict one of them to
     * clear space for the incoming element.  If all elements have set clock bits
     * then we just kick one out at random.
     */
    int index = getEvictionIndex();

    int tableLength = hashtable.capacity();
    int probeLength = length * ENTRY_SIZE;

    if (probeLength >= hashtable.capacity()) {
      return index;
    }

    int end = (start + probeLength) & (tableLength - 1);

    if (index < 0) {
      return index;
    } else if ((end > start) && (index >= start) && (index <= end)) {
      return index;
    } else if ((end < start) && ((index >= start) || (index < end))) {
      return index;
    } else {
      evict(index, false);

      for (int i = 0, clock = start; i < length; i++) {
        if ((clock += ENTRY_SIZE) >= tableLength) {
          clock = start;
        }

        int hand = hashtable.get(clock + STATUS);

        if (evictable(hand) && ((hand & PRESENT_CLOCK) == 0)) {
          return clock;
        }
      }

      int lastEvictable = -1;
      for (int i = 0, clock = start; i < rndm.nextInt(length) || (lastEvictable < 0 && i < length); i++) {
        if ((clock += ENTRY_SIZE) >= tableLength) {
          clock = start;
        }

        int hand = hashtable.get(clock + STATUS);

        if (evictable(hand)) {
          lastEvictable = clock;
        }
      }
      return lastEvictable;
    }
  }

  protected boolean evictable(int status) {
    return (status & STATUS_USED) == STATUS_USED && ((status & Metadata.PINNED) == 0);
  }

  @Override
  public boolean evict(int index, boolean shrink) {
    Lock l = writeLock();
    l.lock();
    try {
      if (evictable(hashtable.get(index + STATUS))) {
        removeAtTableOffset(index, shrink);
        return true;
      } else {
        return false;
      }
    } finally {
      l.unlock();
    }
  }

  @Override
  public boolean isPinned(Object key) {
    Integer metadata = getMetadata(key, Metadata.PINNED);
    return metadata != null && metadata != 0;
  }

  @Override
  public void setPinning(K key, boolean pinned) {
    if (pinned) {
      getAndSetMetadata(key, Metadata.PINNED, Metadata.PINNED);
    } else {
      getAndSetMetadata(key, Metadata.PINNED, 0);
    }
  }

  @Override
  public V putPinned(K key, V value) {
    return put(key, value, Metadata.PINNED);
  }

  @Override
  public V getAndPin(K key) {
    return getValueAndSetMetadata(key, Metadata.PINNED, Metadata.PINNED);
  }
}
