/*
 * All content copyright (c) Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.storage.restartable.partial;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.offheapstore.paging.OffHeapStorageArea;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.storage.PointerSize;
import com.terracottatech.offheapstore.storage.portability.Portability;
import com.terracottatech.offheapstore.util.Factory;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.terracottatech.offheapstore.storage.restartable.partial.RestartableMinimalStorageEngine.detach;
import static com.terracottatech.offheapstore.util.Validation.shouldValidate;
import static com.terracottatech.offheapstore.util.Validation.validate;

/**
 *
 * @author cdennis
 */
public class RestartablePartialStorageEngine<I, K, V> extends RestartableMinimalStorageEngine<I, K, V> {

  private static final boolean VALIDATING = shouldValidate(RestartablePartialStorageEngine.class);
  
  private static final long NULL_ENCODING = -1L;
  private static final int CACHE_META_OFFSET = 0;
  private static final int CACHE_PREVIOUS_OFFSET = 8;
  private static final int CACHE_NEXT_OFFSET = 16;
  private static final int CACHE_EVICTION_DATA_OFFSET = 24;
  private static final int CACHE_KEY_LENGTH_OFFSET = 28;
  private static final int CACHE_VALUE_LENGTH_OFFSET = 32;
  private static final int CACHE_DATA_OFFSET = 36;

  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final OffHeapStorageArea storage;

  private long first = NULL_ENCODING;
  private long last = NULL_ENCODING;
  private long hand = NULL_ENCODING;

  public static <I, K, V> Factory<RestartablePartialStorageEngine<I, K, V>> createPartialFactory(final I identifier, final RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, final boolean synchronous,
          final PointerSize width, final PageSource source, final int pageSize, final Portability<? super K> keyPortability, final Portability<? super V> valuePortability, final boolean thief, final boolean victim, final float compressThreshold) {
    return new Factory<RestartablePartialStorageEngine<I, K, V>>() {

      @Override
      public RestartablePartialStorageEngine<I, K, V> newInstance() {
        return new RestartablePartialStorageEngine<I, K, V>(identifier, transactionSource, synchronous, width, source, pageSize, keyPortability, valuePortability, thief, victim, compressThreshold);
      }
    };
  }

  public static <I, K, V> Factory<RestartablePartialStorageEngine<I, K, V>> createPartialFactory(final I identifier, final RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, final boolean synchronous,
          final PointerSize width, final PageSource source, final int initialPageSize, final int maximalPageSize, final Portability<? super K> keyPortability, final Portability<? super V> valuePortability, final boolean thief, final boolean victim, final float compressThreshold) {
    return new Factory<RestartablePartialStorageEngine<I, K, V>>() {

      @Override
      public RestartablePartialStorageEngine<I, K, V> newInstance() {
        return new RestartablePartialStorageEngine<I, K, V>(identifier, transactionSource, synchronous, width, source, initialPageSize, maximalPageSize, keyPortability, valuePortability, thief, victim, compressThreshold);
      }
    };
  }
  
  public RestartablePartialStorageEngine(I identifier, RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, boolean synchronous, PointerSize width, PageSource source, int pageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability, float compressThreshold) {
    super(identifier, transactionSource, synchronous, width, source, pageSize, keyPortability, valuePortability, compressThreshold);
    this.storage = new OffHeapStorageArea(width, new CacheStorageAreaOwner(), source, pageSize, false, true, compressThreshold);
  }

  public RestartablePartialStorageEngine(I identifier, RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, boolean synchronous, PointerSize width, PageSource source, int initialPageSize, int maximalPageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability, float compressThreshold) {
    super(identifier, transactionSource, synchronous, width, source, initialPageSize, maximalPageSize, keyPortability, valuePortability, compressThreshold);
    this.storage = new OffHeapStorageArea(width, new CacheStorageAreaOwner(), source, maximalPageSize, false, true, compressThreshold);
  }

  public RestartablePartialStorageEngine(I identifier, RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, boolean synchronous, PointerSize width, PageSource source, int pageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean thief, boolean victim, float compressThreshold) {
    super(identifier, transactionSource, synchronous, width, source, pageSize, keyPortability, valuePortability, thief, victim, compressThreshold);
    this.storage = new OffHeapStorageArea(width, new CacheStorageAreaOwner(), source, pageSize, false, true, compressThreshold);
  }

  public RestartablePartialStorageEngine(I identifier, RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, boolean synchronous, PointerSize width, PageSource source, int initialPageSize, int maximalPageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean thief, boolean victim, float compressThreshold) {
    super(identifier, transactionSource, synchronous, width, source, initialPageSize, maximalPageSize, keyPortability, valuePortability, thief, victim, compressThreshold);
    this.storage = new OffHeapStorageArea(width, new CacheStorageAreaOwner(), source, maximalPageSize, false, true, compressThreshold);
  }
  
  @Override
  public Long writeMapping(final K key, V value, int hash, int metadata) {
    do {
      Long result = super.writeMapping(key, value, hash, metadata);
      if (result != null) {
        createEntry(result).close();
        return result;
      }
    } while (shrink());
    
    return null;
  }

  @Override
  public Long writeBinaryMapping(ByteBuffer binaryKey, ByteBuffer binaryValue, int pojoHash, int metadata) {
    Long result = super.writeBinaryMapping(binaryKey, binaryValue, pojoHash, metadata);
    if (result != null) {
      metadataArea.writeLong(result + getCacheOffset(result), NULL_ENCODING);
    }
    return result;
  }

  @Override
  public Long writeBinaryMapping(ByteBuffer[] binaryKey, ByteBuffer[] binaryValue, int pojoHash, int metadata) {
    Long result = super.writeBinaryMapping(binaryKey, binaryValue, pojoHash, metadata);
    if (result != null) {
      metadataArea.writeLong(result + getCacheOffset(result), NULL_ENCODING);
    }
    return result;
  }

  @Override
  protected int getRequiredEntrySize(ByteBuffer binaryKey, ByteBuffer binaryValue) {
    return super.getRequiredEntrySize(binaryKey, binaryValue) + (Long.SIZE / Byte.SIZE);
  }
  
  @Override
  protected int getActualEntrySize(long encoding) {
    return super.getActualEntrySize(encoding) + (Long.SIZE / Byte.SIZE);
  }

  @Override
  public void freeMapping(long encoding, int hash, boolean removal) {
    lock.writeLock().lock();
    try {
      //XXX this needs to happen half-way through... or in parts
      long cacheAddress = metadataArea.readLong(encoding + getCacheOffset(encoding));
      super.freeMapping(encoding, hash, removal);
      if (cacheAddress >= 0) {
        unlinkEntry(cacheAddress);
        storage.free(cacheAddress);
      }
      validateCache();
    } finally {
      lock.writeLock().unlock();
    }
  }
  
  @Override
  public V readValue(long encoding) {
    lock.readLock().lock();
    try {
      ByteBuffer binaryValue = readCachedBinaryValue(encoding);
      if (binaryValue != null) {
        return (V) valuePortability.decode(binaryValue);
      }
    } finally {
      lock.readLock().unlock();
    }

    Entry result = createEntry(encoding);
    try {
      return (V) valuePortability.decode(result.getValue());
    } finally {
      result.close();
    }
  }

  @Override
  public boolean equalsValue(Object value, long encoding) {
    lock.readLock().lock();
    try {
      ByteBuffer binaryValue = readCachedBinaryValue(encoding);
      if (binaryValue != null) {
        return valuePortability.equals(value, binaryValue);
      }
    } finally {
      lock.readLock().unlock();
    }
    
    Entry result = createEntry(encoding);
    try {
      return valuePortability.equals(value, result.getValue());
    } finally {
      result.close();
    }
  }

  @Override
  public K readKey(long encoding, int hashcode) {
    lock.readLock().lock();
    try {
      ByteBuffer binaryKey = readCachedBinaryKey(encoding);
      if (binaryKey != null) {
        return (K) keyPortability.decode(binaryKey);
      }
    } finally {
      lock.readLock().unlock();
    }

    Entry result = createEntry(encoding);
    try {
      return (K) keyPortability.decode(result.getKey());
    } finally {
      result.close();
    }
  }

  @Override
  public boolean equalsKey(Object key, long encoding) {
    lock.readLock().lock();
    try {
      ByteBuffer binaryKey = readCachedBinaryKey(encoding);
      if (binaryKey != null) {
        return keyPortability.equals(key, binaryKey);
      }
    } finally {
      lock.readLock().unlock();
    }

    Entry result = createEntry(encoding);
    try {
      return keyPortability.equals(key, result.getKey());
    } finally {
      result.close();
    }
  }

  @Override
  public boolean equalsBinaryKey(ByteBuffer probeBinaryKey, long encoding) {
    lock.readLock().lock();
    try {
      ByteBuffer binaryKey = readCachedBinaryKey(encoding);
      if (binaryKey != null) {
        validateCache();
        return equalsBinaryKey(probeBinaryKey, binaryKey);
      }
    } finally {
      lock.readLock().unlock();
    }

    Entry result = createEntry(encoding);
    try {
      return equalsBinaryKey(probeBinaryKey, result.getKey());
    } finally {
      result.close();
    }
  }

  @Override
  public void clear() {
    lock.writeLock().lock();
    try {
      storage.clear();
      first = NULL_ENCODING;
      last = NULL_ENCODING;
      hand = NULL_ENCODING;
      validateCache();
    } finally {
      lock.writeLock().unlock();
    }
    super.clear();
  }

  
  @Override
  public long getAllocatedMemory() {
    return storage.getAllocatedMemory() + super.getAllocatedMemory();
  }

  @Override
  public long getOccupiedMemory() {
    return storage.getOccupiedMemory() + super.getOccupiedMemory();
  }

  @Override
  public void bind(Owner owner) {
    super.bind(new CacheStorageEngineOwner(owner));
  }

  @Override
  public void destroy() {
    lock.writeLock().lock();
    try {
      storage.destroy();
    } finally {
      lock.writeLock().unlock();
    }
    super.destroy();
  }
  
  
  @Override
  public boolean shrink() {
    lock.writeLock().lock();
    try {
      if (storage.shrink()) {
        return true;
      }
    } finally {
      lock.writeLock().unlock();
    }
    return super.shrink();
  }

  @Override
  public ByteBuffer readBinaryKey(long encoding) {
    lock.readLock().lock();
    try {
      ByteBuffer binaryKey = readCachedBinaryKey(encoding);
      if (binaryKey != null) {
        return detach(binaryKey);
      }
    } finally {
      lock.readLock().unlock();
    }

    Entry result = createEntry(encoding);
    try {
      return detach(result.getKey());
    } finally {
      result.close();
    }
  }

  @Override
  public ByteBuffer readBinaryValue(long encoding) {
    lock.readLock().lock();
    try {
      ByteBuffer binaryValue = readCachedBinaryValue(encoding);
      if (binaryValue != null) {
        return detach(binaryValue);
      }
    } finally {
      lock.readLock().unlock();
    }

    Entry result = createEntry(encoding);
    try {
      return detach(result.getValue());
    } finally {
      result.close();
    }
  }
  
  private int getCacheOffset(long encoding) {
    return super.getActualEntrySize(encoding);
  }
  
  //called concurrently (under either read or write lock)
  private Entry createEntry(final long encoding) {
    Entry entry = readEntry(encoding);
    try {
      lock.writeLock().lock();
      try {
        int keyLength = entry.getKey().remaining();
        int valueLength = entry.getValue().remaining();
        do {
          long cacheAddress = storage.allocate(CACHE_DATA_OFFSET + keyLength + valueLength);
          if (cacheAddress >= 0) {
            validateCache();
            storage.writeLong(cacheAddress + CACHE_META_OFFSET, encoding);
            storage.writeInt(cacheAddress + CACHE_EVICTION_DATA_OFFSET, 1);
            storage.writeInt(cacheAddress + CACHE_KEY_LENGTH_OFFSET, keyLength);
            storage.writeInt(cacheAddress + CACHE_VALUE_LENGTH_OFFSET, valueLength);
            storage.writeBuffer(cacheAddress + CACHE_DATA_OFFSET, entry.getKey().duplicate());
            storage.writeBuffer(cacheAddress + CACHE_DATA_OFFSET + keyLength, entry.getValue().duplicate());
            metadataArea.writeLong(encoding + getCacheOffset(encoding), cacheAddress);
            linkEntry(cacheAddress);
            validateCache();
            return entry;
          }
        } while (evict());

        metadataArea.writeLong(encoding + getCacheOffset(encoding), NULL_ENCODING);
        return entry;
      } finally {
        lock.writeLock().unlock();
      }
    } catch (Throwable t) {
      entry.close();
      throw new RuntimeException(t);
    }
  }

  private void free(long encoding) {
    lock.writeLock().lock();
    try {
      long cacheAddress = metadataArea.readLong(encoding + getCacheOffset(encoding));
      if (cacheAddress >= 0) {
        metadataArea.writeLong(encoding + getCacheOffset(encoding), NULL_ENCODING);
        unlinkEntry(cacheAddress);
        storage.free(cacheAddress);
      }
      validateCache();
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void linkEntry(long cacheAddress) {
    if (hand == NULL_ENCODING) {
      storage.writeLong(cacheAddress + CACHE_PREVIOUS_OFFSET, last);
      storage.writeLong(cacheAddress + CACHE_NEXT_OFFSET, NULL_ENCODING);
      if (last == NULL_ENCODING) {
        first = cacheAddress;
      } else {
        storage.writeLong(last + CACHE_NEXT_OFFSET, cacheAddress);
      }
      last = cacheAddress;
    } else {
      long prev = storage.readLong(hand + CACHE_PREVIOUS_OFFSET);
      storage.writeLong(cacheAddress + CACHE_PREVIOUS_OFFSET, prev);
      if (prev == NULL_ENCODING) {
        first = cacheAddress;
      } else {
        storage.writeLong(prev + CACHE_NEXT_OFFSET, cacheAddress);
      }
      storage.writeLong(hand + CACHE_PREVIOUS_OFFSET, cacheAddress);
      storage.writeLong(cacheAddress + CACHE_NEXT_OFFSET, hand);
    }
  }

  private void unlinkEntry(long cacheAddress) {
    long prev = storage.readLong(cacheAddress + CACHE_PREVIOUS_OFFSET);
    long next = storage.readLong(cacheAddress + CACHE_NEXT_OFFSET);
    storage.writeLong(cacheAddress + CACHE_PREVIOUS_OFFSET, NULL_ENCODING);
    storage.writeLong(cacheAddress + CACHE_NEXT_OFFSET, NULL_ENCODING);

    if (prev == NULL_ENCODING) {
      first = next;
    } else {
      storage.writeLong(prev + CACHE_NEXT_OFFSET, next);
    }
    if (next == NULL_ENCODING) {
      last = prev;
    } else {
      storage.writeLong(next + CACHE_PREVIOUS_OFFSET, prev);
    }

    if (hand == cacheAddress) {
      hand = next;
    }
  }

  private boolean evict() {
    while (true) {
      if (hand == NULL_ENCODING) {
        if (first == NULL_ENCODING) {
          return false;
        } else {
          hand = first;
        }
      }

      if (storage.readInt(hand + CACHE_EVICTION_DATA_OFFSET) == 0) {
        free(storage.readLong(hand + CACHE_META_OFFSET));
        return true;
      } else {
        storage.writeInt(hand + CACHE_EVICTION_DATA_OFFSET, 0);
        hand = storage.readLong(hand + CACHE_NEXT_OFFSET);
      }
    }
  }

  private void validateCache() {
    if (VALIDATING) {
      long previous = NULL_ENCODING;
      long current = first;

      while (true) {
        if (current == NULL_ENCODING) {
          validate(last == previous);
          break;
        } else {
          long currentMeta = storage.readLong(current + CACHE_META_OFFSET);
          validate(metadataArea.readLong(currentMeta + getCacheOffset(currentMeta)) == current);
        }

        validate(!VALIDATING || storage.readLong(current + CACHE_PREVIOUS_OFFSET) == previous);
        if (previous != NULL_ENCODING) {
          validate(storage.readLong(previous + CACHE_NEXT_OFFSET) == current);
        }
        previous = current;
        current = storage.readLong(previous + CACHE_NEXT_OFFSET);
      }
    }
  }

  private ByteBuffer readCachedBinaryKey(long encoding) {
    long cacheAddress = metadataArea.readLong(encoding + getCacheOffset(encoding));
    if (cacheAddress >= 0) {
      try {
        storage.writeInt(cacheAddress + CACHE_EVICTION_DATA_OFFSET, 1);
        int keyLength = storage.readInt(cacheAddress + CACHE_KEY_LENGTH_OFFSET);
        validateCache();
        return storage.readBuffer(cacheAddress + CACHE_DATA_OFFSET, keyLength);
      } catch (NullPointerException e) {
        throw new NullPointerException("NPE reading key @ " + cacheAddress);
      }
    } else {
      return null;
    }
  }
  
  private ByteBuffer readCachedBinaryValue(long encoding) {
    long cacheAddress = metadataArea.readLong(encoding + getCacheOffset(encoding));
    if (cacheAddress >= 0) {
      storage.writeInt(cacheAddress + CACHE_EVICTION_DATA_OFFSET, 1);
      int keyLength = storage.readInt(cacheAddress + CACHE_KEY_LENGTH_OFFSET);
      int valueLength = storage.readInt(cacheAddress + CACHE_VALUE_LENGTH_OFFSET);
      validateCache();
      return storage.readBuffer(cacheAddress + CACHE_DATA_OFFSET + keyLength, valueLength);
    } else {
      return null;
    }
  }
  
  private class CacheStorageEngineOwner implements Owner {

    private final Owner owner;
    
    public CacheStorageEngineOwner(Owner owner) {
      this.owner = owner;
    }

    @Override
    public Long getEncodingForHashAndBinary(int hash, ByteBuffer offHeapBinaryKey) {
      return owner.getEncodingForHashAndBinary(hash, offHeapBinaryKey);
    }

    @Override
    public long getSize() {
      return owner.getSize();
    }

    @Override
    public long installMappingForHashAndEncoding(int pojoHash, ByteBuffer offheapBinaryKey, ByteBuffer offheapBinaryValue, int metadata) {
      return owner.installMappingForHashAndEncoding(pojoHash, offheapBinaryKey, offheapBinaryValue, metadata);
    }

    @Override
    public Iterable<Long> encodingSet() {
      return owner.encodingSet();
    }

    @Override
    public boolean updateEncoding(int hashCode, long from, long to, long mask) {
      if (owner.updateEncoding(hashCode, from, to, mask)) {
        lock.writeLock().lock();
        try {
          long cacheAddress = metadataArea.readLong(to + getCacheOffset(to));
          if (cacheAddress >= 0) {
            storage.writeLong(cacheAddress + CACHE_META_OFFSET, to);
          }
        } finally {
          lock.writeLock().unlock();
        }
        return true;
      } else {
        return false;
      }
    }

    @Override
    public Integer getSlotForHashAndEncoding(int hash, long address, long mask) {
      return owner.getSlotForHashAndEncoding(hash, address, mask);
    }

    @Override
    public boolean evict(int slot, boolean b) {
      return owner.evict(slot, b);
    }

    @Override
    public boolean isThiefForTableAllocations() {
      return owner.isThiefForTableAllocations();
    }

    @Override
    public Lock readLock() {
      return owner.readLock();
    }

    @Override
    public Lock writeLock() {
      return owner.writeLock();
    }
  }
 
  private class CacheStorageAreaOwner implements OffHeapStorageArea.Owner {

    //called under write lock
    @Override
    public boolean evictAtAddress(long address, boolean shrink) {
      free(storage.readLong(address + CACHE_META_OFFSET));
      return true;
    }

    //no need for locking
    @Override
    public Lock writeLock() {
      return lock.writeLock();
    }

    //no need for locking
    @Override
    public boolean isThief() {
      return false;
    }

    //called under write lock
    @Override
    public boolean moved(long from, long to) {
      lock.writeLock().lock();
      try {
        long metaAddress = storage.readLong(to + CACHE_META_OFFSET);
        metadataArea.writeLong(metaAddress + getCacheOffset(to), to);
        
        long prev = storage.readLong(to + CACHE_PREVIOUS_OFFSET);
        long next = storage.readLong(to + CACHE_NEXT_OFFSET);
        if (prev == NULL_ENCODING) {
          first = to;
        } else {
          storage.writeLong(prev + CACHE_NEXT_OFFSET, to);
        }
        if (next == NULL_ENCODING) {
          last = to;
        } else {
          storage.writeLong(next + CACHE_PREVIOUS_OFFSET, to);
        }

        if (hand == from) {
          hand = to;
        }
        validateCache();
        return true;
      } finally {
        lock.writeLock().unlock();
      }
    }

    //called under write lock
    @Override
    public int sizeOf(long cacheAddress) {
      lock.readLock().lock();
      try {
        int keyLength = storage.readInt(cacheAddress + CACHE_KEY_LENGTH_OFFSET);
        int valueLength = storage.readInt(cacheAddress + CACHE_VALUE_LENGTH_OFFSET);
        return CACHE_DATA_OFFSET + keyLength + valueLength;
      } finally {
        lock.readLock().unlock();
      }
    }
  }
}
