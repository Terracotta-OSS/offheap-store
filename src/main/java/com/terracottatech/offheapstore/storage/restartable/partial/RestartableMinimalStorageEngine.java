/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.storage.restartable.partial;

import com.terracottatech.frs.Disposable;
import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.Tuple;
import com.terracottatech.frs.object.ObjectManagerEntry;
import com.terracottatech.frs.object.ObjectManagerSegment;
import com.terracottatech.frs.object.SimpleObjectManagerEntry;
import com.terracottatech.offheapstore.paging.OffHeapStorageArea;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.storage.BinaryStorageEngine;
import com.terracottatech.offheapstore.storage.PointerSize;
import com.terracottatech.offheapstore.storage.StorageEngine;
import com.terracottatech.offheapstore.storage.listener.AbstractListenableStorageEngine;
import com.terracottatech.offheapstore.storage.portability.Portability;
import com.terracottatech.offheapstore.util.Factory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;

import static com.terracottatech.offheapstore.util.Validation.shouldValidate;
import static com.terracottatech.offheapstore.util.Validation.validate;
import static com.terracottatech.offheapstore.storage.restartable.RestartableStorageEngine.decodeKey;
import static com.terracottatech.offheapstore.storage.restartable.RestartableStorageEngine.encodeKey;
import static com.terracottatech.offheapstore.storage.restartable.RestartableStorageEngine.decodeValue;
import static com.terracottatech.offheapstore.storage.restartable.RestartableStorageEngine.encodeValue;
import static com.terracottatech.offheapstore.storage.restartable.RestartableStorageEngine.extractEncoding;
import static com.terracottatech.offheapstore.storage.restartable.RestartableStorageEngine.extractMetadata;
import static com.terracottatech.offheapstore.util.ByteBufferUtils.aggregate;

public class RestartableMinimalStorageEngine<I, K, V> extends AbstractListenableStorageEngine<K, V> implements StorageEngine<K, V>, BinaryStorageEngine, ObjectManagerSegment<I, ByteBuffer, ByteBuffer> {

  private static final boolean VALIDATING = shouldValidate(RestartableMinimalStorageEngine.class);
  
  private static final int META_LSN_OFFSET = 0;
  private static final int META_PREVIOUS_OFFSET = 8;
  private static final int META_NEXT_OFFSET = 16;
  private static final int META_KEY_HASH_OFFSET = 24;
  private static final int META_ENTRY_SIZE_OFFSET = 28;
  private static final int META_SIZE = 32;

  private static final long NULL_ENCODING = Long.MIN_VALUE;
  
  public static <I, K, V> Factory<RestartableMinimalStorageEngine<I, K, V>> createMinimalFactory(final I identifier, final RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, final boolean synchronous,
          final PointerSize width, final PageSource source, final int pageSize, final Portability<? super K> keyPortability, final Portability<? super V> valuePortability, final boolean thief, final boolean victim, final float compressThreshold) {
    return new Factory<RestartableMinimalStorageEngine<I, K, V>>() {

      @Override
      public RestartableMinimalStorageEngine<I, K, V> newInstance() {
        return new RestartableMinimalStorageEngine<I, K, V>(identifier, transactionSource, synchronous, width, source, pageSize, keyPortability, valuePortability, thief, victim, compressThreshold);
      }
    };
  }

  public static <I, K, V> Factory<RestartableMinimalStorageEngine<I, K, V>> createMinimalFactory(final I identifier, final RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, final boolean synchronous,
          final PointerSize width, final PageSource source, final int initialPageSize, final int maximalPageSize, final Portability<? super K> keyPortability, final Portability<? super V> valuePortability, final boolean thief, final boolean victim, final float compressThreshold) {
    return new Factory<RestartableMinimalStorageEngine<I, K, V>>() {

      @Override
      public RestartableMinimalStorageEngine<I, K, V> newInstance() {
        return new RestartableMinimalStorageEngine<I, K, V>(identifier, transactionSource, synchronous, width, source, initialPageSize, maximalPageSize, keyPortability, valuePortability, thief, victim, compressThreshold);
      }
    };
  }
  
  private final Map<Long, Map.Entry<ByteBuffer, ByteBuffer>> holdingArea = new HashMap<Long, Map.Entry<ByteBuffer, ByteBuffer>>();
  protected final OffHeapStorageArea metadataArea;
  protected final Portability<? super K> keyPortability;
  protected final Portability<? super V> valuePortability;
  private final I identifier;
  private final RestartStore<I, ByteBuffer, ByteBuffer> transactionSource;
  private final boolean synchronous;
  
  protected volatile Owner owner;

  private long lsnFirst = NULL_ENCODING;
  private long lsnLast = NULL_ENCODING;
  private ObjectManagerEntry<I, ByteBuffer, ByteBuffer> compactingEntry;
  
  private volatile long dataSize = 0L;

  public RestartableMinimalStorageEngine(I identifier, RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, boolean synchronous,
          PointerSize width, PageSource source, int pageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability, float compressThreshold) {
    this(identifier, transactionSource, synchronous, width, source, pageSize, pageSize, keyPortability, valuePortability, compressThreshold);
  }

  public RestartableMinimalStorageEngine(I identifier, RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, boolean synchronous,
          PointerSize width, PageSource source, int initialPageSize, int maximalPageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability, float compressThreshold) {
    this(identifier, transactionSource, synchronous, width, source, initialPageSize, maximalPageSize, keyPortability, valuePortability, false, false, compressThreshold);
  }

  public RestartableMinimalStorageEngine(I identifier, RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, boolean synchronous,
          PointerSize width, PageSource source, int pageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean thief, boolean victim, float compressThreshold) {
    this(identifier, transactionSource, synchronous, width, source, pageSize, pageSize, keyPortability, valuePortability, thief, victim, compressThreshold);
  }

  public RestartableMinimalStorageEngine(I identifier, RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, boolean synchronous, PointerSize width, PageSource source, int initialPageSize, int maximalPageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean thief, boolean victim, float compressThreshold) {
    this.identifier = identifier;
    this.transactionSource = transactionSource;
    this.metadataArea = new OffHeapStorageArea(width, new MetadataOwner(), source, initialPageSize, maximalPageSize, thief, victim, compressThreshold);
    this.keyPortability = keyPortability;
    this.valuePortability = valuePortability;
    this.synchronous = synchronous;
  }

  // Called under segment lock
  @Override
  public Long writeMapping(final K key, V value, int hash, int metadata) {
    ByteBuffer binaryKey = keyPortability.encode(key);
    ByteBuffer binaryValue = valuePortability.encode(value);
    
    Long result = writePartialEntry(hash, binaryKey, binaryValue);
    if (result == null) {
      return null;
    } else {
      holdingArea.put(result, new SimpleImmutableEntry<ByteBuffer, ByteBuffer>(binaryKey, binaryValue));
      if (hasListeners()) {
        fireWritten(key, value, binaryKey.duplicate(), binaryValue.duplicate(), hash, metadata, result);
      }
      return result;
    }
  }

  protected Long writePartialEntry(int hash, ByteBuffer binaryKey, ByteBuffer binaryValue) {
    int entrySize = getRequiredEntrySize(binaryKey, binaryValue);
    long encoding = metadataArea.allocate(entrySize);
    if (encoding >= 0) {
      int size = binaryKey.remaining() + binaryValue.remaining();
      metadataArea.writeLong(encoding + META_LSN_OFFSET, NULL_ENCODING);
      metadataArea.writeLong(encoding + META_NEXT_OFFSET, NULL_ENCODING);
      metadataArea.writeLong(encoding + META_PREVIOUS_OFFSET, NULL_ENCODING);
      metadataArea.writeInt(encoding + META_KEY_HASH_OFFSET, hash);
      metadataArea.writeInt(encoding + META_ENTRY_SIZE_OFFSET, size);
      dataSize += size;
      return encoding;
    } else {
      return null;
    }
  }

  protected int getRequiredEntrySize(ByteBuffer binaryKey, ByteBuffer binaryValue) {
    return META_SIZE;
  }

  protected int getActualEntrySize(long encoding) {
    return META_SIZE;
  }

  // Called under segment lock
  @Override
  public void attachedMapping(long encoding, int hash, int metadata) {
    //XXX This is fragile but... We *know* that the entry is in offheap here because it isn't in the clock and therefore cannot have been evicted
    ByteBuffer frsBinaryKey = encodeKey(readBinaryKey(encoding), hash);
    ByteBuffer frsBinaryValue = encodeValue(readBinaryValue(encoding), encoding, metadata);
    try {
      transactionSource.beginTransaction(synchronous).put(identifier, frsBinaryKey, frsBinaryValue).commit();
    } catch (TransactionException e) {
      throw new RuntimeException(e);
    }
  }
  
  // Called under segment lock
  @Override
  public void freeMapping(long encoding, int hash, boolean removal) {
    boolean listeners = hasListeners();
    ByteBuffer rawKey = null;
    if (listeners || removal) {
      rawKey = readBinaryKey(encoding);
    }
    
    unlinkNode(encoding);
    if (removal) {
      restartabilityRemove(rawKey.duplicate(), hash);
    }
    
    dataSize -= metadataArea.readInt(encoding + META_ENTRY_SIZE_OFFSET);
    metadataArea.free(encoding);
    holdingArea.remove(encoding);
    validChain();
    
    if (listeners) {
      fireFreed(encoding, hash, rawKey, removal);
    }
  }

  private void restartabilityRemove(ByteBuffer offheapBinaryKey, int hash) {
    ByteBuffer frsBinaryKey = encodeKey(offheapBinaryKey, hash);
    try {
      transactionSource.beginTransaction(synchronous).remove(identifier, frsBinaryKey).commit();
    } catch (TransactionException e) {
      throw new RuntimeException(e);
    }
  }
  
  // Called under segment lock
  @Override
  public V readValue(final long encoding) {
    Entry result = readEntry(encoding);
    try {
      return (V) valuePortability.decode(result.getValue());
    } finally {
      result.close();
    }
  }

  // Called under segment lock
  @Override
  public boolean equalsValue(Object value, final long encoding) {
    Entry result = readEntry(encoding);
    try {
      return valuePortability.equals(value, result.getValue());
    } finally {
      result.close();
    }
  }

  // Called under segment lock
  @Override
  public K readKey(final long encoding, int hashCode) {
    Entry result = readEntry(encoding);
    try {
      return (K) keyPortability.decode(result.getKey());
    } finally {
      result.close();
    }
  }

  // Called under segment lock
  @Override
  public boolean equalsKey(Object key, final long encoding) {
    Entry result = readEntry(encoding);
    try {
      return keyPortability.equals(key, result.getKey());
    } finally {
      result.close();
    }
  }

  // Called under segment lock
  @Override
  public boolean equalsBinaryKey(ByteBuffer offheapBinaryKey, final long encoding) {
    Entry result = readEntry(encoding);
    try {
      return equalsBinaryKey(offheapBinaryKey, result.getKey());
    } finally {
      result.close();
    }
  }
  
  protected final boolean equalsBinaryKey(ByteBuffer probe, ByteBuffer stored) {
      return probe.equals(stored.duplicate()) || keyPortability.equals(keyPortability.decode(probe.duplicate()), stored.duplicate());
  }
  
  // Called under segment lock  
  @Override
  public void clear() {
    lsnFirst = NULL_ENCODING;
    lsnLast = NULL_ENCODING;
    dataSize = 0;
    restartabilityDelete();
    metadataArea.clear();
    validChain();
    fireCleared();
  }

  private void restartabilityDelete() {
    try {
      transactionSource.beginTransaction(synchronous).delete(identifier).commit();
    } catch (TransactionException e) {
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public long getAllocatedMemory() {
    return metadataArea.getAllocatedMemory();
  }

  @Override
  public long getOccupiedMemory() {
    return metadataArea.getOccupiedMemory();
  }

  @Override
  public long getVitalMemory() {
    return metadataArea.getAllocatedMemory();
  }

  @Override
  public long getDataSize() {
    return dataSize;
  }

  @Override
  public void invalidateCache() {
    //no-op - there is no caching of portable forms yet
  }

  @Override
  public void bind(Owner owner) {
    this.owner = owner;
  }

  @Override
  public void destroy() {
    metadataArea.destroy();
  }

  @Override
  public boolean shrink() {
    return metadataArea.shrink();
  }

  protected final void assignLsn(long encoding, long lsn) {
    unlinkNode(encoding);
    linkNodeExpectingLast(encoding, lsn);
    metadataArea.writeLong(encoding + META_LSN_OFFSET, lsn);
    holdingArea.remove(encoding);
    validChain();
  }
  
  protected final void unlinkNode(long encoding) {
    long next = metadataArea.readLong(encoding + META_NEXT_OFFSET);
    long prev = metadataArea.readLong(encoding + META_PREVIOUS_OFFSET);
    if (lsnLast == encoding) {
      lsnLast = prev;
    }
    if (lsnFirst == encoding) {
      lsnFirst = next;
    }
    if (next != NULL_ENCODING) {
      metadataArea.writeLong(next + META_PREVIOUS_OFFSET, prev);
    }
    if (prev != NULL_ENCODING) {
      metadataArea.writeLong(prev + META_NEXT_OFFSET, next);
    }

    metadataArea.writeLong(encoding + META_NEXT_OFFSET, NULL_ENCODING);
    metadataArea.writeLong(encoding + META_PREVIOUS_OFFSET, NULL_ENCODING);
  }

  protected final void linkNodeExpectingLast(long node, long lsn) {
    if (lsn < 0) {
      throw new AssertionError("Received illegal lsn " + lsn);
    }

    if (lsnLast == NULL_ENCODING) {
      //insertion in empty list
      validate(!VALIDATING || lsnFirst == NULL_ENCODING);
      lsnLast = node;
      lsnFirst = node;
      return;
    }

    //insertion in non-empty list
    long previous = lsnLast;
    long next;
    while (true) {
      if (metadataArea.readLong(previous + META_LSN_OFFSET) < lsn) {
        next = metadataArea.readLong(previous + META_NEXT_OFFSET);
        break;
      } else if (metadataArea.readLong(previous + META_PREVIOUS_OFFSET) == NULL_ENCODING) {
        next = previous;
        previous = NULL_ENCODING;
        break;
      }
      previous = metadataArea.readLong(previous + META_PREVIOUS_OFFSET);
    }

    if (next == NULL_ENCODING) {
      //insertion at last
      validate(!VALIDATING || previous == lsnLast);
      lsnLast = node;
      metadataArea.writeLong(node + META_PREVIOUS_OFFSET, previous);
      metadataArea.writeLong(previous + META_NEXT_OFFSET, node);
    } else {
      if (previous == NULL_ENCODING) {
        //insertion at first
        validate(!VALIDATING || next == lsnFirst);
        lsnFirst = node;
        metadataArea.writeLong(node + META_NEXT_OFFSET, next);
        metadataArea.writeLong(next + META_PREVIOUS_OFFSET, node);
      } else {
        //insertion in middle
        metadataArea.writeLong(node + META_NEXT_OFFSET, next);
        metadataArea.writeLong(node + META_PREVIOUS_OFFSET, previous);

        metadataArea.writeLong(previous + META_NEXT_OFFSET, node);
        metadataArea.writeLong(next + META_PREVIOUS_OFFSET, node);
      }
    }
  }
  
  protected final void linkNodeExpectingFirst(long node, long lsn) {
    if (lsn < 0) {
      throw new AssertionError("Received illegal lsn " + lsn);
    }
    
    if (lsnLast == NULL_ENCODING) {
      //insertion in empty list
      validate(!VALIDATING || lsnFirst == NULL_ENCODING);
      lsnLast = node;
      lsnFirst = node;
      return;
    }
    
    //insertion in non-empty list
    long next = lsnFirst;
    long previous;
    while (true) {
      if (metadataArea.readLong(next + META_LSN_OFFSET) > lsn) {
        previous = metadataArea.readLong(next + META_PREVIOUS_OFFSET);
        break;
      } else if (metadataArea.readLong(next + META_NEXT_OFFSET) == NULL_ENCODING) {
        previous = next;
        next = NULL_ENCODING;
        break;
      }
      next = metadataArea.readLong(next + META_NEXT_OFFSET);
    }

    if (previous == NULL_ENCODING) {
      //insertion at first
      validate(!VALIDATING || next == lsnFirst);
      lsnFirst = node;
      metadataArea.writeLong(node + META_NEXT_OFFSET, next);
      metadataArea.writeLong(next + META_PREVIOUS_OFFSET, node);
    } else {
      if (next == NULL_ENCODING) {
        //insertion at last
        validate(!VALIDATING || previous == lsnLast);
        lsnLast = node;
        metadataArea.writeLong(node + META_PREVIOUS_OFFSET, previous);
        metadataArea.writeLong(previous + META_NEXT_OFFSET, node);
      } else {
        //insertion in middle
        metadataArea.writeLong(node + META_PREVIOUS_OFFSET, previous);
        metadataArea.writeLong(node + META_NEXT_OFFSET, next);
        
        metadataArea.writeLong(next + META_PREVIOUS_OFFSET, node);
        metadataArea.writeLong(previous + META_NEXT_OFFSET, node);
      }
    }
  }
  
  protected final void validChain() {
    if (VALIDATING) {
      long previous = NULL_ENCODING;
      long current = lsnFirst;

      while (true) {
        if (current == NULL_ENCODING) {
          validate(lsnLast == previous);
          break;
        }

        validate(!VALIDATING || metadataArea.readLong(current + META_PREVIOUS_OFFSET) == previous);
        if (previous != NULL_ENCODING) {
          validate(metadataArea.readLong(previous + META_NEXT_OFFSET) == current);
        }
        previous = current;
        current = metadataArea.readLong(previous + META_NEXT_OFFSET);
      }
    }
  }

  //Expect locking in caller...
  protected long firstEncoding() {
    return lsnFirst;
  }
  
  //Expect locking in caller...
  protected long lastEncoding() {
    return lsnLast;
  }

  @Override
  public long size() {
    return owner.getSize();
  }

  @Override
  public Long getLowestLsn() {
    Lock l = owner.readLock();
    l.lock();
    try {
      long lowest = firstEncoding();
      if (lowest == NULL_ENCODING) {
        return null;
      } else {
        return metadataArea.readLong(lowest + META_LSN_OFFSET);
      }
    } finally {
      l.unlock();
    }
  }

  @Override
  public Long getLsn(int pojoHash, ByteBuffer frsBinaryKey) {
    ByteBuffer offheapBinaryKey = decodeKey(frsBinaryKey);
    Lock l = owner.readLock();
    l.lock();
    try {
      Long encoding = lookupEncoding(pojoHash, offheapBinaryKey);
      if (encoding == null) {
        return null;
      } else {
        return metadataArea.readLong(encoding + META_LSN_OFFSET);
      }
    } finally {
      l.unlock();
    }
  }

  @Override
  public void put(int pojoHash, ByteBuffer frsBinaryKey, ByteBuffer frsBinaryValue, long lsn) {
    //this encoding *must* be valid... this is not installed in the map yet...
    long encoding = extractEncoding(frsBinaryValue);
    Lock l = owner.writeLock();
    l.lock();
    try {
      assignLsn(encoding, lsn);
    } finally {
      l.unlock();
    }
  }

  @Override
  public void remove(int pojoHash, ByteBuffer frsBinaryKey) {
    //lookupEncoding calls to the owner map which will do it's own locking
    validate(!VALIDATING || lookupEncoding(pojoHash, decodeKey(frsBinaryKey)) != null);
  }

  @Override
  public void replayPut(final int pojoHash, ByteBuffer frsBinaryKey, ByteBuffer frsBinaryValue, long lsn) {
    int metadata = extractMetadata(frsBinaryValue);
    final ByteBuffer offheapBinaryKey = decodeKey(frsBinaryKey);
    final ByteBuffer offheapBinaryValue = decodeValue(frsBinaryValue);
    
    Lock l = owner.writeLock();
    l.lock();
    try {
      final long encoding = owner.installMappingForHashAndEncoding(pojoHash, offheapBinaryKey, offheapBinaryValue, metadata);
      linkNodeExpectingFirst(encoding, lsn);
      metadataArea.writeLong(encoding + META_LSN_OFFSET, lsn);
      validChain();
      if (hasRecoveryListeners()) {
        final Thread caller = Thread.currentThread();
        fireRecovered(new Callable<K>() {
          @Override
          public K call() throws Exception {
            if (caller == Thread.currentThread()) {
              return (K) keyPortability.decode(offheapBinaryKey.duplicate());
            } else {
              throw new IllegalStateException();
            }
          }
        }, new Callable<V>() {
          @Override
          public V call() throws Exception {
            if (caller == Thread.currentThread()) {
              return (V) valuePortability.decode(offheapBinaryValue.duplicate());
            } else {
              throw new IllegalStateException();
            }
          }
        }, offheapBinaryKey.duplicate(), offheapBinaryValue.duplicate(), pojoHash, metadata, encoding);
      }
    } finally {
      l.unlock();
    }
  }

  @Override
  public Long writeBinaryMapping(ByteBuffer binaryKey, ByteBuffer binaryValue, int pojoHash, int metadata) {
    return writePartialEntry(pojoHash, binaryKey, binaryValue);
  }

  @Override
  public Long writeBinaryMapping(ByteBuffer[] binaryKey, ByteBuffer[] binaryValue, int pojoHash, int metadata) {
    return writePartialEntry(pojoHash, aggregate(binaryKey), aggregate(binaryValue));
  }

  @Override
  public int readKeyHash(long encoding) {
    return metadataArea.readInt(encoding + META_KEY_HASH_OFFSET);
  }

  @Override
  public ByteBuffer readBinaryKey(final long encoding) {
    Entry result = readEntry(encoding);
    try {
      return detach(result.getKey());
    } finally {
      result.close();
    }
  }

  @Override
  public ByteBuffer readBinaryValue(final long encoding) {
    Entry result = readEntry(encoding);
    try {
      return detach(result.getValue());
    } finally {
      result.close();
    }
  }
  
  protected static ByteBuffer detach(ByteBuffer attached) {
    ByteBuffer detached = ByteBuffer.allocate(attached.remaining());
    detached.put(attached).flip();
    return detached;
  }
  
  protected Entry readEntry(long encoding) {
    Map.Entry<ByteBuffer, ByteBuffer> e = holdingArea.get(encoding);
    if (e != null) {
      return new DetachedEntry(e.getKey().duplicate(), e.getValue().duplicate());
    } else {
      long lsn = metadataArea.readLong(encoding + META_LSN_OFFSET);
      Tuple<?, ByteBuffer, ByteBuffer> result = transactionSource.get(lsn);
      return new DecodedEntry(result);
    }
  }
  
  @Override
  public ObjectManagerEntry<I, ByteBuffer, ByteBuffer> acquireCompactionEntry(long ceilingLsn) {
    Lock l = owner.writeLock();
    l.lock();
    long encoding = firstEncoding();
    if (encoding == NULL_ENCODING) {
      l.unlock();
      return null;
    }
    try {
      long lsn = metadataArea.readLong(encoding + META_LSN_OFFSET);
      if (lsn >= ceilingLsn) {
        l.unlock();
        return null;
      }

      // This exit means that the current thread will be holding the segment lock.
      return compactingEntry = new SimpleObjectManagerEntry<I, ByteBuffer, ByteBuffer>(identifier,
                                                                                encodeKey(readBinaryKey(encoding), readKeyHash(encoding)),
                                                                                encodeValue(readBinaryValue(encoding), encoding, deriveMetadata(encoding)),
                                                                                lsn);
    } catch (RuntimeException e) {
      l.unlock();
      throw e;
    } catch (Error e) {
      l.unlock();
      throw e;
    } catch (Throwable e) {
      l.unlock();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void releaseCompactionEntry(ObjectManagerEntry<I, ByteBuffer, ByteBuffer> entry) {
    if (entry == null) {
      throw new NullPointerException("Tried to release a null entry.");
    }
    if (entry != compactingEntry) {
      throw new IllegalArgumentException("Released entry is not the same as acquired entry.");
    }
    compactingEntry = null;
    // Releasing the lock acquired in acquireFirstEntry().
    owner.writeLock().unlock();
  }

  @Override
  public void updateLsn(int pojoHash, ObjectManagerEntry<I, ByteBuffer, ByteBuffer> entry, long newLsn) {
    // Expected to be called under the segment lock.
    if (entry != compactingEntry) {
      throw new IllegalArgumentException(
              "Tried to update the LSN on an entry that was not acquired.");
    }
    long encoding = extractEncoding(entry.getValue());
    validate(!VALIDATING || metadataArea.readLong(encoding + META_LSN_OFFSET) == entry.getLsn());
    assignLsn(encoding, newLsn);
  }

  private Long lookupEncoding(int hash, ByteBuffer offHeapBinaryKey) {
    return owner.getEncodingForHashAndBinary(hash, offHeapBinaryKey);
  }
  
  protected int deriveMetadata(long encoding) {
    return 0;
  }

  @Override
  public long sizeInBytes() {
    return metadataArea.getOccupiedMemory();
  }  

  protected interface Entry extends Closeable {

    ByteBuffer getKey();
    
    ByteBuffer getValue();
    
    @Override
    void close();
  }

  private static class DetachedEntry implements Entry {

    private final ByteBuffer key;
    private final ByteBuffer value;

    public DetachedEntry(ByteBuffer key, ByteBuffer value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public ByteBuffer getKey() {
      return key;
    }

    @Override
    public ByteBuffer getValue() {
      return value;
    }

    @Override
    public void close() {
      //no-op
    }
  }

  private static class DecodedEntry implements Entry {

    private final Tuple<?, ?, ?> encoded;
    private final ByteBuffer key;
    private final ByteBuffer value;
    
    public DecodedEntry(Tuple<?, ByteBuffer, ByteBuffer> encoded) {
      this.encoded = encoded;
      this.key = decodeKey(encoded.getKey());
      this.value = decodeValue(encoded.getValue());
    }

    @Override
    public ByteBuffer getKey() {
      return key;
    }

    @Override
    public ByteBuffer getValue() {
      return value;
    }

    @Override
    public void close() {
      if (encoded instanceof Disposable) {
        ((Disposable) encoded).dispose();
      }
    }
  }

  class MetadataOwner implements OffHeapStorageArea.Owner {

    @Override
    public boolean evictAtAddress(long address, boolean shrink) {
      int hash = readKeyHash(address);
      int slot = owner.getSlotForHashAndEncoding(hash, address, ~0);
      return owner.evict(slot, shrink);
    }

    @Override
    public Lock writeLock() {
      return owner.writeLock();
    }

    @Override
    public boolean isThief() {
      return owner.isThiefForTableAllocations();
    }

    @Override
    public boolean moved(long from, long to) {
      if (owner.updateEncoding(readKeyHash(to), from, to, ~0)) {
        if (lsnLast == from) {
          lsnLast = to;
        }
        if (lsnFirst == from) {
          lsnFirst = to;
        }
        long next = metadataArea.readLong(to + META_NEXT_OFFSET);
        if (next != NULL_ENCODING) {
          metadataArea.writeLong(next + META_PREVIOUS_OFFSET, to);
        }
        long prev = metadataArea.readLong(to + META_PREVIOUS_OFFSET);
        if (prev != NULL_ENCODING) {
          metadataArea.writeLong(prev + META_NEXT_OFFSET, to);
        }
        validChain();
        return true;
      } else {
        return false;
      }
    }

    @Override
    public int sizeOf(long address) {
      return getActualEntrySize(address);
    }
  }
}

