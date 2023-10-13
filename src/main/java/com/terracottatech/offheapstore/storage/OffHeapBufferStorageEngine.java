/*
 * All content copyright (c) 2010-2011 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore.storage;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;

import com.terracottatech.offheapstore.paging.OffHeapStorageArea;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.storage.portability.Portability;
import com.terracottatech.offheapstore.storage.portability.WriteContext;
import com.terracottatech.offheapstore.util.DebuggingUtils;
import com.terracottatech.offheapstore.util.Factory;

import static com.terracottatech.offheapstore.util.ByteBufferUtils.totalLength;

/**
 * A generic ByteBuffer based key/value store.
 * <p>
 * This storage engine implementation uses {@link Portability} instances to
 * convert key/value instances in to ByteBuffers.  The content of these
 * ByteBuffers are then stored in slices of a single large data area, whose
 * space allocation is governed by an external {@link Allocator}.
 *
 * @param <K> key type handled by this engine
 * @param <V> value type handled by this engine
 *
 * @author Chris Dennis
 */
public class OffHeapBufferStorageEngine<K, V> extends PortabilityBasedStorageEngine<K, V> implements OffHeapStorageArea.Owner {

  private static final int KEY_HASH_OFFSET = 0;
  private static final int KEY_LENGTH_OFFSET = 4;
  private static final int VALUE_LENGTH_OFFSET = 8;
  private static final int DATA_OFFSET = 12;
  private static final int HEADER_SIZE = DATA_OFFSET;

  /*
   * Future design ideas:
   *
   * We might want to look in to supporting shrinking of the data areas, in case
   * our storage demands change (e.g. due to a change in key distribution, or
   * change in value types).
   */

  public static <K, V> Factory<OffHeapBufferStorageEngine<K, V>> createFactory(final PointerSize width, final PageSource source, final int pageSize, final Portability<? super K> keyPortability, final Portability<? super V> valuePortability, final boolean thief, final boolean victim) {
    return new Factory<OffHeapBufferStorageEngine<K, V>>() {

      @Override
      public OffHeapBufferStorageEngine<K, V> newInstance() {
        return new OffHeapBufferStorageEngine<K, V>(width, source, pageSize, keyPortability, valuePortability, thief, victim);
      }
    };
  }

  public static <K, V> Factory<OffHeapBufferStorageEngine<K, V>> createFactory(final PointerSize width, final PageSource source, final int pageSize, final Portability<? super K> keyPortability, final Portability<? super V> valuePortability, final boolean thief, final boolean victim, final float compressThreshold) {
    return new Factory<OffHeapBufferStorageEngine<K, V>>() {

      @Override
      public OffHeapBufferStorageEngine<K, V> newInstance() {
        return new OffHeapBufferStorageEngine<K, V>(width, source, pageSize, keyPortability, valuePortability, thief, victim, compressThreshold);
      }
    };
  }

  public static <K, V> Factory<OffHeapBufferStorageEngine<K, V>> createFactory(final PointerSize width, final PageSource source, final int initialPageSize, final int maximalPageSize, final Portability<? super K> keyPortability, final Portability<? super V> valuePortability, final boolean thief, final boolean victim) {
    return new Factory<OffHeapBufferStorageEngine<K, V>>() {

      @Override
      public OffHeapBufferStorageEngine<K, V> newInstance() {
        return new OffHeapBufferStorageEngine<K, V>(width, source, initialPageSize, maximalPageSize, keyPortability, valuePortability, thief, victim);
      }
    };
  }
  
  public static <K, V> Factory<OffHeapBufferStorageEngine<K, V>> createFactory(final PointerSize width, final PageSource source, final int initialPageSize, final int maximalPageSize, final Portability<? super K> keyPortability, final Portability<? super V> valuePortability, final boolean thief, final boolean victim, final float compressThreshold) {
    return new Factory<OffHeapBufferStorageEngine<K, V>>() {

      @Override
      public OffHeapBufferStorageEngine<K, V> newInstance() {
        return new OffHeapBufferStorageEngine<K, V>(width, source, initialPageSize, maximalPageSize, keyPortability, valuePortability, thief, victim, compressThreshold);
      }
    };
  }

  protected final OffHeapStorageArea storageArea;
  
  protected volatile Owner owner;

  /**
   * Creates a storage engine using the given allocator, and portabilities.
   * 
   * @param allocator allocator used for storage allocation
   * @param keyPortability key type portability
   * @param valuePortability value type portability
   */
  public OffHeapBufferStorageEngine(PointerSize width, PageSource source, int pageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability) {
    this(width, source, pageSize, pageSize, keyPortability, valuePortability);
  }

  public OffHeapBufferStorageEngine(PointerSize width, PageSource source, int pageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability, float compressThreshold) {
    this(width, source, pageSize, pageSize, keyPortability, valuePortability, compressThreshold);
  }
  
  public OffHeapBufferStorageEngine(PointerSize width, PageSource source, int initialPageSize, int maximalPageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability) {
    this(width, source, initialPageSize, maximalPageSize, keyPortability, valuePortability, false, false);
  }
  
  public OffHeapBufferStorageEngine(PointerSize width, PageSource source, int initialPageSize, int maximalPageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability, float compressThreshold) {
    this(width, source, initialPageSize, maximalPageSize, keyPortability, valuePortability, false, false, compressThreshold);
  }

  public OffHeapBufferStorageEngine(PointerSize width, PageSource source, int pageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean thief, boolean victim) {
    this(width, source, pageSize, pageSize, keyPortability, valuePortability, thief, victim);
  }
  
  public OffHeapBufferStorageEngine(PointerSize width, PageSource source, int pageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean thief, boolean victim, float compressThreshold) {
    this(width, source, pageSize, pageSize, keyPortability, valuePortability, thief, victim, compressThreshold);
  }
  
  public OffHeapBufferStorageEngine(PointerSize width, PageSource source, int initialPageSize, int maximalPageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean thief, boolean victim) {
    super(keyPortability, valuePortability);
    this.storageArea = new OffHeapStorageArea(width, this, source, initialPageSize, maximalPageSize, thief, victim);
  }

  public OffHeapBufferStorageEngine(PointerSize width, PageSource source, int initialPageSize, int maximalPageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean thief, boolean victim, float compressThreshold) {
    super(keyPortability, valuePortability);
    this.storageArea = new OffHeapStorageArea(width, this, source, initialPageSize, maximalPageSize, thief, victim, compressThreshold);
  }
  
  @Override
  protected void clearInternal() {
    storageArea.clear();
  }

  @Override
  protected void free(long address) {
    storageArea.free(address);
  }

  @Override
  public ByteBuffer readKeyBuffer(long address) {
    int length = storageArea.readInt(address + KEY_LENGTH_OFFSET);
    return storageArea.readBuffer(address + DATA_OFFSET, length).asReadOnlyBuffer();
  }

  @Override
  protected WriteContext getKeyWriteContext(long address) {
    int keyLength = storageArea.readInt(address + KEY_LENGTH_OFFSET);
    return getWriteContext(address + DATA_OFFSET, keyLength);
  }
  
  @Override
  public ByteBuffer readValueBuffer(long address) {
    int keyLength = storageArea.readInt(address + KEY_LENGTH_OFFSET);
    int valueLength = storageArea.readInt(address + VALUE_LENGTH_OFFSET);
    return storageArea.readBuffer(address + DATA_OFFSET + keyLength, valueLength).asReadOnlyBuffer();
  }

  @Override
  protected WriteContext getValueWriteContext(long address) {
    int keyLength = storageArea.readInt(address + KEY_LENGTH_OFFSET);
    int valueLength = storageArea.readInt(address + VALUE_LENGTH_OFFSET);
    return getWriteContext(address + DATA_OFFSET + keyLength, valueLength);
  }
  
  private WriteContext getWriteContext(final long address, final int max) {
    return new WriteContext() {
      
      @Override
      public void setLong(int offset, long value) {
        if (offset < 0 || offset > max) {
          throw new IllegalArgumentException();
        } else {
          storageArea.writeLong(address + offset, value);
        }
      }
      
      @Override
      public void flush() {
        //no-op
      }
    };
  }
  @Override
  protected Long writeMappingBuffers(ByteBuffer keyBuffer, ByteBuffer valueBuffer, int hash) {
    int keyLength = keyBuffer.remaining();
    int valueLength = valueBuffer.remaining();
    long address = storageArea.allocate(keyLength + valueLength + HEADER_SIZE);

    if (address >= 0) {
      storageArea.writeInt(address + KEY_HASH_OFFSET, hash);
      storageArea.writeInt(address + KEY_LENGTH_OFFSET, keyLength);
      storageArea.writeInt(address + VALUE_LENGTH_OFFSET, valueLength);
      storageArea.writeBuffer(address + DATA_OFFSET, keyBuffer);
      storageArea.writeBuffer(address + DATA_OFFSET + keyLength, valueBuffer);
      return address;
    } else {
      return null;
    }
  }
  
  @Override
  protected Long writeMappingBuffersGathering(ByteBuffer[] keyBuffers, ByteBuffer[] valueBuffers, int hash) {
    int keyLength = totalLength(keyBuffers);
    int valueLength = totalLength(valueBuffers);
    long address = storageArea.allocate(keyLength + valueLength + HEADER_SIZE);

    if (address >= 0) {
      storageArea.writeInt(address + KEY_HASH_OFFSET, hash);
      storageArea.writeInt(address + KEY_LENGTH_OFFSET, keyLength);
      storageArea.writeInt(address + VALUE_LENGTH_OFFSET, valueLength);
      storageArea.writeBuffers(address + DATA_OFFSET, keyBuffers);
      storageArea.writeBuffers(address + DATA_OFFSET + keyLength, valueBuffers);
      return address;
    } else {
      return null;
    }
  }
  
  @Override
  public long getAllocatedMemory() {
    return storageArea.getAllocatedMemory();
  }

  @Override
  public long getOccupiedMemory() {
    return storageArea.getOccupiedMemory();
  }

  @Override
  public long getVitalMemory() {
    return getAllocatedMemory();
  }

  @Override
  public long getDataSize() {
    //TODO This is an overestimate.
    return getOccupiedMemory();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("OffHeapBufferStorageEngine ");
    sb.append("allocated=").append(DebuggingUtils.toBase2SuffixedString(getAllocatedMemory())).append("B ");
    sb.append("occupied=").append(DebuggingUtils.toBase2SuffixedString(getOccupiedMemory())).append("B\n");
    sb.append("Storage Area: ").append(storageArea);
    return sb.toString();
  }

  @Override
  public void destroy() {
    storageArea.destroy();
  }

  @Override
  public boolean shrink() {
    return storageArea.shrink();
  }

  @Override
  public boolean evictAtAddress(long address, boolean shrink) {
    int hash = storageArea.readInt(address + KEY_HASH_OFFSET);
    int slot = owner.getSlotForHashAndEncoding(hash, address, ~0);
    return owner.evict(slot, shrink);
  }

  @Override
  public boolean isThief() {
    return owner.isThiefForTableAllocations();
  }

  @Override
  public int readKeyHash(long encoding) {
    return storageArea.readInt(encoding + KEY_HASH_OFFSET);
  }
  
  @Override
  public boolean moved(long from, long to) {
    return owner.updateEncoding(readKeyHash(to), from, to, ~0);
  }

  @Override
  public int sizeOf(long address) {
    int keyLength = storageArea.readInt(address + KEY_LENGTH_OFFSET);
    int valueLength = storageArea.readInt(address + VALUE_LENGTH_OFFSET);
    return DATA_OFFSET + keyLength + valueLength;
  }

  @Override
  public void bind(Owner m) {
    if (owner != null) {
      throw new AssertionError();
    }
    owner = m;
  }

  @Override
  public Lock writeLock() {
    return owner.writeLock();
  }
}
