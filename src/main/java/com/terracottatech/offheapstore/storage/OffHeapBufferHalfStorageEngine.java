/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.storage;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;

import com.terracottatech.offheapstore.paging.OffHeapStorageArea;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.storage.StorageEngine.Owner;
import com.terracottatech.offheapstore.storage.portability.Portability;
import com.terracottatech.offheapstore.util.DebuggingUtils;
import com.terracottatech.offheapstore.util.Factory;

/**
 *
 * @author cdennis
 */
public class OffHeapBufferHalfStorageEngine<T> extends PortabilityBasedHalfStorageEngine<T> implements OffHeapStorageArea.Owner {

  private static final int KEY_HASH_OFFSET = 0;
  private static final int LENGTH_OFFSET = 4;
  private static final int DATA_OFFSET = 8;
  private static final int HEADER_LENGTH = DATA_OFFSET;

  public static <T> Factory<OffHeapBufferHalfStorageEngine<T>> createFactory(final PageSource source, final int pageSize, final Portability<? super T> portability) {
    return createFactory(source, pageSize, portability, false, false);
  }

  public static <T> Factory<OffHeapBufferHalfStorageEngine<T>> createFactory(final PageSource source, final int pageSize, final Portability<? super T> portability, final boolean thief, final boolean victim) {
    return createFactory(source, pageSize, pageSize, portability, thief, victim);
  }

  public static <T> Factory<OffHeapBufferHalfStorageEngine<T>> createFactory(final PageSource source, final int initialPageSize, final int maximalPageSize, final Portability<? super T> portability, final boolean thief, final boolean victim) {
    return new Factory<OffHeapBufferHalfStorageEngine<T>>() {
      @Override
      public OffHeapBufferHalfStorageEngine<T> newInstance() {
        return new OffHeapBufferHalfStorageEngine<T>(source, initialPageSize, maximalPageSize, portability, thief, victim);
      }
    };
  }

  private volatile Owner owner;
  private volatile long mask;
  private final OffHeapStorageArea storageArea;

  /**
   * Creates a storage engine using the given allocator, and portabilities.
   *
   * @param allocator allocator used for storage allocation
   * @param keyPortability key type portability
   * @param valuePortability value type portability
   */
  public OffHeapBufferHalfStorageEngine(PageSource source, int pageSize, Portability<? super T> portability) {
    this(source, pageSize, portability, false, false);
  }

  public OffHeapBufferHalfStorageEngine(PageSource source, int pageSize, Portability<? super T> portability, boolean thief, boolean victim) {
    this(source, pageSize, pageSize, portability, thief, victim);
  }

  public OffHeapBufferHalfStorageEngine(PageSource source, int initialPageSize, int maximalPageSize, Portability<? super T> portability, boolean thief, boolean victim) {
    super(portability);
    this.storageArea = new OffHeapStorageArea(PointerSize.INT, this, source, initialPageSize, maximalPageSize, thief, victim);
  }

  @Override
  public void clear() {
    storageArea.clear();
  }

  @Override
  public void free(int address) {
    storageArea.free(address);
  }

  @Override
  protected ByteBuffer readBuffer(int address) {
    int length = storageArea.readInt(address + LENGTH_OFFSET);
    return storageArea.readBuffer(address + DATA_OFFSET, length);
  }

  @Override
  protected Integer writeBuffer(ByteBuffer buffer, int hash) {
    int length = buffer.remaining();
    int address = (int) storageArea.allocate(length + HEADER_LENGTH);

    if (address >= 0) {
      storageArea.writeInt(address + KEY_HASH_OFFSET, hash);
      storageArea.writeInt(address + LENGTH_OFFSET, length);
      storageArea.writeBuffer(address + DATA_OFFSET, buffer);
      return Integer.valueOf(address);
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
    sb.append("Allocator: ").append(storageArea);
    return sb.toString();
  }

  @Override
  public void bind(Owner o, long m) {
    if (owner != null) {
      throw new AssertionError();
    }
    owner = o;
    mask = m;
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
    int slot = owner.getSlotForHashAndEncoding(hash, address, mask);
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
    int hash = storageArea.readInt(to + KEY_HASH_OFFSET);
    return owner.updateEncoding(hash, from, to, mask);
  }

  @Override
  public int sizeOf(long address) {
    int length = storageArea.readInt(address + LENGTH_OFFSET);
    return HEADER_LENGTH + length;
  }
}
