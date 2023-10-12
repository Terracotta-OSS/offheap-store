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
package org.terracotta.offheapstore.storage;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.locks.Lock;

import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.StorageEngine.Owner;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.util.DebuggingUtils;
import org.terracotta.offheapstore.util.Factory;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;

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
    return () -> new OffHeapBufferHalfStorageEngine<>(source, initialPageSize, maximalPageSize, portability, thief, victim);
  }

  private volatile Owner owner;
  private volatile long mask;
  private final OffHeapStorageArea storageArea;

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
    return "OffHeapBufferStorageEngine " + "allocated=" + DebuggingUtils.toBase2SuffixedString(getAllocatedMemory()) + "B " +
                "occupied=" + DebuggingUtils.toBase2SuffixedString(getOccupiedMemory()) + "B\n" +
                "Allocator: " + storageArea;
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
  public Collection<Long> evictAtAddress(long address, boolean shrink) {
    int hash = storageArea.readInt(address + KEY_HASH_OFFSET);
    int slot = owner.getSlotForHashAndEncoding(hash, address, mask);
    if (owner.evict(slot, shrink)) {
      return singleton(address);
    } else {
      return emptyList();
    }
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
