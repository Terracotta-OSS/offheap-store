/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.storage;

import com.terracottatech.offheapstore.util.Factory;

/**
 * A {@code Long} key storage engine.
 * <p>
 * This engine stores long keys as their primitive representations split between
 * the hashCode and value fields.
 *
 * @author Chris Dennis
 */
public class LongStorageEngine<V> implements StorageEngine<Long, V> {

  public static <V> Factory<LongStorageEngine<V>> createFactory(final Factory<? extends HalfStorageEngine<V>> valueFactory) {
    return new Factory<LongStorageEngine<V>>() {
      @Override
      public LongStorageEngine<V> newInstance() {
        return new LongStorageEngine<V>(valueFactory.newInstance());
      }
    };
  }

  private final HalfStorageEngine<V> valueStorage;

  public LongStorageEngine(HalfStorageEngine<V> valueStorage) {
    this.valueStorage = valueStorage;
  }

  @Override
  public Long writeMapping(Long key, V value, int hash, int metadata) {
    Integer valueEncoding = valueStorage.write(value, hash);
    if (valueEncoding == null) {
      return null;
    } else {
      return SplitStorageEngine.encoding(key.intValue(), valueEncoding);
    }
  }

  @Override
  public void attachedMapping(long encoding, int hash, int metadata) {
    //no-op
  }
  
  @Override
  public void freeMapping(long encoding, int hash, boolean removal) {
    valueStorage.free(SplitStorageEngine.valueEncoding(encoding));
  }

  @Override
  public V readValue(long encoding) {
    return valueStorage.read(SplitStorageEngine.valueEncoding(encoding));
  }

  @Override
  public boolean equalsValue(Object value, long encoding) {
    return valueStorage.equals(value, SplitStorageEngine.valueEncoding(encoding));
  }

  @Override
  public Long readKey(long encoding, int hashCode) {
    int keyEncoding = SplitStorageEngine.keyEncoding(encoding);
    return (((long) (hashCode ^ keyEncoding)) << 32) | (keyEncoding & 0xffffffffL);
  }

  @Override
  public boolean equalsKey(Object key, long encoding) {
    if (key instanceof Long) {
      return ((Long) key).intValue() == SplitStorageEngine.keyEncoding(encoding);
    } else {
      return false;
    }
  }

  @Override
  public void clear() {
    //no-op
  }

  @Override
  public long getAllocatedMemory() {
    return valueStorage.getAllocatedMemory();
  }

  @Override
  public long getOccupiedMemory() {
    return valueStorage.getOccupiedMemory();
  }

  @Override
  public long getVitalMemory() {
    return valueStorage.getVitalMemory();
  }

  @Override
  public long getDataSize() {
    return valueStorage.getDataSize();
  }

  @Override
  public String toString() {
    return "LongStorageEngine : " + valueStorage;
  }

  @Override
  public void invalidateCache() {
    valueStorage.invalidateCache();
  }

  @Override
  public void bind(Owner owner) {
    valueStorage.bind(owner, ~0L >>> Integer.SIZE);
  }

  @Override
  public void destroy() {
    valueStorage.destroy();
  }

  @Override
  public boolean shrink() {
    return valueStorage.shrink();
  }
}
