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
package org.terracotta.offheapstore.storage;

import org.terracotta.offheapstore.util.Factory;

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
        return new LongStorageEngine<>(valueFactory.newInstance());
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
    return key instanceof Long && ((Long) key).intValue() == SplitStorageEngine.keyEncoding(encoding);
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
