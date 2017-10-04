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
 * A {@code StorageEngine} composed of two independent engines, one for the
 * keys, one for the values.
 *
 * @param <K> key type handled by this engine
 * @param <V> value type handled by this engine
 *
 * @author Chris Dennis
 */
public class SplitStorageEngine<K, V> implements StorageEngine<K, V> {

  public static <K, V> Factory<SplitStorageEngine<K, V>> createFactory(final Factory<? extends HalfStorageEngine<K>> keyFactory, final Factory<? extends HalfStorageEngine<V>> valueFactory) {
    return () -> new SplitStorageEngine<>(keyFactory.newInstance(), valueFactory.newInstance());
  }

  protected final HalfStorageEngine<? super K> keyStorageEngine;
  protected final HalfStorageEngine<? super V> valueStorageEngine;

  /**
   * Creates a composite storage engine, with independent key and value engines.
   *
   * @param keyStorageEngine storage engine for the keys
   * @param valueStorageEngine storage engine for the values
   */
  public SplitStorageEngine(HalfStorageEngine<? super K> keyStorageEngine, HalfStorageEngine<? super V> valueStorageEngine) {
    this.keyStorageEngine = keyStorageEngine;
    this.valueStorageEngine = valueStorageEngine;
  }

  @Override
  public Long writeMapping(K key, V value, int hash, int metadata) {
    Integer keyEncoding = keyStorageEngine.write(key, hash);
    if (keyEncoding == null) {
      return null;
    }

    Integer valueEncoding = valueStorageEngine.write(value, hash);
    if (valueEncoding == null) {
      keyStorageEngine.free(keyEncoding);
      return null;
    }

    return encoding(keyEncoding, valueEncoding);
  }

  @Override
  public void attachedMapping(long encoding, int hash, int metadata) {
    //no-op
  }

  @Override
  public void freeMapping(long encoding, int hash, boolean removal) {
    keyStorageEngine.free(keyEncoding(encoding));
    valueStorageEngine.free(valueEncoding(encoding));
  }

  @SuppressWarnings("unchecked")
  @Override
  public V readValue(long encoding) {
    return (V) valueStorageEngine.read(valueEncoding(encoding));
  }

  @Override
  public boolean equalsValue(Object value, long encoding) {
    return valueStorageEngine.equals(value, valueEncoding(encoding));
  }

  @SuppressWarnings("unchecked")
  @Override
  public K readKey(long encoding, int hashCode) {
    return (K) keyStorageEngine.read(keyEncoding(encoding));
  }

  @Override
  public boolean equalsKey(Object key, long encoding) {
    return keyStorageEngine.equals(key, keyEncoding(encoding));
  }

  @Override
  public void clear() {
    keyStorageEngine.clear();
    valueStorageEngine.clear();
  }

  @Override
  public long getAllocatedMemory() {
    return keyStorageEngine.getAllocatedMemory() + valueStorageEngine.getAllocatedMemory();
  }

  @Override
  public long getOccupiedMemory() {
    return keyStorageEngine.getOccupiedMemory() + valueStorageEngine.getOccupiedMemory();
  }

  @Override
  public long getVitalMemory() {
    return keyStorageEngine.getVitalMemory() + valueStorageEngine.getVitalMemory();
  }

  @Override
  public long getDataSize() {
    return keyStorageEngine.getDataSize() + valueStorageEngine.getDataSize();
  }

  @Override
  public String toString() {
    return "SplitStorageEngine:\n" +
                "Keys:\n" +
                keyStorageEngine + '\n' +
                "Values:\n" +
                valueStorageEngine;
  }

  @Override
  public void invalidateCache() {
    keyStorageEngine.invalidateCache();
    valueStorageEngine.invalidateCache();
  }

  @Override
  public void bind(Owner owner) {
    keyStorageEngine.bind(owner, ~0L << Integer.SIZE);
    valueStorageEngine.bind(owner, ~0L >>> Integer.SIZE);
  }

  @Override
  public void destroy() {
    keyStorageEngine.destroy();
    valueStorageEngine.destroy();
  }

  @Override
  public boolean shrink() {
    return keyStorageEngine.shrink() || valueStorageEngine.shrink();
  }

  public static int valueEncoding(long encoding) {
    return (int) encoding;
  }

  public static int keyEncoding(long encoding) {
    return (int) (encoding >>> Integer.SIZE);
  }

  public static long encoding(int keyEncoding, int valueEncoding) {
    return (((long) keyEncoding) << Integer.SIZE) | (valueEncoding & 0xffffffffL);
  }
}
