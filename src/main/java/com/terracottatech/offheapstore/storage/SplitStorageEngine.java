/*
 * All content copyright (c) 2010-2011 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore.storage;

import com.terracottatech.offheapstore.util.Factory;

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
    return new Factory<SplitStorageEngine<K, V>>() {

      @Override
      public SplitStorageEngine<K, V> newInstance() {
        return new SplitStorageEngine<K, V>(keyFactory.newInstance(), valueFactory.newInstance());
      }
    };
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
    StringBuilder sb = new StringBuilder();
    sb.append("SplitStorageEngine:\n");
    sb.append("Keys:\n");
    sb.append(keyStorageEngine).append('\n');
    sb.append("Values:\n");
    sb.append(valueStorageEngine);
    return sb.toString();
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
