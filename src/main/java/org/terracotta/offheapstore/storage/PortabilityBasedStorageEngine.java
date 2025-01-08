/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
 * Copyright IBM Corp. 2024, 2025
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

import org.terracotta.offheapstore.storage.listener.AbstractListenableStorageEngine;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.storage.portability.WriteBackPortability;
import org.terracotta.offheapstore.storage.portability.WriteContext;

import static org.terracotta.offheapstore.util.ByteBufferUtils.aggregate;

/**
 *
 * @author Chris Dennis
 */
public abstract class PortabilityBasedStorageEngine<K, V> extends AbstractListenableStorageEngine<K, V> implements StorageEngine<K, V>, BinaryStorageEngine {

  /*
   * Future design ideas:
   *
   * We might want to look in to supporting shrinking of the data areas, in case
   * our storage demands change (e.g. due to a change in key distribution, or
   * change in value types).
   */

  protected final Portability<? super K> keyPortability;
  protected final Portability<? super V> valuePortability;

  private CachedEncode<K, V> lastMapping;

  /**
   * Creates a storage engine using the given portabilities.
   *
   * @param keyPortability key type portability
   * @param valuePortability value type portability
   */
  public PortabilityBasedStorageEngine(Portability<? super K> keyPortability, Portability<? super V> valuePortability) {
    this.keyPortability = keyPortability;
    this.valuePortability = valuePortability;
  }

  @Override
  public final Long writeMapping(K key, V value, int hash, int metadata) {
    Long result;
    if (lastMapping != null && lastMapping.getKey() == key && lastMapping.getValue() == value) {
      result = writeMappingBuffers(lastMapping.getEncodedKey(), lastMapping.getEncodedValue(), hash);
    } else {
      ByteBuffer keyBuffer = keyPortability.encode(key);
      ByteBuffer valueBuffer = valuePortability.encode(value);
      result = writeMappingBuffers(keyBuffer.duplicate(), valueBuffer.duplicate(), hash);
      lastMapping = new CachedEncode<>(key, value, keyBuffer, valueBuffer, result);
    }
    if (result != null) {
      fireWritten(key, value, lastMapping.getEncodedKey(), lastMapping.getEncodedValue(), hash, metadata, result);
    }
    return result;
  }

  @Override
  public void attachedMapping(long encoding, int hash, int metadata) {
    //no-op
  }

  @Override
  public final void freeMapping(long encoding, int hash, boolean removal) {
    if (hasListeners()) {
      ByteBuffer binaryKey = readBinaryKey(encoding);
      free(encoding);
      fireFreed(encoding, hash, binaryKey, removal);
    }
    else {
      free(encoding);
    }
  }


  @Override
  public final void clear() {
    clearInternal();
    fireCleared();
  }

  @SuppressWarnings("unchecked")
  @Override
  public V readValue(long encoding) {
    if (valuePortability instanceof WriteBackPortability<?>) {
      return (V) ((WriteBackPortability<? super V>) valuePortability).decode(readValueBuffer(encoding), getValueWriteContext(encoding));
    } else {
      return (V) valuePortability.decode(readValueBuffer(encoding));
    }
  }

  @Override
  public boolean equalsValue(Object value, long encoding) {
    return valuePortability.equals(value, readValueBuffer(encoding));
  }

  @SuppressWarnings("unchecked")
  @Override
  public K readKey(long encoding, int hashCode) {
    if (keyPortability instanceof WriteBackPortability<?>) {
      return (K) ((WriteBackPortability<? super K>) keyPortability).decode(readKeyBuffer(encoding), getKeyWriteContext(encoding));
    } else {
      return (K) keyPortability.decode(readKeyBuffer(encoding));
    }
  }

  @Override
  public boolean equalsKey(Object key, long encoding) {
    return keyPortability.equals(key, readKeyBuffer(encoding));
  }

  @Override
  public ByteBuffer readBinaryKey(long encoding) {
    CachedEncode<K, V> cache = lastMapping;
    if (cache != null) {
      Long cachedEncoding = cache.getEncoding();
      if (cachedEncoding != null && cachedEncoding == encoding) {
        return cache.getEncodedKey();
      }
    }
    ByteBuffer attached = readKeyBuffer(encoding);
    ByteBuffer detached = ByteBuffer.allocate(attached.remaining());
    detached.put(attached).flip();
    return detached;
  }

  @Override
  public ByteBuffer readBinaryValue(long encoding) {
    CachedEncode<K, V> cache = lastMapping;
    if (cache != null) {
      Long cachedEncoding = cache.getEncoding();
      if (cachedEncoding != null && cachedEncoding == encoding) {
        return cache.getEncodedValue();
      }
    }
    ByteBuffer attached = readValueBuffer(encoding);
    ByteBuffer detached = ByteBuffer.allocate(attached.remaining());
    detached.put(attached).flip();
    return detached;
  }

  @Override
  public boolean equalsBinaryKey(ByteBuffer binaryKey, long encoding) {
    return binaryKey.equals(readBinaryKey(encoding)) || equalsKey(keyPortability.decode(binaryKey.duplicate()), encoding);
  }

  @Override
  public Long writeBinaryMapping(ByteBuffer[] binaryKey, ByteBuffer[] binaryValue, int pojoHash, int metadata) {
    return writeMappingBuffersGathering(binaryKey, binaryValue, pojoHash);
  }

  @Override
  public Long writeBinaryMapping(ByteBuffer binaryKey, ByteBuffer binaryValue, int pojoHash, int metadata) {
    return writeMappingBuffers(binaryKey, binaryValue, pojoHash);
  }

  protected Long writeMappingBuffersGathering(ByteBuffer[] keyBuffers, ByteBuffer[] valueBuffers, int hash) {
    return writeMappingBuffers(aggregate(keyBuffers), aggregate(valueBuffers), hash);
  }

  protected abstract void free(long address);

  protected abstract void clearInternal();

  protected abstract ByteBuffer readKeyBuffer(long address);

  protected abstract WriteContext getKeyWriteContext(long address);

  protected abstract ByteBuffer readValueBuffer(long address);

  protected abstract WriteContext getValueWriteContext(long address);

  protected abstract Long writeMappingBuffers(ByteBuffer keyBuffer, ByteBuffer valueBuffer, int hash);

  @Override
  public void invalidateCache() {
    lastMapping = null;
  }

  static class CachedEncode<K, V> {
    private final K key;
    private final V value;

    private final ByteBuffer keyBuffer;
    private final ByteBuffer valueBuffer;

    private final Long encoding;

    public CachedEncode(K key, V value, ByteBuffer keyBuffer, ByteBuffer valueBuffer, Long encoding) {
      this.key = key;
      this.value = value;

      this.keyBuffer = keyBuffer;
      this.valueBuffer = valueBuffer;

      this.encoding = encoding;
    }

    final K getKey() {
      return key;
    }

    final V getValue() {
      return value;
    }

    final ByteBuffer getEncodedKey() {
      return keyBuffer.duplicate();
    }

    final ByteBuffer getEncodedValue() {
      return valueBuffer.duplicate();
    }

    final Long getEncoding() {
      return encoding;
    }
  }
}
