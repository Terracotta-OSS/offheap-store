/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.storage;

import java.nio.ByteBuffer;

import com.terracottatech.offheapstore.storage.listener.AbstractListenableStorageEngine;
import com.terracottatech.offheapstore.storage.portability.Portability;
import com.terracottatech.offheapstore.storage.portability.WriteBackPortability;
import com.terracottatech.offheapstore.storage.portability.WriteContext;

import static com.terracottatech.offheapstore.util.ByteBufferUtils.aggregate;

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
   * Creates a storage engine using the given allocator, and portabilities.
   *
   * @param allocator allocator used for storage allocation
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
      lastMapping = new CachedEncode<K, V>(key, value, keyBuffer, valueBuffer, result);
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
      if (cachedEncoding != null && cachedEncoding.longValue() == encoding) {
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
      if (cachedEncoding != null && cachedEncoding.longValue() == encoding) {
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
