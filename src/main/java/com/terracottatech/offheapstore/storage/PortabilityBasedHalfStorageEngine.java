/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.storage;

import java.nio.ByteBuffer;

import com.terracottatech.offheapstore.storage.portability.Portability;

/**
 *
 * @author cdennis
 */
public abstract class PortabilityBasedHalfStorageEngine<T> implements HalfStorageEngine<T> {

  private final Portability<? super T> portability;
  
  private CachedEncode<T> lastObject;

  public PortabilityBasedHalfStorageEngine(Portability<? super T> portability) {
    this.portability = portability;
  }

  @Override
  public Integer write(T object, int hash) {
    if (lastObject != null && lastObject.get() == object) {
      return writeBuffer(lastObject.getEncoded(), hash);
    } else {
      ByteBuffer buffer = portability.encode(object);
      Integer result = writeBuffer(buffer, hash);
      if (result == null) {
        lastObject = new CachedEncode<T>(object, buffer.duplicate());
      }
      return result;
    }
  }

  @Override
  public abstract void free(int encoding);

  @SuppressWarnings("unchecked")
  @Override
  public T read(int encoding) {
    return (T) portability.decode(readBuffer(encoding));
  }

  @Override
  public boolean equals(Object value, int encoding) {
    return portability.equals(value, readBuffer(encoding));
  }

  protected abstract ByteBuffer readBuffer(int encoding);

  protected abstract Integer writeBuffer(ByteBuffer buffer, int hash);

  @Override
  public void invalidateCache() {
    lastObject = null;
  }

  static class CachedEncode<T> {
    private final T object;

    private final ByteBuffer buffer;

    public CachedEncode(T object, ByteBuffer buffer) {
      this.object = object;
      this.buffer = buffer;
    }

    final T get() {
      return object;
    }

    final ByteBuffer getEncoded() {
      return buffer.duplicate();
    }
  }
}
