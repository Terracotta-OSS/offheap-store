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

import org.terracotta.offheapstore.storage.portability.Portability;

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
        lastObject = new CachedEncode<>(object, buffer.duplicate());
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
