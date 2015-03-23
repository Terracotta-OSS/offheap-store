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

import java.io.Serializable;

import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.storage.portability.SerializablePortability;
import org.terracotta.offheapstore.util.Factory;

/**
 * A {@code OffHeapBufferStorageEngine} subclass using Java serialization for
 * portability.
 *
 * @author Chris Dennis
 */
public class SerializableStorageEngine extends OffHeapBufferStorageEngine<Serializable, Serializable> {

  public static Factory<SerializableStorageEngine> createFactory(final PointerSize width, final PageSource source, final int pageSize) {
    return new Factory<SerializableStorageEngine>() {
      @Override
      public SerializableStorageEngine newInstance() {
        return new SerializableStorageEngine(width, source, pageSize);
      }
    };
  }

  public static Factory<SerializableStorageEngine> createFactory(final PointerSize width, final PageSource source, final int pageSize, final Portability<Serializable> portability) {
    return new Factory<SerializableStorageEngine>() {
      @Override
      public SerializableStorageEngine newInstance() {
        return new SerializableStorageEngine(width, source, pageSize, portability);
      }
    };
  }

  /**
   * Create an engine using the given allocator, and a unique portability
   * instance.
   *
   * @param allocator data region allocator
   */
  public SerializableStorageEngine(PointerSize width, PageSource source, int pageSize) {
    super(width, source, pageSize, new SerializablePortability(), new SerializablePortability());
  }

  /**
   * Create an engine using the given allocator, and a pre-existing portability
   * instance.
   * <p>
   * This constructor may be useful when constructing multiple caches/maps that
   * need to share a single portability instance.
   *
   * @param allocator data region allocator
   * @param portability existing portability instance
   */
  protected SerializableStorageEngine(PointerSize width, PageSource source, int pageSize, Portability<Serializable> portability) {
    super(width, source, pageSize, portability, portability);
  }  
}
