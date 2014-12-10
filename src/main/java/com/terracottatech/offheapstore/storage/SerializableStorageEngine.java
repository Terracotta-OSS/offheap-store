/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.storage;

import java.io.Serializable;

import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.storage.portability.Portability;
import com.terracottatech.offheapstore.storage.portability.SerializablePortability;
import com.terracottatech.offheapstore.util.Factory;

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
