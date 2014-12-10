/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.storage;

import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.storage.portability.Portability;
import com.terracottatech.offheapstore.storage.portability.StringPortability;
import com.terracottatech.offheapstore.util.Factory;

/**
 * A {@code <String, String>} storage engine.
 *
 * @author Chris Dennis
 */
public class StringStorageEngine extends OffHeapBufferStorageEngine<String, String> {

  public static Factory<StringStorageEngine> createFactory(final PointerSize width, final PageSource source, final int pageSize) {
    return new Factory<StringStorageEngine>() {
      @Override
      public StringStorageEngine newInstance() {
        return new StringStorageEngine(width, source, pageSize);
      }
    };
  }

  private static final Portability<String> PORTABILITY = StringPortability.INSTANCE;

  /**
   * Create an engine using the given allocator.
   *
   * @param allocator data region allocator
   */
  public StringStorageEngine(PointerSize width, PageSource source, int pageSize) {
    super(width, source, pageSize, PORTABILITY, PORTABILITY);
  }

}
