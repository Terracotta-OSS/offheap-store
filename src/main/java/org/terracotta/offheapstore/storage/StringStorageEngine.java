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

import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.storage.portability.StringPortability;
import org.terracotta.offheapstore.util.Factory;

/**
 * A {@code <String, String>} storage engine.
 *
 * @author Chris Dennis
 */
public class StringStorageEngine extends OffHeapBufferStorageEngine<String, String> {

  public static Factory<StringStorageEngine> createFactory(final PointerSize width, final PageSource source, final int pageSize) {
    return () -> new StringStorageEngine(width, source, pageSize);
  }

  private static final Portability<String> PORTABILITY = StringPortability.INSTANCE;

  public StringStorageEngine(PointerSize width, PageSource source, int pageSize) {
    super(width, source, pageSize, PORTABILITY, PORTABILITY);
  }

}
