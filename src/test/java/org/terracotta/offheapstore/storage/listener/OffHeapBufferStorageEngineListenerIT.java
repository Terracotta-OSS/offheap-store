/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
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
package org.terracotta.offheapstore.storage.listener;

import org.terracotta.offheapstore.storage.listener.ListenableStorageEngine;
import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.storage.OffHeapBufferStorageEngine;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.portability.StringPortability;

public class OffHeapBufferStorageEngineListenerIT extends AbstractListenerIT {

  @Override
  protected ListenableStorageEngine<String, String> createStorageEngine() {
    return new OffHeapBufferStorageEngine<>(PointerSize.INT, new UnlimitedPageSource(new HeapBufferSource()), 1024, StringPortability.INSTANCE, StringPortability.INSTANCE);
  }

}
