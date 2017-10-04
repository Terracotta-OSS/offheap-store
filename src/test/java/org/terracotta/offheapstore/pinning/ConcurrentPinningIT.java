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
package org.terracotta.offheapstore.pinning;

import static org.terracotta.offheapstore.util.MemoryUnit.KILOBYTES;

import org.junit.AssumptionViolatedException;
import org.terracotta.offheapstore.concurrent.ConcurrentOffHeapClockCache;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.IntegerStorageEngine;
import org.terracotta.offheapstore.storage.OffHeapBufferStorageEngine;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.SplitStorageEngine;
import org.terracotta.offheapstore.storage.StorageEngine;
import org.terracotta.offheapstore.storage.portability.ByteArrayPortability;
import org.terracotta.offheapstore.storage.portability.SerializablePortability;
import org.terracotta.offheapstore.util.Factory;

import org.hamcrest.core.Is;
import org.junit.Assume;

public class ConcurrentPinningIT extends AbstractPinningIT {

  @Override
  protected PinnableCache<Integer, Integer> createPinnedIntegerCache(PageSource source) {
    Assume.assumeThat(getPointerSize(), Is.is(PointerSize.INT));
    Factory<? extends StorageEngine<Integer, Integer>> storageEngineFactory = SplitStorageEngine.createFactory(IntegerStorageEngine.createFactory(), IntegerStorageEngine.createFactory());
    return new ConcurrentOffHeapClockCache<>(source, storageEngineFactory);
  }

  @Override
  protected PinnableCache<Integer, byte[]> createPinnedByteArrayCache(PageSource source) {
    Factory<OffHeapBufferStorageEngine<Integer, byte[]>> storageEngineFactory = OffHeapBufferStorageEngine.createFactory(getPointerSize(), source, KILOBYTES.toBytes(1), new SerializablePortability(), ByteArrayPortability.INSTANCE, false, false);
    return new ConcurrentOffHeapClockCache<>(source, storageEngineFactory);
  }

  @Override
  protected PinnableCache<Integer, byte[]> createSharingPinnedByteArrayCache(PageSource source) {
    throw new AssumptionViolatedException("Cannot create sharing concurrent cache");
  }
}
