/*
 * Copyright 2016-2023 Terracotta, Inc., a Software AG company.
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
package org.terracotta.offheapstore.util;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.junit.AssumptionViolatedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.disk.paging.MappedPageSource;
import org.terracotta.offheapstore.disk.storage.FileBackedStorageEngine;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.OffHeapBufferStorageEngine;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.portability.Portability;

@RunWith(Parameterized.class)
public abstract class OffHeapAndDiskStorageEngineDependentTest extends StorageEngineDependentTest {

  @ParallelParameterized.Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList(
            new Object[] {OffHeapTestMode.REGULAR_INT},
            new Object[] {OffHeapTestMode.REGULAR_LONG},
            new Object[] {OffHeapTestMode.COMPRESSING_INT},
            new Object[] {OffHeapTestMode.COMPRESSING_LONG},
            new Object[] {DiskTestMode.DISK}
    );
  }

  public OffHeapAndDiskStorageEngineDependentTest(TestMode mode) {
    super(mode);
  }

  public enum OffHeapTestMode implements TestMode {
    REGULAR_INT(PointerSize.INT, 0.0f),
    REGULAR_LONG(PointerSize.LONG, 0.0f),
    COMPRESSING_INT(PointerSize.LONG, 1.0f),
    COMPRESSING_LONG(PointerSize.INT, 1.0f);

    private final PointerSize pointerSize;
    private final float compressionThreshold;

    OffHeapTestMode(PointerSize pointerSize, float compressionThreshold) {
      this.pointerSize = pointerSize;
      this.compressionThreshold = compressionThreshold;
    }


    @Override
    public PageSource createPageSource(long size, MemoryUnit unit) {
      return new UpfrontAllocatingPageSource(new OffHeapBufferSource(), unit.toBytes(size), unit.toBytes((int) size));
    }

    @Override
    public <K,V> OffHeapBufferStorageEngine<K, V> createStorageEngine(PageSource source, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean thief, boolean victim) {
      return new OffHeapBufferStorageEngine<>(pointerSize, source, 1024, keyPortability, valuePortability, thief, victim, compressionThreshold);
    }

    @Override
    public <K, V> Factory<? extends OffHeapBufferStorageEngine<K, V>> createStorageEngineFactory(PageSource source, final Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean thief, boolean victim) {
      return OffHeapBufferStorageEngine.createFactory(pointerSize, source, 1024, keyPortability, valuePortability, thief, victim, compressionThreshold);
    }
  }

  public enum DiskTestMode implements TestMode {
    DISK {
      @Override
      public <K, V> FileBackedStorageEngine<K, V> createStorageEngine(PageSource source, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean thief, boolean victim) {
        if (!thief && !victim) {
          return new FileBackedStorageEngine<>((MappedPageSource) source, 1024, MemoryUnit.BYTES, keyPortability, valuePortability);
        } else {
          throw new AssumptionViolatedException("FileBackedStorageEngine doesn't support stealing");
        }
      }

      @Override
      public <K, V> Factory<FileBackedStorageEngine<K, V>> createStorageEngineFactory(PageSource source, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean thief, boolean victim) {
        if (!thief && !victim) {
          return FileBackedStorageEngine.createFactory((MappedPageSource) source, 1024, MemoryUnit.BYTES, keyPortability, valuePortability);
        } else {
          throw new AssumptionViolatedException("FileBackedStorageEngine doesn't support stealing");
        }
      }

      @Override
      public PageSource createPageSource(long size, MemoryUnit unit) {
        try {
          File file = File.createTempFile("DiskTestMode.DISK", ".data");
          file.deleteOnExit();
          return new MappedPageSource(file, unit.toBytes(size));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
