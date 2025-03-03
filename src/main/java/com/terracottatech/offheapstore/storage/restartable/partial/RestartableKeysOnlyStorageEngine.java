/*
 * Copyright 2014-2023 Terracotta, Inc., a Software AG company.
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

package com.terracottatech.offheapstore.storage.restartable.partial;

import com.terracottatech.frs.RestartStore;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.util.Factory;

import java.nio.ByteBuffer;

/**
 *
 * @author cdennis
 */
public class RestartableKeysOnlyStorageEngine<I, K, V> extends RestartableMinimalStorageEngine<I, K, V> {

  public static <I, K, V> Factory<RestartableKeysOnlyStorageEngine<I, K, V>> createKeysOnlyFactory(final I identifier, final RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, final boolean synchronous,
          final PointerSize width, final PageSource source, final int pageSize, final Portability<? super K> keyPortability, final Portability<? super V> valuePortability, final boolean thief, final boolean victim, final float compressThreshold) {
    return new Factory<RestartableKeysOnlyStorageEngine<I, K, V>>() {

      @Override
      public RestartableKeysOnlyStorageEngine<I, K, V> newInstance() {
        return new RestartableKeysOnlyStorageEngine<I, K, V>(identifier, transactionSource, synchronous, width, source, pageSize, keyPortability, valuePortability, thief, victim, compressThreshold);
      }
    };
  }

  public static <I, K, V> Factory<RestartableKeysOnlyStorageEngine<I, K, V>> createKeysOnlyFactory(final I identifier, final RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, final boolean synchronous,
          final PointerSize width, final PageSource source, final int initialPageSize, final int maximalPageSize, final Portability<? super K> keyPortability, final Portability<? super V> valuePortability, final boolean thief, final boolean victim, final float compressThreshold) {
    return new Factory<RestartableKeysOnlyStorageEngine<I, K, V>>() {

      @Override
      public RestartableKeysOnlyStorageEngine<I, K, V> newInstance() {
        return new RestartableKeysOnlyStorageEngine<I, K, V>(identifier, transactionSource, synchronous, width, source, initialPageSize, maximalPageSize, keyPortability, valuePortability, thief, victim, compressThreshold);
      }
    };
  }
  
  public RestartableKeysOnlyStorageEngine(I identifier, RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, boolean synchronous, PointerSize width, PageSource source, int pageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability, float compressThreshold) {
    super(identifier, transactionSource, synchronous, width, source, pageSize, keyPortability, valuePortability, compressThreshold);
  }

  public RestartableKeysOnlyStorageEngine(I identifier, RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, boolean synchronous, PointerSize width, PageSource source, int initialPageSize, int maximalPageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability, float compressThreshold) {
    super(identifier, transactionSource, synchronous, width, source, initialPageSize, maximalPageSize, keyPortability, valuePortability, compressThreshold);
  }

  public RestartableKeysOnlyStorageEngine(I identifier, RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, boolean synchronous, PointerSize width, PageSource source, int pageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean thief, boolean victim, float compressThreshold) {
    super(identifier, transactionSource, synchronous, width, source, pageSize, keyPortability, valuePortability, thief, victim, compressThreshold);
  }

  public RestartableKeysOnlyStorageEngine(I identifier, RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, boolean synchronous, PointerSize width, PageSource source, int initialPageSize, int maximalPageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean thief, boolean victim, float compressThreshold) {
    super(identifier, transactionSource, synchronous, width, source, initialPageSize, maximalPageSize, keyPortability, valuePortability, thief, victim, compressThreshold);
  }

  @Override
  protected Long writePartialEntry(int hash, ByteBuffer binaryKey, ByteBuffer binaryValue) {
    Long result = super.writePartialEntry(hash, binaryKey, binaryValue);
    if (result != null) {
      int keyOffset = getKeyOffset(result);
      metadataArea.writeInt(result + keyOffset, binaryKey.remaining());
      metadataArea.writeBuffer(result + keyOffset + (Integer.SIZE / Byte.SIZE), binaryKey.duplicate());
    }
    return result;
  }

  @Override
  protected int getActualEntrySize(long encoding) {
    int keyOffset = getKeyOffset(encoding);
    int keyLength = metadataArea.readInt(encoding + keyOffset);
    return super.getActualEntrySize(encoding) + (Integer.SIZE / Byte.SIZE) + keyLength;
  }

  @Override
  protected int getRequiredEntrySize(ByteBuffer binaryKey, ByteBuffer binaryValue) {
    return super.getRequiredEntrySize(binaryKey, binaryValue) + (Integer.SIZE / Byte.SIZE) + binaryKey.remaining();
  }

  
  private int getKeyOffset(long result) {
    return super.getActualEntrySize(result);
  }

  @Override
  protected Entry readEntry(long encoding) {
    return new KeyBypassEntry(encoding);
  }

  class KeyBypassEntry implements Entry {

    private final long encoding;
    
    private Entry frsEntry;
    
    KeyBypassEntry(long encoding) {
      this.encoding = encoding;
    }
    
    @Override
    public ByteBuffer getKey() {
      int keyOffset = getKeyOffset(encoding);
      int keyLength = metadataArea.readInt(encoding + keyOffset);
      return metadataArea.readBuffer(encoding + keyOffset + (Integer.SIZE / Byte.SIZE), keyLength);
    }

    @Override
    public ByteBuffer getValue() {
      if (frsEntry == null) {
        frsEntry = RestartableKeysOnlyStorageEngine.super.readEntry(encoding);
      }
      return frsEntry.getValue();
    }

    @Override
    public void close() {
      if (frsEntry != null) {
        frsEntry.close();
      }
    }
  }
}
