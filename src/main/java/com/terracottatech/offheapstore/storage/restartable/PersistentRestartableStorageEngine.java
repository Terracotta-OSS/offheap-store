/*
 * Copyright 2014-2023 Terracotta, Inc., a Software AG company.
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
package com.terracottatech.offheapstore.storage.restartable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

import com.terracottatech.frs.RestartStore;
import org.terracotta.offheapstore.disk.persistent.PersistentStorageEngine;
import org.terracotta.offheapstore.storage.BinaryStorageEngine;
import org.terracotta.offheapstore.util.Factory;

public class PersistentRestartableStorageEngine<T extends PersistentStorageEngine<K, LinkedNode<V>> & BinaryStorageEngine, I, K, V> extends RestartableStorageEngine<T, I, K, V> implements PersistentStorageEngine<K, V> {

  public static <T extends PersistentStorageEngine<K, LinkedNode<V>> & BinaryStorageEngine, I, K, V> Factory<PersistentRestartableStorageEngine<T, I, K, V>> createPersistentFactory(final I identifier, final RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, final Factory<T> delegateFactory, final boolean synchronous) {
    return new Factory<PersistentRestartableStorageEngine<T,I,K,V>>() {

      @Override
      public PersistentRestartableStorageEngine<T, I, K, V> newInstance() {
        return new PersistentRestartableStorageEngine<T, I, K, V>(identifier, transactionSource, delegateFactory.newInstance(), synchronous);
      }
    };
  }

  public PersistentRestartableStorageEngine(I identifier, RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, T storageEngine, boolean synchronous) {
    super(identifier, transactionSource, storageEngine, synchronous);
  }

  @Override
  public void flush() throws IOException {
    delegateStorageEngine.flush();
  }

  @Override
  public void close() throws IOException {
    delegateStorageEngine.close();
  }

  @Override
  public void persist(ObjectOutput output) throws IOException {
    delegateStorageEngine.persist(output);
  }

  @Override
  public void bootstrap(ObjectInput input) throws IOException {
    delegateStorageEngine.bootstrap(input);
  }
}
