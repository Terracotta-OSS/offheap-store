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
package org.terracotta.offheapstore.storage.listener;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArraySet;

public abstract class AbstractListenableStorageEngine<K, V> implements ListenableStorageEngine<K, V> {

  private final Set<RuntimeStorageEngineListener<? super K, ? super V>> runtimeListeners = new CopyOnWriteArraySet<RuntimeStorageEngineListener<? super K, ? super V>>();
  private final Set<RecoveryStorageEngineListener<? super K, ? super V>> recoveryListeners = new CopyOnWriteArraySet<RecoveryStorageEngineListener<? super K, ? super V>>();

  @Override
  public void registerListener(StorageEngineListener<? super K, ? super V> listener) {
    if (listener instanceof RecoveryStorageEngineListener) {
      recoveryListeners.add((RecoveryStorageEngineListener) listener);
    }
    if (listener instanceof RuntimeStorageEngineListener) {
      runtimeListeners.add((RuntimeStorageEngineListener) listener);
    }
  }
  
  protected final boolean hasListeners() {
    return !runtimeListeners.isEmpty();
  }

  protected final boolean hasRecoveryListeners() {
    return !recoveryListeners.isEmpty();
  }
  
  protected final void fireRecovered(Callable<K> key, Callable<V> value, ByteBuffer binaryKey, ByteBuffer binaryValue, int hash, int metadata, long encoding) {
    for (RecoveryStorageEngineListener<? super K, ? super V> listener : recoveryListeners) {
      listener.recovered(key, value, binaryKey.duplicate(), binaryValue.duplicate(), hash, metadata, encoding);
    }
  }
  
  protected final void fireWritten(K key, V value, ByteBuffer binaryKey, ByteBuffer binaryValue, int hash, int metadata, long encoding) {
    for (RuntimeStorageEngineListener<? super K, ? super V> listener : runtimeListeners) {
      listener.written(key, value, binaryKey.duplicate(), binaryValue.duplicate(), hash, metadata, encoding);
    }
  }
  
  protected final void fireFreed(long encoding, int hash, ByteBuffer binaryKey, boolean removed) {
    for (RuntimeStorageEngineListener<? super K, ? super V> listener : runtimeListeners) {
      listener.freed(encoding, hash, binaryKey.duplicate(), removed);
    }
  }

  protected final void fireCleared() {
    for (RuntimeStorageEngineListener<? super K, ? super V> listener : runtimeListeners) {
      listener.cleared();
    }
  }
  
  protected final void fireCopied(int hash, long oldEncoding, long newEncoding, int metadata) {
    for (RuntimeStorageEngineListener<? super K, ? super V> listener : runtimeListeners) {
      listener.copied(hash, oldEncoding, newEncoding, metadata);
    }
  }
  
}
