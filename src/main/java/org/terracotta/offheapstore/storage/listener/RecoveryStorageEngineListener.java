/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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
import java.util.concurrent.Callable;

/**
 *
 * @author cdennis
 */
public interface RecoveryStorageEngineListener<K, V> extends StorageEngineListener<K, V> {
  
  void recovered(Callable<? extends K> key, Callable<? extends V> value, ByteBuffer binaryKey, ByteBuffer binaryValue, int hash, int metadata, long encoding);
}
