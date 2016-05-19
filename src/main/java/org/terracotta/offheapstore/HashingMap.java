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
package org.terracotta.offheapstore;

import java.util.Map;

/**
 * Interface of all map implementations based on hash-coding.
 */
public interface HashingMap<K, V> extends Map<K, V> {

  /**
   * Remove all keys having a specific hashcode.
   * @param keyHash the hashcode of the keys to be removed.
   * @return a {@link Map} containing the removed mappings.
   */
  Map<K, V> removeAllWithHash(int keyHash);

}
