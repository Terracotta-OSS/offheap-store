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
package org.terracotta.offheapstore.storage.allocator;

public interface Allocator extends Iterable<Long> {

  long allocate(long size);
  
  void free(long address);
  
  void clear();

  void expand(long size);

  long occupied();

  void validateAllocator();

  long getLastUsedAddress();
  
  long getLastUsedPointer();
  
  int getMinimalSize();

  long getMaximumAddress();

}
