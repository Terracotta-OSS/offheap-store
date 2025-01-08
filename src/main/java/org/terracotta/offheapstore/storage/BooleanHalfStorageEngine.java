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

import org.terracotta.offheapstore.storage.StorageEngine.Owner;

/**
 *
 * @author cdennis
 */
public final class BooleanHalfStorageEngine implements HalfStorageEngine<Boolean> {

  public static final BooleanHalfStorageEngine INSTANCE = new BooleanHalfStorageEngine();

  private BooleanHalfStorageEngine() {
    //singleton
  }

  @Override
  public Integer write(Boolean object, int hash) {
    return object ? 1 : 0;
  }

  @Override
  public void free(int encoding) {
    //no-op
  }

  @Override
  public Boolean read(int encoding) {
    return encoding == 1;
  }

  @Override
  public boolean equals(Object object, int encoding) {
    return object instanceof Boolean && write((Boolean) object, 0) == encoding;
  }

  @Override
  public void clear() {
    //no-op
  }

  @Override
  public long getAllocatedMemory() {
    return 0L;
  }

  @Override
  public long getOccupiedMemory() {
    return 0L;
  }

  @Override
  public long getVitalMemory() {
    return 0L;
  }

  @Override
  public long getDataSize() {
    return 0;
  }

  @Override
  public void invalidateCache() {
    //no-op
  }

  @Override
  public void bind(Owner owner, long mask) {
    //no-op
  }

  @Override
  public void destroy() {
    //no-op
  }

  @Override
  public boolean shrink() {
    return false;
  }

}
