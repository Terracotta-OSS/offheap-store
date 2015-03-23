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
package org.terracotta.offheapstore.storage;

import org.terracotta.offheapstore.storage.StorageEngine.Owner;
import org.terracotta.offheapstore.util.Factory;

/**
 * A symmetric {@code Integer} storage engine.
 * <p>
 * This engine stores the keys and values directly as their primitive
 * representations, and therefore uses no additional data structure.
 *
 * @author Chris Dennis
 */
public class IntegerStorageEngine implements HalfStorageEngine<Integer> {

  private static final IntegerStorageEngine              SINGLETON = new IntegerStorageEngine();
  private static final Factory<IntegerStorageEngine> FACTORY   = new Factory<IntegerStorageEngine>() {
    @Override
    public IntegerStorageEngine newInstance() {
      return SINGLETON;
    }
  };

  public static IntegerStorageEngine instance() {
    return SINGLETON;
  }
  
  public static Factory<IntegerStorageEngine> createFactory() {
    return FACTORY;
  }
  
  @Override
  public Integer read(int address) {
    return address;
  }

  @Override
  public Integer write(Integer value, int hash) {
    return value;
  }

  @Override
  public void free(int address) {
    //no-op
  }

  @Override
  public boolean equals(Object key, int address) {
    if (key instanceof Integer) {
      return ((Integer) key) == address;
    } else {
      return false;
    }
  }

  @Override
  public void clear() {
    //no-op
  }

  @Override
  public long getAllocatedMemory() {
    return 0;
  }

  @Override
  public long getOccupiedMemory() {
    return 0;
  }

  @Override
  public long getVitalMemory() {
    return 0;
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
