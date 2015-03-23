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

import org.terracotta.offheapstore.CapacityLimitedIntegerStorageEngineFactory.CapacityLimitedIntegerStorageEngine;
import org.terracotta.offheapstore.storage.IntegerStorageEngine;
import org.terracotta.offheapstore.storage.SplitStorageEngine;
import org.terracotta.offheapstore.util.Factory;

public class CapacityLimitedIntegerStorageEngineFactory implements Factory<CapacityLimitedIntegerStorageEngine> {

  private int used;
  private int capacity;
  
  public void setCapacity(int capacity) {
    this.capacity = capacity;
  }
  
  @Override
  public CapacityLimitedIntegerStorageEngine newInstance() {
    return new CapacityLimitedIntegerStorageEngine();
  }
  
  public class CapacityLimitedIntegerStorageEngine extends SplitStorageEngine<Integer, Integer> {

    public CapacityLimitedIntegerStorageEngine() {
      super(new IntegerStorageEngine(), new IntegerStorageEngine());
    }

    @Override
    public Long writeMapping(Integer key, Integer value, int hash, int metadata) {
      if (used < capacity) {
        used++;
        return super.writeMapping(key, value, hash, metadata);
      } else {
        return null;
      }
    }
    
    @Override
    public void freeMapping(long encoding, int hash, boolean removal) {
      used--;
      super.freeMapping(encoding, hash, removal);
    }
  }
}
