package com.terracottatech.offheapstore;

import com.terracottatech.offheapstore.CapacityLimitedIntegerStorageEngineFactory.CapacityLimitedIntegerStorageEngine;
import com.terracottatech.offheapstore.storage.IntegerStorageEngine;
import com.terracottatech.offheapstore.storage.SplitStorageEngine;
import com.terracottatech.offheapstore.util.Factory;

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
