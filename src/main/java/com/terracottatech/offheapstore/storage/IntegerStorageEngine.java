/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.storage;

import com.terracottatech.offheapstore.storage.StorageEngine.Owner;
import com.terracottatech.offheapstore.util.Factory;

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
    return Integer.valueOf(address);
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
      return ((Integer) key).intValue() == address;
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
