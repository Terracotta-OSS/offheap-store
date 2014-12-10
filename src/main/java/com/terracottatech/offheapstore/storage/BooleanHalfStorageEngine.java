/*
 * All content copyright (c) 2011 Terracotta, Inc., except as may otherwise be
 * noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore.storage;

import com.terracottatech.offheapstore.storage.StorageEngine.Owner;

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
    return object.booleanValue() ? 1 : 0;
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
    if (object instanceof Boolean) {
      return write((Boolean) object, 0) == encoding;
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
