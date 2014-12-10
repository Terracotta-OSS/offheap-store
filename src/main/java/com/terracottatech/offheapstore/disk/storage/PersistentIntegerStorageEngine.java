/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.disk.storage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.terracottatech.offheapstore.disk.persistent.PersistentHalfStorageEngine;
import com.terracottatech.offheapstore.storage.IntegerStorageEngine;
import com.terracottatech.offheapstore.util.Factory;

/**
 *
 * @author Chris Dennis
 */
public class PersistentIntegerStorageEngine extends IntegerStorageEngine implements PersistentHalfStorageEngine<Integer> {

  private static final PersistentIntegerStorageEngine                   SINGLETON = new PersistentIntegerStorageEngine();
  private static final Factory<PersistentIntegerStorageEngine> FACTORY   = new Factory<PersistentIntegerStorageEngine>() {
    @Override
    public PersistentIntegerStorageEngine newInstance() {
      return SINGLETON;
    }
  };

  public static Factory<PersistentIntegerStorageEngine> createPersistentFactory() {
    return FACTORY;
  }
  
  @Override
  public void flush() throws IOException {
    //no-op
  }

  @Override
  public void close() throws IOException {
    //no-op
  }

  @Override
  public void persist(ObjectOutput output) throws IOException {
    //no-op
  }

  @Override
  public void bootstrap(ObjectInput input) throws IOException {
    //no-op
  }
}
