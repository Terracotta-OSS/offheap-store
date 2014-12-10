/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.disk.storage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.terracottatech.offheapstore.disk.persistent.Persistent;
import com.terracottatech.offheapstore.disk.persistent.PersistentHalfStorageEngine;
import com.terracottatech.offheapstore.disk.persistent.PersistentStorageEngine;
import com.terracottatech.offheapstore.storage.SplitStorageEngine;
import com.terracottatech.offheapstore.util.Factory;

/**
 *
 * @author Chris Dennis
 */
public class PersistentSplitStorageEngine<K, V> extends SplitStorageEngine<K, V> implements PersistentStorageEngine<K, V> {

  public static <K, V> Factory<PersistentSplitStorageEngine<K, V>> createPersistentFactory(final Factory<? extends PersistentHalfStorageEngine<K>> keyFactory, final Factory<? extends PersistentHalfStorageEngine<V>> valueFactory) {
    return new Factory<PersistentSplitStorageEngine<K, V>>() {

      @Override
      public PersistentSplitStorageEngine<K, V> newInstance() {
        return new PersistentSplitStorageEngine<K, V>(keyFactory.newInstance(), valueFactory.newInstance());
      }
    };
  }

  public PersistentSplitStorageEngine(PersistentHalfStorageEngine<K> keyStorageEngine, PersistentHalfStorageEngine<V> valueStorageEngine) {
    super(keyStorageEngine, valueStorageEngine);
  }

  @Override
  public void flush() throws IOException {
    ((Persistent) keyStorageEngine).flush();
    ((Persistent) valueStorageEngine).flush();
  }

  @Override
  public void close() throws IOException {
    ((Persistent) keyStorageEngine).close();
    ((Persistent) valueStorageEngine).close();
  }

  @Override
  public void persist(ObjectOutput output) throws IOException {
    ((Persistent) keyStorageEngine).persist(output);
    ((Persistent) valueStorageEngine).persist(output);
  }

  @Override
  public void bootstrap(ObjectInput input) throws IOException {
    ((Persistent) keyStorageEngine).bootstrap(input);
    ((Persistent) valueStorageEngine).bootstrap(input);
  }

}
