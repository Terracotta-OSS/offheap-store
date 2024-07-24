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
package org.terracotta.offheapstore.disk.storage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.terracotta.offheapstore.disk.persistent.Persistent;
import org.terracotta.offheapstore.disk.persistent.PersistentHalfStorageEngine;
import org.terracotta.offheapstore.disk.persistent.PersistentStorageEngine;
import org.terracotta.offheapstore.storage.SplitStorageEngine;
import org.terracotta.offheapstore.util.Factory;

/**
 *
 * @author Chris Dennis
 */
public class PersistentSplitStorageEngine<K, V> extends SplitStorageEngine<K, V> implements PersistentStorageEngine<K, V> {

  public static <K, V> Factory<PersistentSplitStorageEngine<K, V>> createPersistentFactory(final Factory<? extends PersistentHalfStorageEngine<K>> keyFactory, final Factory<? extends PersistentHalfStorageEngine<V>> valueFactory) {
    return () -> new PersistentSplitStorageEngine<>(keyFactory.newInstance(), valueFactory.newInstance());
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
