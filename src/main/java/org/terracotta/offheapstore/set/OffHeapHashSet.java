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
package org.terracotta.offheapstore.set;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;

import org.terracotta.offheapstore.OffHeapHashMap;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.StorageEngine;

/**
 *
 * @author cdennis
 */
public class OffHeapHashSet<E> extends AbstractSet<E> {

  private final OffHeapHashMap<E, Boolean> map;

  public OffHeapHashSet(PageSource source, boolean tableSteals, StorageEngine<? super E, Boolean> engine, int capacity) {
    this(new OffHeapHashMap<>(source, tableSteals, engine, capacity));
  }

  public OffHeapHashSet(OffHeapHashMap<E, Boolean> map) {
    this.map = map;
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return map.containsKey(o);
  }

  @Override
  public Iterator<E> iterator() {
    return map.keySet().iterator();
  }

  @Override
  public Object[] toArray() {
    return map.keySet().toArray();
  }

  @Override
  public <T> T[] toArray(T[] ts) {
    return map.keySet().toArray(ts);
  }

  @Override
  public boolean add(E e) {
    return map.put(e, Boolean.TRUE) == null;
  }

  @Override
  public boolean remove(Object o) {
    return map.remove(o) != null;
  }

  @Override
  public boolean containsAll(Collection<?> clctn) {
    return map.keySet().containsAll(clctn);
  }

  @Override
  public void clear() {
    map.clear();
  }

  public StorageEngine<? super E, ?> getStorageEngine() {
    return map.getStorageEngine();
  }
}
