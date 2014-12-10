/*
 * All content copyright (c) 2011 Terracotta, Inc., except as may otherwise be
 * noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore.set;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;

import com.terracottatech.offheapstore.OffHeapHashMap;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.storage.StorageEngine;

/**
 *
 * @author cdennis
 */
public class OffHeapHashSet<E> extends AbstractSet<E> {

  private final OffHeapHashMap<E, Boolean> map;
  
  public OffHeapHashSet(PageSource source, boolean tableSteals, StorageEngine<? super E, Boolean> engine, int capacity) {
    this(new OffHeapHashMap<E, Boolean>(source, tableSteals, engine, capacity));
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
