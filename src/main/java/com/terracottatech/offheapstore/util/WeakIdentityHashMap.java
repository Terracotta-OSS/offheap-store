/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.util;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

//TODO This class needs some direct unit test coverage
/**
 *
 * @author Chris Dennis
 */
public class WeakIdentityHashMap<K, V> {

  private final Map<WeakReference<K>, V> map = new HashMap<WeakReference<K>, V>();
  private final ReferenceQueue<K> queue = new ReferenceQueue<K>();
  private final ReaperTask<V> reaperTask;
  
  public WeakIdentityHashMap() {
    this(null);
  }
  
  public WeakIdentityHashMap(ReaperTask<V> reaperTask) {
    this.reaperTask = reaperTask;
  }

  public V put(K key, V value) {
    reap();
    WeakReference<K> keyRef = new IdentityWeakReference<K>(key, queue);
    return map.put(keyRef, value);
  }

  public V get(K key) {
    reap();
    WeakReference<K> keyRef = new IdentityWeakReference<K>(key);
    return map.get(keyRef);
  }
  
  public V remove(K key) {
    reap();
    return map.remove(new IdentityWeakReference<K>(key));
  }

  public Iterator<Entry<K, V>> entries() {
    return new Iterator<Entry<K, V>>() {

      private final Iterator<Entry<WeakReference<K>, V>> delegate = map.entrySet().iterator();

      @Override
      public boolean hasNext() {
        return delegate.hasNext();
      }

      @Override
      public Entry<K, V> next() {
        Entry<WeakReference<K>, V> e = delegate.next();
        K key = e.getKey().get();
        V value = e.getValue();
        if (key == null) {
          return null;
        } else {
          return new SimpleEntry<K, V>(key, value);
        }
      }

      @Override
      public void remove() {
        delegate.remove();
      }
    };
  }

  public Iterator<V> values() {
    return map.values().iterator();
  }
  
  public void reap() {
    Reference<? extends K> ref;
    while ((ref = queue.poll()) != null) {
      V removed = map.remove(ref);
      if (reaperTask != null && removed != null) {
        reaperTask.reap(removed);
      }
    }
  }

  static class IdentityWeakReference<T> extends WeakReference<T> {

    final int hashCode;

    IdentityWeakReference(T o) {
      this(o, null);
    }

    IdentityWeakReference(T o, ReferenceQueue<T> q) {
      super(o, q);
      this.hashCode = (o == null) ? 0 : System.identityHashCode(o);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof IdentityWeakReference<?>)) {
        return false;
      } else {
        IdentityWeakReference<?> wr = (IdentityWeakReference<?>) o;
        Object got = get();
        return (got != null && got == wr.get());
      }
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }

  static class SimpleEntry<K, V> implements Entry<K, V> {

    private final K key;
    private final V value;

    SimpleEntry(K key, V value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public V setValue(V v) {
      throw new UnsupportedOperationException();
    }
    
  }
  
  public interface ReaperTask<T> {
    void reap(T object);
  }
}
