package com.terracottatech.offheapstore.storage.listener;

import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;

import org.hamcrest.core.Is;
import org.hamcrest.core.IsEqual;
import org.hamcrest.core.IsNull;
import org.junit.Assert;
import org.junit.Test;

import com.terracottatech.offheapstore.Metadata;
import com.terracottatech.offheapstore.OffHeapHashMap;
import com.terracottatech.offheapstore.buffersource.HeapBufferSource;
import com.terracottatech.offheapstore.paging.UnlimitedPageSource;
import com.terracottatech.offheapstore.storage.StorageEngine;

@SuppressWarnings("unchecked")
public abstract class AbstractListenerIT {

  @Test
  public void testListeningForPuts() {
    ListenableStorageEngine<String, String> storageEngine = createStorageEngine();

    TrackingListener<String, String> listener = new TrackingListener<String, String>();
    storageEngine.registerListener(listener);
    Map<String, String> map = new OffHeapHashMap<String, String>(new UnlimitedPageSource(new HeapBufferSource()), (StorageEngine<? super String, ? super String>) storageEngine);

    for (int i = 0; i < 100; i++) {
      map.put(Integer.toString(i), Integer.toHexString(i));
      Assert.assertThat(listener.getTrackingMap(), IsEqual.equalTo(map));
    }
  }

  @Test
  public void testListeningForRemoves() {
    ListenableStorageEngine<String, String> storageEngine = createStorageEngine();

    TrackingListener<String, String> listener = new TrackingListener<String, String>();
    storageEngine.registerListener(listener);
    Map<String, String> map = new OffHeapHashMap<String, String>(new UnlimitedPageSource(new HeapBufferSource()), (StorageEngine<? super String, ? super String>) storageEngine);

    HashMap<String, String> values = new HashMap<String, String>();
    for (int i = 0; i < 100; i++) {
      values.put(Integer.toString(i), Integer.toHexString(i));
    }

    map.putAll(values);
    Assert.assertThat(listener.getTrackingMap(), IsEqual.equalTo(map));

    for (int i = 0; i < 100; i++) {
      map.remove(Integer.toString(i));
      Assert.assertThat(listener.getTrackingMap(), IsEqual.equalTo(map));
    }
  }

  @Test
  public void testListeningForClears() {
    ListenableStorageEngine<String, String> storageEngine = createStorageEngine();

    TrackingListener<String, String> listener = new TrackingListener<String, String>();
    storageEngine.registerListener(listener);
    Map<String, String> map = new OffHeapHashMap<String, String>(new UnlimitedPageSource(new HeapBufferSource()), (StorageEngine<? super String, ? super String>) storageEngine);

    HashMap<String, String> values = new HashMap<String, String>();
    for (int i = 0; i < 100; i++) {
      values.put(Integer.toString(i), Integer.toHexString(i));
    }

    map.putAll(values);
    Assert.assertThat(listener.getTrackingMap(), IsEqual.equalTo(map));

    map.clear();
    Assert.assertThat(listener.getTrackingMap(), IsEqual.equalTo(map));
  }

  protected abstract ListenableStorageEngine<String, String> createStorageEngine();

  static class TrackingListener<K, V> implements RuntimeStorageEngineListener<K, V>, RecoveryStorageEngineListener<K, V> {

    private final Map<Long, Entry<K, V>> trackingMap = new HashMap<Long, Map.Entry<K,V>>();
    private final Set<Long> pinnedEncodings = new HashSet<Long>();

    @Override
    public void written(K key, V value, ByteBuffer binaryKey, ByteBuffer binaryValue, int hash, int metadata,
                        long encoding) {
      Assert.assertThat(key.hashCode(), Is.is(hash));
      Entry<K, V> previous = trackingMap.put(encoding, new SimpleEntry<K, V>(key, value));
      Assert.assertThat(previous, IsNull.nullValue());
    }

    @Override
    public void freed(long encoding, int hash, ByteBuffer key, boolean removed) {
      Entry<K, V> previous = trackingMap.remove(encoding);
      Assert.assertThat(previous, IsNull.notNullValue());
      Assert.assertThat(previous.getKey().hashCode(), Is.is(hash));
    }

    @Override
    public void cleared() {
      trackingMap.clear();
    }

    @Override
    public void copied(int hash, long oldEncoding, long newEncoding, int metadata) {
      Entry<K, V> mapping = trackingMap.get(oldEncoding);
      Assert.assertThat(mapping, IsNull.notNullValue());
      Assert.assertThat(mapping.getKey().hashCode(), Is.is(hash));
      Entry<K, V> previous = trackingMap.put(newEncoding, mapping);
      Assert.assertThat(previous, IsNull.nullValue());
      if ((Metadata.PINNED & metadata) == 0) {
        Assert.assertTrue(pinnedEncodings.remove(oldEncoding));
      } else {
        Assert.assertFalse(pinnedEncodings.remove(oldEncoding));
        Assert.assertTrue(pinnedEncodings.add(newEncoding));
      }
    }

    public Map<K, V> getTrackingMap() {
      Map<K, V> result = new HashMap<K, V>();
      for (Entry<K, V> entry : trackingMap.values()) {
        V previous = result.put(entry.getKey(), entry.getValue());
        Assert.assertThat(previous, IsNull.nullValue());
      }
      return result;
    }

    @Override
    public void recovered(Callable<? extends K> key, Callable<? extends V> value, ByteBuffer binaryKey, ByteBuffer binaryValue, int hash, int metadata, long encoding) {
      try {
        written(key.call(), value.call(), binaryKey, binaryValue, hash, metadata, encoding);
      } catch (Exception ex) {
        throw new AssertionError(ex);
      }
    }
  }
}
