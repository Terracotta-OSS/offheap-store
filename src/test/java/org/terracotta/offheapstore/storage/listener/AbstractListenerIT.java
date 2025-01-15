/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
 * Copyright IBM Corp. 2024, 2025
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
package org.terracotta.offheapstore.storage.listener;

import org.terracotta.offheapstore.storage.listener.ListenableStorageEngine;
import org.terracotta.offheapstore.storage.listener.RecoveryStorageEngineListener;
import org.terracotta.offheapstore.storage.listener.RuntimeStorageEngineListener;
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

import org.terracotta.offheapstore.Metadata;
import org.terracotta.offheapstore.OffHeapHashMap;
import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.storage.StorageEngine;

@SuppressWarnings("unchecked")
public abstract class AbstractListenerIT {

  @Test
  public void testListeningForPuts() {
    ListenableStorageEngine<String, String> storageEngine = createStorageEngine();

    TrackingListener<String, String> listener = new TrackingListener<>();
    storageEngine.registerListener(listener);
    Map<String, String> map = new OffHeapHashMap<>(new UnlimitedPageSource(new HeapBufferSource()), (StorageEngine<? super String, ? super String>) storageEngine);

    for (int i = 0; i < 100; i++) {
      map.put(Integer.toString(i), Integer.toHexString(i));
      Assert.assertThat(listener.getTrackingMap(), IsEqual.equalTo(map));
    }
  }

  @Test
  public void testListeningForRemoves() {
    ListenableStorageEngine<String, String> storageEngine = createStorageEngine();

    TrackingListener<String, String> listener = new TrackingListener<>();
    storageEngine.registerListener(listener);
    Map<String, String> map = new OffHeapHashMap<>(new UnlimitedPageSource(new HeapBufferSource()), (StorageEngine<? super String, ? super String>) storageEngine);

    HashMap<String, String> values = new HashMap<>();
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

    TrackingListener<String, String> listener = new TrackingListener<>();
    storageEngine.registerListener(listener);
    Map<String, String> map = new OffHeapHashMap<>(new UnlimitedPageSource(new HeapBufferSource()), (StorageEngine<? super String, ? super String>) storageEngine);

    HashMap<String, String> values = new HashMap<>();
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

    private final Map<Long, Entry<K, V>> trackingMap = new HashMap<>();
    private final Set<Long> pinnedEncodings = new HashSet<>();

    @Override
    public void written(K key, V value, ByteBuffer binaryKey, ByteBuffer binaryValue, int hash, int metadata,
                        long encoding) {
      Assert.assertThat(key.hashCode(), Is.is(hash));
      Entry<K, V> previous = trackingMap.put(encoding, new SimpleEntry<>(key, value));
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
      Map<K, V> result = new HashMap<>();
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
