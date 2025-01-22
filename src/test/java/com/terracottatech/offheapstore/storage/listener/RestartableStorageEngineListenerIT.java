package com.terracottatech.offheapstore.storage.listener;

import org.terracotta.offheapstore.storage.listener.RuntimeStorageEngineListener;
import org.terracotta.offheapstore.storage.listener.RecoveryStorageEngineListener;
import org.terracotta.offheapstore.storage.listener.ListenableStorageEngine;
import static org.terracotta.offheapstore.util.MemoryUnit.MEGABYTES;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;

import org.hamcrest.collection.IsMapContaining;
import org.hamcrest.core.Is;
import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Test;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.RestartStoreFactory;
import com.terracottatech.frs.object.RegisterableObjectManager;
import org.terracotta.offheapstore.Metadata;
import org.terracotta.offheapstore.OffHeapHashMap;
import org.terracotta.offheapstore.ReadWriteLockedOffHeapHashMap;
import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.OffHeapBufferStorageEngine;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.StorageEngine;
import org.terracotta.offheapstore.storage.portability.StringPortability;
import com.terracottatech.offheapstore.storage.restartable.LinkedNode;
import com.terracottatech.offheapstore.storage.restartable.LinkedNodePortability;
import com.terracottatech.offheapstore.storage.restartable.NoOpRestartStore;
import com.terracottatech.offheapstore.storage.restartable.OffHeapObjectManagerStripe;
import com.terracottatech.offheapstore.storage.restartable.RestartabilityTestUtilities;
import com.terracottatech.offheapstore.storage.restartable.RestartableStorageEngine;
import org.terracotta.offheapstore.util.MemoryUnit;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import org.hamcrest.core.IsNull;

public class RestartableStorageEngineListenerIT {

  protected ListenableStorageEngine<String, String> createStorageEngine() {
    PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), MemoryUnit.MEGABYTES.toBytes(4), MemoryUnit.KILOBYTES.toBytes(4));
    OffHeapBufferStorageEngine<String, LinkedNode<String>> delegate = new OffHeapBufferStorageEngine<String, LinkedNode<String>>(PointerSize.INT, source, MemoryUnit.KILOBYTES.toBytes(1), StringPortability.INSTANCE, new LinkedNodePortability<String>(StringPortability.INSTANCE));
    return new RestartableStorageEngine<OffHeapBufferStorageEngine<String, LinkedNode<String>>, String, String, String>("id", new NoOpRestartStore<String, ByteBuffer, ByteBuffer>(), delegate, true);
  }
  
  @Test
  public void testRecoveryFiresListeners() throws Exception {
    File directory = RestartabilityTestUtilities.createTempDirectory(getClass().getSimpleName() + ".testBasicRecovery_");
    ByteBuffer id = ByteBuffer.wrap("map".getBytes("US-ASCII"));

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence =
              RestartStoreFactory.createStore(objectMgr, directory,
                                              MEGABYTES.toBytes(1));
      persistence.startup().get();
      try {
          PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), MEGABYTES.toBytes(1), MemoryUnit.MEGABYTES.toBytes(1));
          RestartableStorageEngine<?, ByteBuffer, String, String> storageEngine = createStorageEngine(source, id, persistence);
          TrackingListener<String, String> listener = new TrackingListener<String, String>();
          storageEngine.registerListener(listener);
          
          OffHeapHashMap<String, String> map = new ReadWriteLockedOffHeapHashMap<String, String>(source, storageEngine);
          objectMgr.registerObject(new OffHeapObjectManagerStripe<ByteBuffer>(id, map));
          try {
            map.put("foo", "bar");
            Assert.assertThat(listener.getTrackingMap(), IsEqual.<Map<String, String>>equalTo(map));
            map.put("baz", "foo");
            Assert.assertThat(listener.getTrackingMap(), IsEqual.<Map<String, String>>equalTo(map));
            map.put("foo", "bob");
            Assert.assertThat(listener.getTrackingMap(), IsEqual.<Map<String, String>>equalTo(map));
          } finally {
            map.destroy();
          }
      } finally {
        persistence.shutdown();
      }
    }

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence =
              RestartStoreFactory.createStore(objectMgr, directory,
                                              MEGABYTES.toBytes(1));
      try {
          PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), MEGABYTES.toBytes(1), MemoryUnit.MEGABYTES.toBytes(1));
          RestartableStorageEngine<?, ByteBuffer, String, String> storageEngine = createStorageEngine(source, id, persistence);
          TrackingListener<String, String> listener = new TrackingListener<String, String>();
          storageEngine.registerListener(listener);
          
          OffHeapHashMap<String, String> map = new ReadWriteLockedOffHeapHashMap<String, String>(source, storageEngine);
          objectMgr.registerObject(new OffHeapObjectManagerStripe<ByteBuffer>(id, map));
          try {

            persistence.startup().get();

            Assert.assertThat(map.size(), Is.is(2));
            Assert.assertThat(map, IsMapContaining.hasEntry("foo", "bob"));
            Assert.assertThat(map, IsMapContaining.hasEntry("baz", "foo"));
            Assert.assertThat(listener.getTrackingMap(), IsEqual.<Map<String, String>>equalTo(map));

            map.put("alice", "bob");
            Assert.assertThat(listener.getTrackingMap(), IsEqual.<Map<String, String>>equalTo(map));
          } finally {
            map.destroy();
          }

      } finally {
        persistence.shutdown();
      }
    }

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence =
              RestartStoreFactory.createStore(objectMgr, directory,
                                              MEGABYTES.toBytes(1));
      try {
          PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), MEGABYTES.toBytes(1), MemoryUnit.MEGABYTES.toBytes(1));
          RestartableStorageEngine<?, ByteBuffer, String, String> storageEngine = createStorageEngine(source, id, persistence);
          TrackingListener<String, String> listener = new TrackingListener<String, String>();
          storageEngine.registerListener(listener);
          
          OffHeapHashMap<String, String> map = new ReadWriteLockedOffHeapHashMap<String, String>(source, storageEngine);
          objectMgr.registerObject(new OffHeapObjectManagerStripe<ByteBuffer>(id, map));
          try {

            persistence.startup().get();

            Assert.assertThat(map.size(), Is.is(3));
            Assert.assertThat(map, IsMapContaining.hasEntry("foo", "bob"));
            Assert.assertThat(map, IsMapContaining.hasEntry("baz", "foo"));
            Assert.assertThat(map, IsMapContaining.hasEntry("alice", "bob"));
            Assert.assertThat(listener.getTrackingMap(), IsEqual.<Map<String, String>>equalTo(map));
          } finally {
            map.destroy();
          }
      } finally {
        persistence.shutdown();
      }
    }
  }
  
  private RestartableStorageEngine<?, ByteBuffer, String, String> createStorageEngine(PageSource source, ByteBuffer id, RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence) {
    OffHeapBufferStorageEngine<String, LinkedNode<String>> delegateEngine = new OffHeapBufferStorageEngine<String, LinkedNode<String>>(PointerSize.INT, source, MemoryUnit.KILOBYTES.toBytes(1), StringPortability.INSTANCE, new LinkedNodePortability<String>(StringPortability.INSTANCE));
    return new RestartableStorageEngine<OffHeapBufferStorageEngine<String, LinkedNode<String>>, ByteBuffer, String, String>(id, persistence, delegateEngine, true);
  }

  static class TrackingListener<K, V> implements RuntimeStorageEngineListener<K, V>, RecoveryStorageEngineListener<K, V> {

    private final Map<Long, Map.Entry<K, V>> trackingMap = new HashMap<Long, Map.Entry<K, V>>();
    private final Set<Long> pinnedEncodings = new HashSet<Long>();

    @Override
    public void written(K key, V value, ByteBuffer binaryKey, ByteBuffer binaryValue, int hash, int metadata, long encoding) {
      Assert.assertThat(key.hashCode(), Is.is(hash));
      Map.Entry<K, V> previous = trackingMap.put(encoding, new AbstractMap.SimpleEntry<K, V>(key, value));
      Assert.assertThat(previous, IsNull.nullValue());
    }

    @Override
    public void freed(long encoding, int hash, ByteBuffer key, boolean removed) {
      Map.Entry<K, V> previous = trackingMap.remove(encoding);
      Assert.assertThat(previous, IsNull.notNullValue());
      Assert.assertThat(previous.getKey().hashCode(), Is.is(hash));
    }

    @Override
    public void cleared() {
      trackingMap.clear();
    }

    @Override
    public void copied(int hash, long oldEncoding, long newEncoding, int metadata) {
      Map.Entry<K, V> mapping = trackingMap.get(oldEncoding);
      Assert.assertThat(mapping, IsNull.notNullValue());
      Assert.assertThat(mapping.getKey().hashCode(), Is.is(hash));
      Map.Entry<K, V> previous = trackingMap.put(newEncoding, mapping);
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
      for (Map.Entry<K, V> entry : trackingMap.values()) {
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
}
