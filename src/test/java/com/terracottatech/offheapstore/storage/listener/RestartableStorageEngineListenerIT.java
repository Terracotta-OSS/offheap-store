package com.terracottatech.offheapstore.storage.listener;

import static com.terracottatech.offheapstore.util.MemoryUnit.MEGABYTES;

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
import com.terracottatech.offheapstore.OffHeapHashMap;
import com.terracottatech.offheapstore.ReadWriteLockedOffHeapHashMap;
import com.terracottatech.offheapstore.buffersource.OffHeapBufferSource;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.paging.UpfrontAllocatingPageSource;
import com.terracottatech.offheapstore.storage.OffHeapBufferStorageEngine;
import com.terracottatech.offheapstore.storage.PointerSize;
import com.terracottatech.offheapstore.storage.portability.StringPortability;
import com.terracottatech.offheapstore.storage.restartable.LinkedNode;
import com.terracottatech.offheapstore.storage.restartable.LinkedNodePortability;
import com.terracottatech.offheapstore.storage.restartable.NoOpRestartStore;
import com.terracottatech.offheapstore.storage.restartable.OffHeapObjectManagerStripe;
import com.terracottatech.offheapstore.storage.restartable.RestartabilityTestUtilities;
import com.terracottatech.offheapstore.storage.restartable.RestartableStorageEngine;
import com.terracottatech.offheapstore.util.MemoryUnit;

public class RestartableStorageEngineListenerIT extends AbstractListenerIT {

  @Override
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
}
