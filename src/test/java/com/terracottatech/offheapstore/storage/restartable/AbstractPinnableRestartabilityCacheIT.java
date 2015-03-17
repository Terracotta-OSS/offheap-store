package com.terracottatech.offheapstore.storage.restartable;

import static org.terracotta.offheapstore.util.MemoryUnit.MEGABYTES;

import java.io.File;
import java.nio.ByteBuffer;

import org.hamcrest.collection.IsMapContaining;
import org.hamcrest.core.Is;
import org.hamcrest.core.IsNot;
import org.hamcrest.number.OrderingComparison;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.RestartStoreFactory;
import com.terracottatech.frs.object.RegisterableObjectManager;
import org.terracotta.offheapstore.pinning.PinnableCache;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.storage.portability.StringPortability;
import org.terracotta.offheapstore.util.MemoryUnit;

public abstract class AbstractPinnableRestartabilityCacheIT extends AbstractRestartabilityCacheIT {

  @Test
  @Ignore("Rewrite this! Pinning isn't persisted!")
  public void testPinnedStatusSurvivesRestart() throws Exception {
    File directory = RestartabilityTestUtilities.createTempDirectory(getClass().getSimpleName() + ".testPinnedStatusSurvivesRestart_");
    ByteBuffer id = ByteBuffer.wrap("map".getBytes("US-ASCII"));
    
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = createRestartStore(objectMgr, directory);
      persistence.startup().get();
      try {
        Portability<String> portability = StringPortability.INSTANCE;

        PinnableCache<String, String> map = createRestartableMap(1, MEGABYTES, id, persistence, objectMgr, portability, portability, true);
        try {
          map.setPinning("foo", true);
          map.put("foo", "bar");
          map.put("baz", "foo");
          map.put("foo", "bob");
        } finally {
          destroyMap(map);
        }
      } finally {
        persistence.shutdown();
      }
    }
    
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = createRestartStore(objectMgr, directory);
      try {
        Portability<String> portability = StringPortability.INSTANCE;

        PinnableCache<String, String> map = createRestartableMap(1, MEGABYTES, id, persistence, objectMgr, portability, portability, true);
        try {
          persistence.startup().get();

          Assert.assertThat(map.size(), Is.is(2));
          Assert.assertThat(map, IsMapContaining.hasEntry("foo", "bob"));
          Assert.assertThat(map, IsMapContaining.hasEntry("baz", "foo"));
          Assert.assertTrue(map.isPinned("foo"));
          Assert.assertFalse(map.isPinned("baz"));
        } finally {
          destroyMap(map);
        }
      } finally {
        persistence.shutdown();
      }
    }
  }
  
  @Test
  @Ignore("Rewrite this! Pinning isn't persisted!")
  public void testPinnedNotPresentSurvivesRestart() throws Exception {
    File directory = RestartabilityTestUtilities.createTempDirectory(getClass().getSimpleName() + ".testPinnedNotPresentSurvivesRestart_");
    ByteBuffer id = ByteBuffer.wrap("map".getBytes("US-ASCII"));
    
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = createRestartStore(objectMgr, directory);
      persistence.startup().get();
      try {
          Portability<String> portability = StringPortability.INSTANCE;
          
          PinnableCache<String, String> map = createRestartableMap(1, MEGABYTES, id, persistence, objectMgr, portability, portability, true);
          try {
            map.setPinning("foo", true);
            map.setPinning("baz", true);
            map.put("foo", "bar");
          } finally {
            destroyMap(map);
          }
      } finally {
        persistence.shutdown();
      }
    }
    
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = createRestartStore(objectMgr, directory);
      try {
        Portability<String> portability = StringPortability.INSTANCE;

        PinnableCache<String, String> map = createRestartableMap(1, MEGABYTES, id, persistence, objectMgr, portability, portability, true);
        try {

          persistence.startup().get();

          Assert.assertThat(map.size(), Is.is(1));
          Assert.assertThat(map, IsMapContaining.hasEntry("foo", "bar"));
          Assert.assertTrue(map.isPinned("foo"));
          Assert.assertTrue(map.isPinned("baz"));
        } finally {
          destroyMap(map);
        }
      } finally {
        persistence.shutdown();
      }
    }
  }
  
  @Test
  public void testPinnedThenUnpinSurvivesRestart() throws Exception {
    File directory = RestartabilityTestUtilities.createTempDirectory(getClass().getSimpleName() + ".testPinnedNotPresentSurvivesRestart_");
    ByteBuffer id = ByteBuffer.wrap("map".getBytes("US-ASCII"));
    
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = createRestartStore(objectMgr, directory);
      persistence.startup().get();
      try {
        Portability<String> portability = StringPortability.INSTANCE;

        PinnableCache<String, String> map = createRestartableMap(1, MEGABYTES, id, persistence, objectMgr, portability, portability, true);
        try {
          map.setPinning("foo", true);
          map.put("foo", "bar");
          map.setPinning("foo", false);
        } finally {
          destroyMap(map);
        }
      } finally {
        persistence.shutdown();
      }
    }
    
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = createRestartStore(objectMgr, directory);
      try {
        Portability<String> portability = StringPortability.INSTANCE;

        PinnableCache<String, String> map = createRestartableMap(1, MEGABYTES, id, persistence, objectMgr, portability, portability, true);
        try {
          persistence.startup().get();

          Assert.assertThat(map.size(), Is.is(1));
          Assert.assertThat(map, IsMapContaining.hasEntry("foo", "bar"));
          Assert.assertFalse(map.isPinned("foo"));
        } finally {
          destroyMap(map);
        }
      } finally {
        persistence.shutdown();
      }
    }
  }
  
  @Test
  @Ignore("Rewrite this! Pinning isn't persisted!")
  public void testRecoveredPinnedValueIsTrulyPinned() throws Exception {
    File directory = RestartabilityTestUtilities.createTempDirectory(getClass().getSimpleName() + ".testPinnedNotPresentSurvivesRestart_");
    ByteBuffer id = ByteBuffer.wrap("map".getBytes("US-ASCII"));
    
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = createRestartStore(objectMgr, directory);
      persistence.startup().get();
      try {
        Portability<String> portability = StringPortability.INSTANCE;

        PinnableCache<String, String> map = createRestartableMap(1, MEGABYTES, id, persistence, objectMgr, portability, portability, true);
        try {
          map.setPinning("foo", true);
          map.put("foo", "bar");
        } finally {
          destroyMap(map);
        }
      } finally {
        persistence.shutdown();
      }
    }
    
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = createRestartStore(objectMgr, directory);
      try {
        Portability<String> portability = StringPortability.INSTANCE;

        PinnableCache<String, String> map = createRestartableMap(1, MEGABYTES, id, persistence, objectMgr, portability, portability, true);
        try {
          persistence.startup().get();

          Assert.assertThat(map.size(), Is.is(1));
          Assert.assertThat(map, IsMapContaining.hasEntry("foo", "bar"));
          Assert.assertTrue(map.isPinned("foo"));

          StringBuilder sb = new StringBuilder("Na ");
          for (int i = 0; i < 1024; i++) {
            sb.append("na ");
          }
          sb.append("Batman!");
          String batman = sb.toString();

          for (int i = 0; i < 1024; i++) {
            map.put(Integer.toString(i), batman);
          }

          Assert.assertThat(map.size(), OrderingComparison.lessThan(1025));
          Assert.assertThat(map, IsMapContaining.hasEntry("foo", "bar"));
          Assert.assertTrue(map.isPinned("foo"));
        } finally {
          destroyMap(map);
        }
      } finally {
        persistence.shutdown();
      }
    }
  }
  
  @Test
  @Ignore("Rewrite this! Pinning isn't persisted!")
  public void testRecoveredPinnedThenUnpinnedValueIsTrulyUnpinned() throws Exception {
    File directory = RestartabilityTestUtilities.createTempDirectory(getClass().getSimpleName() + ".testPinnedNotPresentSurvivesRestart_");
    ByteBuffer id = ByteBuffer.wrap("map".getBytes("US-ASCII"));
    
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = createRestartStore(objectMgr, directory);
      persistence.startup().get();
      try {
        Portability<String> portability = StringPortability.INSTANCE;

        PinnableCache<String, String> map = createRestartableMap(1, MEGABYTES, id, persistence, objectMgr, portability, portability, true);
        try {
          map.setPinning("foo", true);
          map.put("foo", "bar");
        } finally {
          destroyMap(map);
        }
      } finally {
        persistence.shutdown();
      }
    }
    
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = createRestartStore(objectMgr, directory);
      try {
        Portability<String> portability = StringPortability.INSTANCE;

        PinnableCache<String, String> map = createRestartableMap(1, MEGABYTES, id, persistence, objectMgr, portability, portability, true);
        try {
          persistence.startup().get();

          Assert.assertThat(map.size(), Is.is(1));
          Assert.assertThat(map, IsMapContaining.hasEntry("foo", "bar"));
          Assert.assertTrue(map.isPinned("foo"));

          StringBuilder sb = new StringBuilder("Na ");
          for (int i = 0; i < 1024; i++) {
            sb.append("na ");
          }
          sb.append("Batman!");
          String batman = sb.toString();

          for (int i = 0; i < 1024; i++) {
            map.put(Integer.toString(i), batman);
          }

          Assert.assertThat(map.size(), OrderingComparison.lessThan(1025));
          Assert.assertThat(map, IsMapContaining.hasEntry("foo", "bar"));
          Assert.assertTrue(map.isPinned("foo"));
          
          map.setPinning("foo", false);
        } finally {
          destroyMap(map);
        }
      } finally {
        persistence.shutdown();
      }
    }

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = createRestartStore(objectMgr, directory);
      try {
        Portability<String> portability = StringPortability.INSTANCE;

        PinnableCache<String, String> map = createRestartableMap(1, MEGABYTES, id, persistence, objectMgr, portability, portability, true);
        try {
          persistence.startup().get();

          Assert.assertThat(map, IsMapContaining.hasEntry("foo", "bar"));
          Assert.assertFalse(map.isPinned("foo"));

          StringBuilder sb = new StringBuilder("Na ");
          for (int i = 0; i < 1024; i++) {
            sb.append("na ");
          }
          sb.append("Batman!");
          String batman = sb.toString();

          for (int i = 0; i < 1024; i++) {
            map.put(Integer.toString(i), batman);
          }

          Assert.assertThat(map.size(), OrderingComparison.lessThan(1025));
          Assert.assertThat(map, IsNot.not(IsMapContaining.hasEntry("foo", "bar")));
          Assert.assertFalse(map.isPinned("foo"));
        } finally {
          destroyMap(map);
        }
      } finally {
        persistence.shutdown();
      }
    }
  }
  
  @Override
  protected abstract <K, V> PinnableCache<K, V> createRestartableMap(long size, MemoryUnit unit, ByteBuffer id, RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence, RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean synchronous);
}
