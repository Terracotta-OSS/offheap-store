package com.terracottatech.offheapstore.storage.restartable;

import static com.terracottatech.offheapstore.util.MemoryUnit.KILOBYTES;
import static com.terracottatech.offheapstore.util.MemoryUnit.MEGABYTES;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;
import java.util.Properties;

import org.hamcrest.collection.IsMapContaining;
import org.hamcrest.core.Is;
import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Test;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.RestartStoreException;
import com.terracottatech.frs.RestartStoreFactory;
import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.RegisterableObjectManager;
import com.terracottatech.frs.recovery.RecoveryException;
import com.terracottatech.offheapstore.OffHeapHashMap;
import com.terracottatech.offheapstore.concurrent.AbstractConcurrentOffHeapMap;
import com.terracottatech.offheapstore.exceptions.OversizeMappingException;
import com.terracottatech.offheapstore.storage.portability.ByteArrayPortability;
import com.terracottatech.offheapstore.storage.portability.Portability;
import com.terracottatech.offheapstore.storage.portability.StringPortability;
import com.terracottatech.offheapstore.storage.restartable.portability.RestartableSerializablePortability;
import com.terracottatech.offheapstore.util.MemoryUnit;
import com.terracottatech.offheapstore.util.PointerSizeParameterizedTest;

import java.awt.AWTPermission;
import java.io.IOException;

public abstract class AbstractRestartabilityIT extends PointerSizeParameterizedTest {

  @Test
  public void testPutPath() throws Exception {
    File directory = RestartabilityTestUtilities.createTempDirectory(getClass().getSimpleName() + ".testPutPath_");
    ByteBuffer id = ByteBuffer.wrap("map".getBytes("US-ASCII"));

    RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
    RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = createRestartStore(objectMgr, directory);
    persistence.startup().get();
    try {
      Portability<String> portability = StringPortability.INSTANCE;

      Map<String, String> map = createRestartableMap(1, MEGABYTES, id, persistence, objectMgr, portability, portability, true);
      try {
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

  @Test
  public void testBasicRecovery() throws Exception {
    File directory = RestartabilityTestUtilities.createTempDirectory(getClass().getSimpleName() + ".testBasicRecovery_");
    ByteBuffer id = ByteBuffer.wrap("map".getBytes("US-ASCII"));

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = createRestartStore(objectMgr, directory);
      persistence.startup().get();
      try {
          Portability<String> portability = StringPortability.INSTANCE;

          Map<String, String> map = createRestartableMap(1, MEGABYTES, id, persistence, objectMgr, portability, portability, true);
          try {
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
          Map<String, String> map = createRestartableMap(1, MEGABYTES, id, persistence, objectMgr, portability, portability, true);
          try {

            persistence.startup().get();

            Assert.assertThat(map.size(), Is.is(2));
            Assert.assertThat(map, IsMapContaining.hasEntry("foo", "bob"));
            Assert.assertThat(map, IsMapContaining.hasEntry("baz", "foo"));

            map.put("alice", "bob");
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
          Map<String, String> map = createRestartableMap(1, MEGABYTES, id, persistence, objectMgr, portability, portability, true);
          try {

            persistence.startup().get();

            Assert.assertThat(map.size(), Is.is(3));
            Assert.assertThat(map, IsMapContaining.hasEntry("foo", "bob"));
            Assert.assertThat(map, IsMapContaining.hasEntry("baz", "foo"));
            Assert.assertThat(map, IsMapContaining.hasEntry("alice", "bob"));
          } finally {
            destroyMap(map);
          }
      } finally {
        persistence.shutdown();
      }
    }
  }

  @Test
  public void testBasicTypes() throws Exception {
    File directory = RestartabilityTestUtilities.createTempDirectory(getClass().getSimpleName() + ".testBasicTypes_");
    ByteBuffer mapId = ByteBuffer.wrap("map".getBytes("US-ASCII"));
    ByteBuffer portabilityId = ByteBuffer.wrap("portability".getBytes("US-ASCII"));

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = createRestartStore(objectMgr, directory);
      persistence.startup().get();
      try {
        RestartableSerializablePortability<ByteBuffer> portability = new RestartableSerializablePortability<ByteBuffer>(portabilityId, persistence, true);
        objectMgr.registerObject(portability);

        Map<String, String> map = createRestartableMap(1, MEGABYTES, mapId, persistence, objectMgr, portability, portability, true);
        try {
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
          RestartableSerializablePortability<ByteBuffer> portability = new RestartableSerializablePortability<ByteBuffer>(portabilityId, persistence, true);
          objectMgr.registerObject(portability);

          Map<String, String> map = createRestartableMap(1, MEGABYTES, mapId, persistence, objectMgr, portability, portability, true);
          try {

            persistence.startup().get();

            Assert.assertThat(map.size(), Is.is(2));
            Assert.assertThat(map, IsMapContaining.hasEntry("foo", "bob"));
            Assert.assertThat(map, IsMapContaining.hasEntry("baz", "foo"));

            map.put("alice", "bob");
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
          RestartableSerializablePortability<ByteBuffer> portability = new RestartableSerializablePortability<ByteBuffer>(portabilityId, persistence, true);
          objectMgr.registerObject(portability);

          Map<String, String> map = createRestartableMap(1, MEGABYTES, mapId, persistence, objectMgr, portability, portability, true);
          try {

            persistence.startup().get();

            Assert.assertThat(map.size(), Is.is(3));
            Assert.assertThat(map, IsMapContaining.hasEntry("foo", "bob"));
            Assert.assertThat(map, IsMapContaining.hasEntry("baz", "foo"));
            Assert.assertThat(map, IsMapContaining.hasEntry("alice", "bob"));
          } finally {
            destroyMap(map);
          }
      } finally {
        persistence.shutdown();
      }
    }
  }

  @Test
  public void testComplexTypes() throws Exception {
    File directory = RestartabilityTestUtilities.createTempDirectory(getClass().getSimpleName() + ".testComplexTypes_");
    ByteBuffer mapId = ByteBuffer.wrap("map".getBytes("US-ASCII"));
    ByteBuffer portabilityId = ByteBuffer.wrap("portability".getBytes("US-ASCII"));

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = createRestartStore(objectMgr, directory);
      persistence.startup().get();
      try {
        RestartableSerializablePortability<ByteBuffer> portability = new RestartableSerializablePortability<ByteBuffer>(portabilityId, persistence, true);
        objectMgr.registerObject(portability);

        Map<Serializable, Serializable> map = createRestartableMap(1, MEGABYTES, mapId, persistence, objectMgr, portability, portability, true);
        try {
          Calendar calendar = Calendar.getInstance();
          calendar.clear();
          calendar.set(81, 11, 29);
          map.put(new BigInteger("42"), calendar);
          map.put(new AWTPermission("do-everything", "hehehe"), DateFormat.getDateInstance());
          map.put(new BigInteger("42"), new ArrayList<Serializable>(Arrays.<Serializable>asList("foo", calendar.getTime())));
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
        RestartableSerializablePortability<ByteBuffer> portability = new RestartableSerializablePortability<ByteBuffer>(portabilityId, persistence, true);
        objectMgr.registerObject(portability);

        Map<Serializable, Serializable> map = createRestartableMap(1, MEGABYTES, mapId,
                                                                   persistence,
                                                                   objectMgr,
                                                                   portability,
                                                                   portability,
                                                                   true);
        try {
          persistence.startup().get();

          Calendar calendar = Calendar.getInstance();
          calendar.clear();
          calendar.set(81, 11, 29);
          Assert.assertThat(map.size(), Is.is(2));
          Assert.assertThat(map, IsMapContaining.<Serializable, Serializable>hasEntry(new BigInteger("42"),  new ArrayList<Serializable>(Arrays.<Serializable>asList("foo", calendar.getTime()))));
          Assert.assertThat(map, IsMapContaining.<Serializable, Serializable>hasEntry(new AWTPermission("do-everything", "hehehe"), DateFormat.getDateInstance()));

          map.put("alice", "bob");
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
        RestartableSerializablePortability<ByteBuffer> portability = new RestartableSerializablePortability<ByteBuffer>(portabilityId, persistence, true);
        objectMgr.registerObject(portability);

        Map<Serializable, Serializable> map = createRestartableMap(1, MEGABYTES, mapId,
                                                                   persistence,
                                                                   objectMgr,
                                                                   portability,
                                                                   portability,
                                                                   true);
        try {
          persistence.startup().get();

          Calendar calendar = Calendar.getInstance();
          calendar.clear();
          calendar.set(81, 11, 29);
          Assert.assertThat(map.size(), Is.is(3));
          Assert.assertThat(map, IsMapContaining.<Serializable, Serializable>hasEntry(new BigInteger("42"),  new ArrayList<Serializable>(Arrays.<Serializable>asList("foo", calendar.getTime()))));
          Assert.assertThat(map, IsMapContaining.<Serializable, Serializable>hasEntry(new AWTPermission("do-everything", "hehehe"), DateFormat.getDateInstance()));
          Assert.assertThat(map, IsMapContaining.<Serializable, Serializable>hasEntry("alice", "bob"));
        } finally {
          destroyMap(map);
        }
      } finally {
        persistence.shutdown();
      }
    }
  }

  @Test
  public void testCustomEquals() throws Exception {
    File directory = RestartabilityTestUtilities.createTempDirectory(getClass().getSimpleName() + ".testCustomEquals_");
    ByteBuffer mapId = ByteBuffer.wrap("map".getBytes("US-ASCII"));
    ByteBuffer portabilityId = ByteBuffer.wrap("portability".getBytes("US-ASCII"));

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = createRestartStore(objectMgr, directory);
      persistence.startup().get();
      try {
        RestartableSerializablePortability<ByteBuffer> portability = new RestartableSerializablePortability<ByteBuffer>(portabilityId, persistence, true);
        objectMgr.registerObject(portability);

        Map<CustomEqualsType, String> map = createRestartableMap(1, MEGABYTES, mapId, persistence, objectMgr, portability, portability, true);
        try {
          map.put(new CustomEqualsType("foo", 42), "bar");
          map.put(new CustomEqualsType("baz", 43), "foo");
          map.put(new CustomEqualsType("foo", 41), "bob");

          Assert.assertThat(map.size(), IsEqual.equalTo(2));
          Assert.assertThat(map, IsMapContaining.hasEntry(new CustomEqualsType("foo", 44), "bob"));
          Assert.assertThat(map, IsMapContaining.hasEntry(new CustomEqualsType("baz", 44), "foo"));
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
        RestartableSerializablePortability<ByteBuffer> portability = new RestartableSerializablePortability<ByteBuffer>(portabilityId, persistence, true);
        objectMgr.registerObject(portability);

        Map<CustomEqualsType, String> map = createRestartableMap(1, MEGABYTES, mapId, persistence, objectMgr, portability, portability, true);
        try {
          persistence.startup().get();

          Assert.assertThat(map.size(), IsEqual.equalTo(2));
          Assert.assertThat(map, IsMapContaining.hasEntry(new CustomEqualsType("foo", 44), "bob"));
          Assert.assertThat(map, IsMapContaining.hasEntry(new CustomEqualsType("baz", 44), "foo"));
        } finally {
          destroyMap(map);
        }
      } finally {
        persistence.shutdown();
      }
    }
  }

  static final class CustomEqualsType implements Serializable {

    private static final long serialVersionUID = -6696999227380160501L;

    private final String string;
    @SuppressWarnings("unused")
    private final Integer integer;

    public CustomEqualsType(String string, Integer integer) {
      this.string = string;
      this.integer = integer;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof CustomEqualsType) {
        return string.equals(((CustomEqualsType) o).string);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return string.hashCode();
    }
  }

  @Test
  public void testCollidingHashcodes() throws Exception {
    File directory = RestartabilityTestUtilities.createTempDirectory(getClass().getSimpleName() + ".testCollidingHashcodes");
    ByteBuffer mapId = ByteBuffer.wrap("map".getBytes("US-ASCII"));
    ByteBuffer portabilityId = ByteBuffer.wrap("portability".getBytes("US-ASCII"));

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = createRestartStore(objectMgr, directory);
      persistence.startup().get();
      try {
        RestartableSerializablePortability<ByteBuffer> portability = new RestartableSerializablePortability<ByteBuffer>(portabilityId, persistence, true);
        objectMgr.registerObject(portability);

        Map<CollidingHashcodeType, String> map = createRestartableMap(1, MEGABYTES, mapId, persistence, objectMgr, portability, portability, true);
        try {
          map.put(new CollidingHashcodeType("foo"), "bar");
          map.put(new CollidingHashcodeType("baz"), "foo");
          map.put(new CollidingHashcodeType("alice"), "bob");
          map.put(new CollidingHashcodeType("foo"), "bob");

          Assert.assertThat(map.size(), IsEqual.equalTo(3));
          Assert.assertThat(map, IsMapContaining.hasEntry(new CollidingHashcodeType("foo"), "bob"));
          Assert.assertThat(map, IsMapContaining.hasEntry(new CollidingHashcodeType("baz"), "foo"));
          Assert.assertThat(map, IsMapContaining.hasEntry(new CollidingHashcodeType("alice"), "bob"));
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
        RestartableSerializablePortability<ByteBuffer> portability = new RestartableSerializablePortability<ByteBuffer>(portabilityId, persistence, true);
        objectMgr.registerObject(portability);

        Map<CollidingHashcodeType, String> map = createRestartableMap(1, MEGABYTES, mapId, persistence, objectMgr, portability, portability, true);
        try {
          persistence.startup().get();

          Assert.assertThat(map.size(), IsEqual.equalTo(3));
          Assert.assertThat(map, IsMapContaining.hasEntry(new CollidingHashcodeType("foo"), "bob"));
          Assert.assertThat(map, IsMapContaining.hasEntry(new CollidingHashcodeType("baz"), "foo"));
          Assert.assertThat(map, IsMapContaining.hasEntry(new CollidingHashcodeType("alice"), "bob"));
        } finally {
          destroyMap(map);
        }
      } finally {
        persistence.shutdown();
      }
    }
  }
  
  static final class CollidingHashcodeType implements Serializable {

    private static final long serialVersionUID = 2636083977447548382L;
    
    private final String string;

    public CollidingHashcodeType(String string) {
      this.string = string;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof CollidingHashcodeType) {
        return string.equals(((CollidingHashcodeType) o).string);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return 42;
    }
  }

  @Test
  public void testEvictionOnRecovery() throws Exception {
    File directory = RestartabilityTestUtilities.createTempDirectory(getClass().getSimpleName() + ".testEvictionOnRecovery_");
    ByteBuffer id = ByteBuffer.wrap("map".getBytes("US-ASCII"));

    int initialSize;
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = createRestartStore(objectMgr, directory);
      persistence.startup().get();
      try {
        Portability<String> keyPortability = StringPortability.INSTANCE;
        Portability<byte[]> valuePortability = ByteArrayPortability.INSTANCE;

        Map<String, byte[]> map = createRestartableMap(200, KILOBYTES, id, persistence, objectMgr, keyPortability, valuePortability, true);
        try {
          byte[] payload = new byte[1024];

          for (int i = 0; ; i++) {
            try {
              map.put(Integer.toString(i), payload);
            } catch (OversizeMappingException e) {
              break;
            }
          }

          initialSize = map.size();
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

        Map<?, ?> map = createRestartableMap(50, KILOBYTES, id, persistence, objectMgr, portability, portability, true);
        try {
          try {
            persistence.startup().get();
            Assert.fail("Expected failure to recover due to lack of space.");
          } catch (RecoveryException e) {
            //expected
          }
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

        Map<String, String> map = createRestartableMap(200, KILOBYTES, id, persistence, objectMgr, portability, portability, true);
        try {
          persistence.startup().get();

          Assert.assertThat(map.size(), IsEqual.equalTo(initialSize));
        } finally {
          destroyMap(map);
        }
      } finally {
        persistence.shutdown();
      }
    }
  }

  @Test
  public void testObjectManagerSizeJustMap() throws Exception {
    File directory = RestartabilityTestUtilities.createTempDirectory(getClass().getSimpleName() + ".testObjectManagerSizeJustMap_");
    ByteBuffer id = ByteBuffer.wrap("map".getBytes("US-ASCII"));

    RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
    RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = createRestartStore(objectMgr, directory);
    persistence.startup().get();
    try {
      Portability<String> portability = StringPortability.INSTANCE;

      Map<String, String> map = createRestartableMap(1, MEGABYTES, id, persistence,
                                                     objectMgr, portability, portability, true);
      try {
        map.put("foo", "bar");
        assertThat(objectMgr.size(), is(1L));

        map.put("baz", "bar");
        assertThat(objectMgr.size(), is(2L));

        map.remove("foo");
        assertThat(objectMgr.size(), is(1L));

        map.remove("baz");
        assertThat(objectMgr.size(), is(0L));

        map.put("apple", "fruit");
        assertThat(objectMgr.size(), is(1L));

        map.remove("apple");
        assertThat(objectMgr.size(), is(0L));

        map.put("apple", "computer");
        assertThat(objectMgr.size(), is(1L));

        map.put("a", "b");
        assertThat(objectMgr.size(), is(2L));

        map.put("b", "c");
        assertThat(objectMgr.size(), is(3L));
      } finally {
        destroyMap(map);
      }
    } finally {
      persistence.shutdown();
    }
  }

  @Test
  public void testObjectManagerSizeWithSerialization() throws Exception {
    File directory = RestartabilityTestUtilities.createTempDirectory(getClass().getSimpleName() + ".testObjectManagerSizeWithSerialization_");
    ByteBuffer mapId = ByteBuffer.wrap("map".getBytes("US-ASCII"));
    ByteBuffer portabilityId = ByteBuffer.wrap("portability".getBytes("US-ASCII"));

    RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
    RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = createRestartStore(objectMgr, directory);
    persistence.startup().get();
    try {
      RestartableSerializablePortability<ByteBuffer> portability = new RestartableSerializablePortability<ByteBuffer>(portabilityId, persistence, true);
      objectMgr.registerObject(portability);

      Map<Serializable, Serializable> map = createRestartableMap(1, MEGABYTES, mapId,
                                                                 persistence, objectMgr,
                                                                 portability, portability, true);
      try {
        map.put("foo", "bar");
        assertThat(objectMgr.size(), is(1L));

        map.put(Integer.valueOf(42), "bar");
        //mappings + {j.l.Integer, j.l.Number}
        assertThat(objectMgr.size(), is(4L));

        map.remove("foo");
        //mappings + {j.l.Integer, j.l.Number}
        assertThat(objectMgr.size(), is(3L));

        map.remove(Integer.valueOf(42));
        //mappings + {j.l.Integer, j.l.Number}
        assertThat(objectMgr.size(), is(2L));

        map.put(Long.valueOf(0xabb1e), "fruit");
        //mappings + {j.l.Integer, j.l.Number, j.l.Long}
        assertThat(objectMgr.size(), is(4L));

        map.remove(Long.valueOf(0xabb1e));
        //mappings + {j.l.Integer, j.l.Number, j.l.Long}
        assertThat(objectMgr.size(), is(3L));

        map.put("apple", "computer");
        //mappings + {j.l.Integer, j.l.Number, j.l.Long}
        assertThat(objectMgr.size(), is(4L));
      } finally {
        destroyMap(map);
      }
    } finally {
      persistence.shutdown();
    }
  }

  @Test
  public void testClear() throws Exception {
    File directory = RestartabilityTestUtilities.createTempDirectory(getClass().getSimpleName() + ".testClear_");
    ByteBuffer id = ByteBuffer.wrap("map".getBytes("US-ASCII"));

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = createRestartStore(objectMgr, directory);
      persistence.startup().get();
      try {
          Portability<String> portability = StringPortability.INSTANCE;

          Map<String, String> map = createRestartableMap(1, MEGABYTES, id, persistence, objectMgr, portability, portability, true);
          try {
            map.put("foo", "bar");
            map.put("baz", "foo");
            map.put("foo", "bob");
            map.clear();
            map.put("foo", "noo");
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
          Map<String, String> map = createRestartableMap(1, MEGABYTES, id, persistence, objectMgr, portability, portability, true);
          try {

            persistence.startup().get();

            Assert.assertThat(map.size(), Is.is(1));
            Assert.assertThat(map, IsMapContaining.hasEntry("foo", "noo"));
            map.clear();
            map.put("alice", "bob");
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
          Map<String, String> map = createRestartableMap(1, MEGABYTES, id, persistence, objectMgr, portability, portability, true);
          try {

            persistence.startup().get();

            Assert.assertThat(map.size(), Is.is(1));
            Assert.assertThat(map, IsMapContaining.hasEntry("alice", "bob"));
          } finally {
            destroyMap(map);
          }
      } finally {
        persistence.shutdown();
      }
    }
  }
  
  protected RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> createRestartStore(ObjectManager objectMgr, File directory, Properties properties) throws IOException, RestartStoreException {
    return RestartStoreFactory.createStore(objectMgr, directory, properties);
  }
  
  protected RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> createRestartStore(ObjectManager objectMgr, File directory) throws IOException, RestartStoreException {
    Properties properties = new Properties();
    properties.setProperty(FrsProperty.IO_NIO_SEGMENT_SIZE.shortName(), Integer.toString(MEGABYTES.toBytes(1)));
    return createRestartStore(objectMgr, directory, properties);
  }
  
  protected abstract <K, V> Map<K, V> createRestartableMap(long size, MemoryUnit unit, ByteBuffer id, RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence, RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean synchronous);

  protected static void destroyMap(Map<?, ?> map) {
    if (map instanceof OffHeapHashMap<?, ?>) {
      ((OffHeapHashMap<?, ?>) map).destroy();
    } else if (map instanceof AbstractConcurrentOffHeapMap<?, ?>) {
      ((AbstractConcurrentOffHeapMap<?, ?>) map).destroy();
    } else {
      throw new AssertionError();
    }

  }
}
