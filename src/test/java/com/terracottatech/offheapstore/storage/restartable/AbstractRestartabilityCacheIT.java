package com.terracottatech.offheapstore.storage.restartable;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.object.RegisterableObjectManager;
import com.terracottatech.offheapstore.storage.portability.ByteArrayPortability;
import com.terracottatech.offheapstore.storage.portability.Portability;
import com.terracottatech.offheapstore.storage.portability.StringPortability;
import org.hamcrest.collection.IsIn;
import org.hamcrest.core.IsEqual;
import org.hamcrest.number.OrderingComparison;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.terracottatech.offheapstore.util.MemoryUnit.KILOBYTES;

public abstract class AbstractRestartabilityCacheIT extends AbstractRestartabilityIT {

  @Override
  @Test
  public void testEvictionOnRecovery() throws Exception {
    File directory = RestartabilityTestUtilities.createTempDirectory(getClass().getSimpleName() + ".testEvictionOnRecovery_");
    ByteBuffer id = ByteBuffer.wrap("map".getBytes("US-ASCII"));
    
    Set<String> initialKeys;
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = createRestartStore(objectMgr, directory);
      persistence.startup().get();
      try {
        Portability<String> keyPortability = StringPortability.INSTANCE;
        Portability<byte[]> valuePortability = ByteArrayPortability.INSTANCE;
        Map<String, byte[]> map = createRestartableMap(300, KILOBYTES, id, persistence, objectMgr, keyPortability, valuePortability, true);
        try {
          byte[] payload = new byte[1024];

          int i = 0;
          do {
            map.put(Integer.toString(i++), payload);
          } while (map.size() == (i));

          initialKeys = new HashSet<String>(map.keySet());
          Assert.assertThat(initialKeys.size(), IsEqual.equalTo(map.size()));
        } finally {
          destroyMap(map);
        }
      } finally {
        persistence.shutdown();
      }
    }
    
    Set<String> recoveredKeys;
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = createRestartStore(objectMgr, directory);
      try {
        Portability<String> portability = StringPortability.INSTANCE;
        Map<String, String> map = createRestartableMap(150, KILOBYTES, id, persistence,
                                                       objectMgr, portability,
                                                       portability, true);
        try {

          persistence.startup().get();

          recoveredKeys = new HashSet<String>(map.keySet());
          Assert.assertThat(recoveredKeys.size(), IsEqual.equalTo(map.size()));
          Assert.assertThat(recoveredKeys.size(), OrderingComparison.lessThan(initialKeys.size()));
          for (String key : recoveredKeys) {
            Assert.assertThat(key, IsIn.isIn(initialKeys));
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
        Map<String, String> map = createRestartableMap(300, KILOBYTES, id, persistence, objectMgr, portability, portability, true);
        try {
          persistence.startup().get();

          Set<String> finalKeys = new HashSet<String>(map.keySet());
          Assert.assertThat(finalKeys.size(), IsEqual.equalTo(map.size()));
          try {
            Assert.assertThat(finalKeys, IsEqual.equalTo(recoveredKeys));
          } catch (AssertionError e) {
            Set<String> onlyInFinal = new HashSet<String>(finalKeys);
            onlyInFinal.removeAll(recoveredKeys);
            Set<String> onlyInRecovered = new HashSet<String>(recoveredKeys);
            onlyInRecovered.removeAll(finalKeys);
            System.err.println("Keys Only In Recovered Set : " + onlyInRecovered);
            System.err.println("Keys Only In Final Set     : " + onlyInFinal);
            throw e;
          }
        } finally {
          destroyMap(map);
        }
      } finally {
        persistence.shutdown();
      }
    }
  }
}
