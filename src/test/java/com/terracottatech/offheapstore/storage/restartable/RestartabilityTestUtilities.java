package com.terracottatech.offheapstore.storage.restartable;

import com.terracottatech.frs.object.RegisterableObjectManager;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public final class RestartabilityTestUtilities {
  
  private RestartabilityTestUtilities() {
    //static util class
  }
  
  public static RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> createObjectManager() {
    return new RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>();
  }
  
  public static File createTempDirectory(String prefix) throws IOException {
    File storage = File.createTempFile(prefix, "");
    Assert.assertTrue(storage.delete());
    Assert.assertTrue(storage.mkdirs());
    Assert.assertTrue(storage.isDirectory());
    storage.deleteOnExit();
    return storage;
  }

}
