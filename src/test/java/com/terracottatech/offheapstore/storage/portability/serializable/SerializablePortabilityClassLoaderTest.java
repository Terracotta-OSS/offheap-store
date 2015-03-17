package com.terracottatech.offheapstore.storage.portability.serializable;

import java.io.Serializable;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.storage.portability.SerializablePortability;

/**
 * @author teck
 */
public class SerializablePortabilityClassLoaderTest {

  private static ClassLoader newLoader() {
    return new URLClassLoader(((URLClassLoader) SerializablePortabilityClassLoaderTest.class.getClassLoader()).getURLs(), null);
  }

  @Test
  public void testThreadContextLoader() throws Exception {
    Portability<Serializable> portability = new SerializablePortability();

    ClassLoader loader = newLoader();
    ByteBuffer encoded = portability.encode((Serializable) loader.loadClass(Foo.class.getName()).newInstance());

    final ClassLoader original = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(loader);
      Assert.assertSame(loader, portability.decode(encoded).getClass().getClassLoader());
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Test
  public void testExplicitLoader() throws Exception {
    ClassLoader loader = newLoader();
    Portability<Serializable> portability = new SerializablePortability(loader);

    ByteBuffer encoded = portability.encode((Serializable) loader.loadClass(Foo.class.getName()).newInstance());

    final ClassLoader original = Thread.currentThread().getContextClassLoader();
    try {
      // setting TCCL doesn't matter here, but set it to make sure it doesn't get used
      Thread.currentThread().setContextClassLoader(newLoader());
      Assert.assertSame(loader, portability.decode(encoded).getClass().getClassLoader());
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @SuppressWarnings("serial")
  public static class Foo implements Serializable {
    //
  }

}
