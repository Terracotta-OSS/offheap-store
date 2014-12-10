/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.offheapstore.storage.portability.serializable;

import com.terracottatech.offheapstore.storage.portability.Portability;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import static com.terracottatech.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.createClassNameRewritingLoader;
import static com.terracottatech.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.newClassName;
import static com.terracottatech.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.popTccl;
import static com.terracottatech.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.pushTccl;

/**
 *
 * @author cdennis
 */
public class ArrayPackageScopeTest extends AbstractSerializablePortabilityTest {
  
  @Test
  public void testArrayPackageScope() throws Exception {
    Portability<Serializable> p = createPortability();

    ClassLoader loaderA = createClassNameRewritingLoader(Foo_A.class);
    
    Serializable a = (Serializable) Array.newInstance(loaderA.loadClass(newClassName(Foo_A.class)), 0);
    ByteBuffer encodedA = p.encode(a);

    pushTccl(createClassNameRewritingLoader(Foo_B.class));
    try {
      Serializable b = p.decode(encodedA);
      Assert.assertTrue(b.getClass().isArray());
    } finally {
      popTccl();
    }
  }
  
  public static class Foo_A implements java.io.Serializable {
    private static final long serialVersionUID = 0L;
  }

  static class Foo_B {
    private static final long serialVersionUID = 0L;
  }
}
