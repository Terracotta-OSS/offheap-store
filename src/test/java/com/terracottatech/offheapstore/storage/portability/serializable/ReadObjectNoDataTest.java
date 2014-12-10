/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.offheapstore.storage.portability.serializable;

import com.terracottatech.offheapstore.storage.portability.Portability;

import java.io.ObjectStreamException;
import java.io.Serializable;
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
public class ReadObjectNoDataTest extends AbstractSerializablePortabilityTest {
  
  @Test
  public void test() throws Exception {
    Portability<Serializable> p = createPortability();
    ClassLoader loaderW = createClassNameRewritingLoader(C_W.class, B_W.class);
    
    
    ByteBuffer b = p.encode((Serializable) loaderW.loadClass(newClassName(C_W.class)).newInstance());
    
    pushTccl(createClassNameRewritingLoader(C_R.class, B_R.class, A_R.class));
    try {
      Object out = p.decode(b);
      Assert.assertTrue(out.getClass().getField("called").getBoolean(out));
    } finally {
      popTccl();
    }
  }
  
  public static class B_W implements Serializable {
    private static final long serialVersionUID = 0L;
  }

  public static class C_W extends B_W {
    private static final long serialVersionUID = 0L;
  }
  
  
  public static class A_R implements Serializable {
    private static final long serialVersionUID = 0L;
    public boolean called = false;
    private void readObjectNoData() throws ObjectStreamException {
      called = true;
    }
  }

  public static class B_R extends A_R {
    private static final long serialVersionUID = 0L;
  }

  public static class C_R extends B_R {
    private static final long serialVersionUID = 0L;
  }
}
