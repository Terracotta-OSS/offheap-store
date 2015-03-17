/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.offheapstore.storage.portability.serializable;

import org.terracotta.offheapstore.storage.portability.Portability;

import java.io.Serializable;
import java.nio.ByteBuffer;

import org.junit.Test;

import static com.terracottatech.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.createClassNameRewritingLoader;
import static com.terracottatech.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.newClassName;
import static com.terracottatech.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.popTccl;
import static com.terracottatech.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.pushTccl;

/**
 *
 * @author cdennis
 */
public class AddedSuperClassTest extends AbstractSerializablePortabilityTest {
  
  @Test
  public void testAddedSuperClass() throws Exception {
    Portability<Serializable> p = createPortability();

    ClassLoader loaderA = createClassNameRewritingLoader(A_2.class, AddedSuperClass_Hidden.class);
    Serializable a = (Serializable) loaderA.loadClass(newClassName(A_2.class)).newInstance();
    ByteBuffer encodedA = p.encode(a);

    pushTccl(createClassNameRewritingLoader(A_1.class));
    try {
      p.decode(encodedA);
    } finally {
      popTccl();
    }
  }
  
  @Test
  public void testAddedSuperClassNotHidden() throws Exception {
    Portability<Serializable> p = createPortability();

    ClassLoader loaderA = createClassNameRewritingLoader(A_2.class, AddedSuperClass_Hidden.class);
    Serializable a = (Serializable) loaderA.loadClass(newClassName(A_2.class)).newInstance();
    ByteBuffer encodedA = p.encode(a);

    pushTccl(createClassNameRewritingLoader(A_1.class, AddedSuperClass_Hidden.class));
    try {
      p.decode(encodedA);
    } finally {
      popTccl();
    }
  }
  
  public static class AddedSuperClass_Hidden implements Serializable {
    int field;
  }
  
  public static class A_2 extends AddedSuperClass_Hidden  {
    private static final long serialVersionUID = 1L;
  }

  public static class A_1 implements Serializable {
    private static final long serialVersionUID = 1L;
  }

}
