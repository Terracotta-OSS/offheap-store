/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.offheapstore.storage.portability.serializable;

import org.terracotta.offheapstore.storage.portability.Portability;

import java.io.Serializable;

import org.junit.Test;

import static com.terracottatech.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.createClassNameRewritingLoader;
import static com.terracottatech.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.newClassName;
import static com.terracottatech.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.popTccl;
import static com.terracottatech.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.pushTccl;
import static org.junit.Assert.fail;

/**
 *
 * @author cdennis
 */
public class FieldTypeChangeTest extends AbstractSerializablePortabilityTest {
  
  @Test
  public void fieldTypeChangeWithOkayObject() throws Exception {
    Portability<Serializable> p = createPortability();
    
    ClassLoader loaderW = createClassNameRewritingLoader(Foo_W.class);
    Serializable a = (Serializable) loaderW.loadClass(newClassName(Foo_W.class)).getConstructor(Object.class).newInstance("foo");
    
    pushTccl(createClassNameRewritingLoader(Foo_R.class));
    try {
      p.decode(p.encode(a));
    } finally {
      popTccl();
    }
  }
  
  @Test
  public void fieldTypeChangeWithIncompatibleObject() throws Exception {
    Portability<Serializable> p = createPortability();
    
    ClassLoader loaderW = createClassNameRewritingLoader(Foo_W.class);
    
    Serializable a = (Serializable) loaderW.loadClass(newClassName(Foo_W.class)).getConstructor(Object.class).newInstance(Integer.valueOf(42));
    
    pushTccl(createClassNameRewritingLoader(Foo_R.class));
    try {
      p.decode(p.encode(a));
      fail("Expected ClassCastException");
    } catch (ClassCastException e) {
      //expected
    } finally {
      popTccl();
    }
  }
  
  public static class Foo_W implements Serializable {
    private static final long serialVersionUID = 0L;
    Object obj;

    public Foo_W(Object obj) {
        this.obj = obj;
    }
  }
  
  public static class Foo_R implements Serializable {
    private static final long serialVersionUID = 0L;
    String obj;
  }
}
