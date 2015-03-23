/* 
 * Copyright 2015 Terracotta, Inc., a Software AG company.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.terracotta.offheapstore.storage.portability.serializable;

import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.storage.portability.SerializablePortability;

import java.io.Serializable;

import org.junit.Test;

import static org.terracotta.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.createClassNameRewritingLoader;
import static org.terracotta.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.newClassName;
import static org.terracotta.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.popTccl;
import static org.terracotta.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.pushTccl;
import static org.junit.Assert.fail;

/**
 *
 * @author cdennis
 */
public class FieldTypeChangeTest {
  
  @Test
  public void fieldTypeChangeWithOkayObject() throws Exception {
    Portability<Serializable> p = new SerializablePortability();
    
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
    Portability<Serializable> p = new SerializablePortability();
    
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
