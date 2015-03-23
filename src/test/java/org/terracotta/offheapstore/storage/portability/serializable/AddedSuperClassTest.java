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
import java.nio.ByteBuffer;

import org.junit.Test;

import static org.terracotta.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.createClassNameRewritingLoader;
import static org.terracotta.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.newClassName;
import static org.terracotta.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.popTccl;
import static org.terracotta.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.pushTccl;

/**
 *
 * @author cdennis
 */
public class AddedSuperClassTest {
  
  @Test
  public void testAddedSuperClass() throws Exception {
    Portability<Serializable> p = new SerializablePortability();

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
    Portability<Serializable> p = new SerializablePortability();

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
