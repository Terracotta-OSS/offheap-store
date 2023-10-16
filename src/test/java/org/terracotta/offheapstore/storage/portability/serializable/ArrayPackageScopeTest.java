/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
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
import java.lang.reflect.Array;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import static org.terracotta.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.createClassNameRewritingLoader;
import static org.terracotta.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.newClassName;
import static org.terracotta.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.popTccl;
import static org.terracotta.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.pushTccl;

/**
 *
 * @author cdennis
 */
public class ArrayPackageScopeTest {
  
  @Test
  public void testArrayPackageScope() throws Exception {
    Portability<Serializable> p = new SerializablePortability();

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
