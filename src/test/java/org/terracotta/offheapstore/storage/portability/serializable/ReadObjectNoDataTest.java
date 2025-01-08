/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
 * Copyright IBM Corp. 2024, 2025
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

import java.io.ObjectStreamException;
import java.io.Serializable;
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
public class ReadObjectNoDataTest {
  
  @Test
  public void test() throws Exception {
    Portability<Serializable> p = new SerializablePortability();
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
