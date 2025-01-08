/*
 * Copyright 2014-2023 Terracotta, Inc., a Software AG company.
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

package com.terracottatech.offheapstore.storage.portability.serializable;

import org.terracotta.offheapstore.storage.portability.Portability;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Date;

import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Test;

import static com.terracottatech.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.createClassNameRewritingLoader;
import static com.terracottatech.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.newClassName;
import static com.terracottatech.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.popTccl;
import static com.terracottatech.offheapstore.storage.portability.serializable.SerializablePortabilityTestUtilities.pushTccl;

public class SerializeAfterEvolutionTest extends AbstractSerializablePortabilityTest {

  @Test
  public void test() throws Exception {
    Portability<Serializable> p = createPortability();

    ClassLoader loaderA = createClassNameRewritingLoader(A_old.class);
    Serializable a = (Serializable) loaderA.loadClass(newClassName(A_old.class)).newInstance();
    ByteBuffer encodedA = p.encode(a);

    ClassLoader loaderB = createClassNameRewritingLoader(A_new.class);
    pushTccl(loaderB);
    try {
      Serializable outA = p.decode(encodedA);
      Assert.assertThat((Integer) outA.getClass().getField("integer").get(outA), Is.is(42));

      Serializable b = (Serializable) loaderB.loadClass(newClassName(A_new.class)).newInstance();
      Serializable outB = p.decode(p.encode(b));
      Assert.assertThat((Integer) outB.getClass().getField("integer").get(outB), Is.is(42));
    } finally {
      popTccl();
    }
  }

  public static class A_old implements Serializable {

    private static final long serialVersionUID = 1L;
    
    public Integer integer;

    public A_old() {
      integer = 42;
    }
  }

  public static class A_new implements Serializable {

    private static final long serialVersionUID = 1L;
    
    public Date date;
    public Integer integer;

    public A_new() {
      date = new Date(42L);
      integer = 42;
    }
  }
}
