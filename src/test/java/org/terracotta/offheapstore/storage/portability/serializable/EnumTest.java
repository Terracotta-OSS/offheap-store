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

import java.io.Serializable;
import java.util.Arrays;

import org.hamcrest.core.IsSame;
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
public class EnumTest {

  @Test
  public void basicInstanceSerialization() {
    Portability<Serializable> p = new SerializablePortability();

    Assert.assertThat(p.decode(p.encode(People.Alice)), IsSame.sameInstance(People.Alice));
    Assert.assertThat(p.decode(p.encode(People.Bob)), IsSame.sameInstance(People.Bob));
    Assert.assertThat(p.decode(p.encode(People.Eve)), IsSame.sameInstance(People.Eve));
  }

  @Test
  public void classSerialization() {
    Portability<Serializable> p = new SerializablePortability();

    Assert.assertThat(p.decode(p.encode(Enum.class)), IsSame.sameInstance(Enum.class));
    Assert.assertThat(p.decode(p.encode(Dogs.Handel.getClass())), IsSame.sameInstance(Dogs.Handel.getClass()));
    Assert.assertThat(p.decode(p.encode(Dogs.Cassie.getClass())), IsSame.sameInstance(Dogs.Cassie.getClass()));
    Assert.assertThat(p.decode(p.encode(Dogs.Penny.getClass())), IsSame.sameInstance(Dogs.Penny.getClass()));
  }

  @Test
  public void shiftingInstanceSerialization() throws Exception {
    Portability<Serializable> p = new SerializablePortability();

    System.out.println(Arrays.toString(Foo_W.class.getDeclaredClasses()));
    System.out.println(Foo_W.c.getClass().getEnclosingClass());

    ClassLoader wLoader = createClassNameRewritingLoader(Foo_W.class);
    ClassLoader rLoader = createClassNameRewritingLoader(Foo_R.class);

    Class<?> wClass = wLoader.loadClass(newClassName(Foo_W.class));
    Class<?> rClass = rLoader.loadClass(newClassName(Foo_R.class));

    Object[] wInstances = wClass.getEnumConstants();
    Object[] rInstances = rClass.getEnumConstants();

    pushTccl(rLoader);
    try {
      for (int i = 0; i < wInstances.length; i++) {
        Assert.assertThat(p.decode(p.encode((Serializable) wInstances[i])), IsSame.sameInstance(rInstances[i]));
      }
    } finally {
      popTccl();
    }
  }

  public enum Foo_W { a, b, c { int i = 5; }, d { float f = 5.0f; } }
  public enum Foo_R { a, b { byte b = 3; }, c, d { double d = 6.0; } }
}

enum People { Alice, Bob, Eve }
enum Dogs { Handel, Cassie { int i = 0; }, Penny { double d = 3.0; } }
