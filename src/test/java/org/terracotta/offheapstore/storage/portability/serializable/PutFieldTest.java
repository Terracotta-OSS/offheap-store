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

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamField;
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
public class PutFieldTest {

  @Test
  public void testWithAllPrimitivesAndString() throws Exception {
    Portability<Serializable> p = new SerializablePortability();

    ClassLoader loaderA = createClassNameRewritingLoader(Foo_A.class);
    Serializable a = (Serializable) loaderA.loadClass(newClassName(Foo_A.class)).newInstance();
    ByteBuffer encodedA = p.encode(a);

    pushTccl(Foo.class.getClassLoader());
    try {
      Foo foo = (Foo) p.decode(encodedA);

      Assert.assertEquals(true, foo.z);
      Assert.assertEquals(5, foo.b);
      Assert.assertEquals('5', foo.c);
      Assert.assertEquals(5, foo.s);
      Assert.assertEquals(5, foo.i);
      Assert.assertEquals(5, foo.j);
      Assert.assertEquals(5, foo.f, 0.0f);
      Assert.assertEquals(5, foo.d, 0.0);
      Assert.assertEquals("5", foo.str);
    } finally {
      popTccl();
    }
  }

  @Test
  public void testWithTwoStrings() throws Exception {
    Portability<Serializable> p = new SerializablePortability();

    ClassLoader loaderA = createClassNameRewritingLoader(Bar_A.class);
    Serializable a = (Serializable) loaderA.loadClass(newClassName(Bar_A.class)).newInstance();
    ByteBuffer encodedA = p.encode(a);

    pushTccl(Bar.class.getClassLoader());
    try {
      Bar bar = (Bar) p.decode(encodedA);

      Assert.assertEquals("qwerty", bar.s1);
      Assert.assertEquals("asdfg", bar.s2);
    } finally {
      popTccl();
    }
  }

  public static class Foo_A implements Serializable {

    private static final long serialVersionUID = 0L;
    private static final ObjectStreamField[] serialPersistentFields = {
      new ObjectStreamField("z", boolean.class),
      new ObjectStreamField("b", byte.class),
      new ObjectStreamField("c", char.class),
      new ObjectStreamField("s", short.class),
      new ObjectStreamField("i", int.class),
      new ObjectStreamField("j", long.class),
      new ObjectStreamField("f", float.class),
      new ObjectStreamField("d", double.class),
      new ObjectStreamField("str", String.class),};

    private void writeObject(ObjectOutputStream out) throws IOException {
      ObjectOutputStream.PutField fields = out.putFields();
      fields.put("z", true);
      fields.put("b", (byte) 5);
      fields.put("c", '5');
      fields.put("s", (short) 5);
      fields.put("i", 5);
      fields.put("j", 5L);
      fields.put("f", 5.0f);
      fields.put("d", 5.0);
      fields.put("str", "5");
      out.writeFields();
    }
  }

  public static class Foo implements Serializable {

    private static final long serialVersionUID = 0L;
    boolean z;
    byte b;
    char c;
    short s;
    int i;
    long j;
    float f;
    double d;
    String str;
  }

  public static class Bar_A implements Serializable {

    private static final long serialVersionUID = 0L;
    private static final ObjectStreamField[] serialPersistentFields = {
      new ObjectStreamField("s1", String.class),
      new ObjectStreamField("s2", String.class)
    };

    private void writeObject(ObjectOutputStream out) throws IOException {
      ObjectOutputStream.PutField fields = out.putFields();
      fields.put("s1", "qwerty");
      fields.put("s2", "asdfg");
      out.writeFields();
    }
  }

  public static class Bar implements Serializable {

    private static final long serialVersionUID = 0L;
    String s1, s2;
  }
}
