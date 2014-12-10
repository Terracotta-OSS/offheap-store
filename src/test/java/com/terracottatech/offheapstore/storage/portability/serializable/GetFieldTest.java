/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.offheapstore.storage.portability.serializable;

import com.terracottatech.offheapstore.storage.portability.Portability;

import java.io.IOException;
import java.io.ObjectInputStream;
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
public class GetFieldTest extends AbstractSerializablePortabilityTest {

  @Test
  public void testGetField() throws Exception {
    Portability<Serializable> p = createPortability();

    ClassLoader loaderA = createClassNameRewritingLoader(Foo_A.class);
    Serializable a = (Serializable) loaderA.loadClass(newClassName(Foo_A.class)).newInstance();
    ByteBuffer encodedA = p.encode(a);

    pushTccl(createClassNameRewritingLoader(Foo_B.class));
    try {
      p.decode(encodedA.duplicate());
    } finally {
      popTccl();
    }

    pushTccl(createClassNameRewritingLoader(Foo_C.class));
    try {
      p.decode(encodedA.duplicate());
    } finally {
      popTccl();
    }
  }

  public static class Foo_A implements Serializable {

    private static final long serialVersionUID = 0L;
    boolean z = true;
    byte b = 5;
    char c = '5';
    short s = 5;
    int i = 5;
    long j = 5;
    float f = 5.0f;
    double d = 5.0;
    String str = "5";
  }

  public static class Foo_B implements Serializable {

    private static final long serialVersionUID = 0L;
    int blargh;

    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
      ObjectInputStream.GetField fields = in.readFields();
      if (!fields.defaulted("blargh")) {
        throw new Error();
      }
      try {
        fields.defaulted("nonexistant");
        throw new Error();
      } catch (IllegalArgumentException ex) {
      }
      if ((fields.get("z", false) != true)
              || (fields.get("b", (byte) 0) != 5)
              || (fields.get("c", '0') != '5')
              || (fields.get("s", (short) 0) != 5)
              || (fields.get("i", 0) != 5)
              || (fields.get("j", 0l) != 5)
              || (fields.get("f", 0.0f) != 5.0f)
              || (fields.get("d", 0.0) != 5.0)
              || (!fields.get("str", null).equals("5"))) {
        throw new Error();
      }
    }
  }

  public static class Foo_C implements Serializable {

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
    Object extra;

    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
      ObjectInputStream.GetField fields = in.readFields();
      if ((fields.get("z", false) != true)
              || (fields.get("b", (byte) 0) != 5)
              || (fields.get("c", '0') != '5')
              || (fields.get("s", (short) 0) != 5)
              || (fields.get("i", 0) != 5)
              || (fields.get("j", 0l) != 5)
              || (fields.get("f", 0.0f) != 5.0f)
              || (fields.get("d", 0.0) != 5.0)
              || (!fields.get("str", null).equals("5"))) {
        throw new Error();
      }
    }
  }
}
