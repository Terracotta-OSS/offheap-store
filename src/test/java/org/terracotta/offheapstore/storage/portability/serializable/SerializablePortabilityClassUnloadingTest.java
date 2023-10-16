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

import org.terracotta.offheapstore.storage.portability.SerializablePortability;

import java.io.File;
import java.io.Serializable;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

//import java.io.File;
//import java.lang.management.ManagementFactory;
//import javax.management.ObjectName;

/**
 *
 * @author cdennis
 */
public class SerializablePortabilityClassUnloadingTest {

  private static final int PACKING_UNIT = 512 * 1024;

  public volatile WeakReference<Class<?>> classRef;
  public volatile Serializable specialObject;

  @Before
  public void createSpecialObject() throws Exception {
    ClassLoader duplicate = newLoader();

    @SuppressWarnings("unchecked")
    Class<? extends Serializable> special = (Class<? extends Serializable>) duplicate.loadClass(SpecialClass.class.getName());
    classRef = new WeakReference<>(special);

    specialObject = special.newInstance();
  }

  @Test
  public void testClassUnloadingAfterSerialization() throws Exception {
    SerializablePortability portability = new SerializablePortability();

    portability.encode(specialObject);

    specialObject = null;

    for (int i = 0; i < 10; i++) {
      if (classRef.get() == null) {
        return;
      } else {
        packHeap();
      }
    }
    throw new AssertionError();
  }

  @Test
  public void testClassUnloadingAfterSerializationAndDeserialization() throws Exception {
    Thread.currentThread().setContextClassLoader(specialObject.getClass().getClassLoader());
    try {
      SerializablePortability portability = new SerializablePortability();
      specialObject = portability.decode(portability.encode(specialObject));
      Assert.assertEquals(SpecialClass.class.getName(), specialObject.getClass().getName());
      Assert.assertNotSame(SpecialClass.class, specialObject.getClass());
    } finally {
      specialObject = null;
      Thread.currentThread().setContextClassLoader(null);
    }

    for (int i = 0; i < 10; i++) {
      if (classRef.get() == null) {
        return;
      } else {
        packHeap();
      }
    }
    throw new AssertionError();
  }

  private static void packHeap() {
    List<SoftReference<?>> packing = new ArrayList<>();
    ReferenceQueue<byte[]> queue = new ReferenceQueue<>();
    packing.add(new SoftReference<>(new byte[PACKING_UNIT], queue));
    while (queue.poll() == null) {
      packing.add(new SoftReference<>(new byte[PACKING_UNIT]));
    }
  }

  public static class SpecialClass implements Serializable {

    private static final long serialVersionUID = 1L;

    //empty impl
  }

  private static ClassLoader newLoader() {
    String pathSeparator = System.getProperty("path.separator");
    String[] classPathEntries = System.getProperty("java.class.path").split(pathSeparator);
    URL[] urls = Arrays.stream(classPathEntries).map(s -> {
      try {
        return new File(s).toURI().toURL();
      } catch (MalformedURLException e) {
        e.printStackTrace();
        return null;
      }
    }).toArray(URL[]::new);
    return new URLClassLoader(urls, null);
  }

//  private static void dumpHeap(String fileName, boolean live) {
//    try {
//      new File(fileName).delete();
//      ManagementFactory.getPlatformMBeanServer().invoke(ObjectName.getInstance("com.sun.management:type=HotSpotDiagnostic"), "dumpHeap", new Object[]{fileName, Boolean.valueOf(live)}, new String[]{"java.lang.String", "boolean"});
//    } catch (RuntimeException re) {
//      throw re;
//    } catch (Exception exp) {
//      throw new RuntimeException(exp);
//    }
//  }
}
