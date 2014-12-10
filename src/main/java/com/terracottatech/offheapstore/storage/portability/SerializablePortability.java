/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.storage.portability;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.terracottatech.offheapstore.util.ByteBufferInputStream;
import com.terracottatech.offheapstore.util.FindbugsSuppressWarnings;

/**
 * A trivially compressed Java serialization based portability.
 * <p>
 * Class descriptors in the resultant bytes are encoded as integers.  Mappings
 * between the integer representation and the {@link ObjectStreamClass}, and the
 * {@code Class} and the integer representation are stored in a single on-heap
 * map.
 *
 * @author Chris Dennis
 */
public class SerializablePortability implements Portability<Serializable> {

  protected int nextStreamIndex = 0;
  protected final ConcurrentMap<Object, Object> lookup = new ConcurrentHashMap<Object, Object>();
  private final ClassLoader loader;
  
  public SerializablePortability() {
    this(null);
  }
  
  public SerializablePortability(ClassLoader loader) {
    this.loader = loader;
  }
  
  @Override
  public ByteBuffer encode(Serializable object) {
    try {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      ObjectOutputStream oout = getObjectOutputStream(bout);
      try {
        oout.writeObject(object);
      } finally {
        oout.close();
      }
      return ByteBuffer.wrap(bout.toByteArray());
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public Serializable decode(ByteBuffer buffer) {
    try {
      ObjectInputStream oin = getObjectInputStream(new ByteBufferInputStream(buffer));
      try {
        return (Serializable) oin.readObject();
      } finally {
        oin.close();
      }
    } catch (ClassNotFoundException e) {
      throw new AssertionError(e);
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  public ObjectOutputStream getObjectOutputStream(OutputStream out) throws IOException {
    return new OOS(out);
  }

  public ObjectInputStream getObjectInputStream(InputStream input) throws IOException {
    return new OIS(input, loader);
  }

  @Override
  public boolean equals(Object value, ByteBuffer readBuffer) {
    return value.equals(decode(readBuffer));
  }

//  protected int getOrAddMapping(ObjectStreamClass desc) throws IOException {
//    SerializableDataKey probe = new SerializableDataKey(desc);
//    Integer rep = (Integer) lookup.get(probe);
//    if (rep == null) {
//      ObjectStreamClass disconnected = disconnect(desc);
//      SerializableDataKey key = new SerializableDataKey(disconnected);
//      rep = nextStreamIndex.getAndIncrement();
//
//      ObjectStreamClass existingOsc = (ObjectStreamClass) lookup.putIfAbsent(rep, disconnected);
//      if (existingOsc == null) {
//        Integer existingRep = (Integer) lookup.putIfAbsent(key, rep);
//        if (existingRep == null) {
//          return rep.intValue();
//        } else {
//          /*
//           * A racing thread established a mapping already.  We must clean up
//           * our half complete mapping.
//           */
//          lookup.remove(rep);
//          return existingRep.intValue();
//        }
//      } else {
//        //impossible as governed by AtomicInteger - excluding wrap-around == 2^32 types)
//        throw new AssertionError();
//      }
//    } else {
//      return rep.intValue();
//    }
//  }

  @FindbugsSuppressWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
  protected int getOrAddMapping(ObjectStreamClass desc) throws IOException {
    SerializableDataKey probe = new SerializableDataKey(desc, false);
    Integer rep = (Integer) lookup.get(probe);
    if (rep == null) {
      synchronized (lookup) {
        rep = (Integer) lookup.get(probe);
        if (rep == null) {
          ObjectStreamClass disconnected = disconnect(desc);
          SerializableDataKey key = new SerializableDataKey(disconnected, true);
          rep = nextStreamIndex++;
          ObjectStreamClass existingOsc = (ObjectStreamClass) lookup.putIfAbsent(rep, disconnected);
          if (existingOsc != null) {
            throw new AssertionError("Existing mapping for this index detected : " + rep + " => " + existingOsc.getName());
          }
          Integer existingRep = (Integer) lookup.putIfAbsent(key, rep);
          if (existingRep != null) {
            throw new AssertionError("Existing mapping to this type detected : " + existingRep + " => " + disconnected.getName());
          }
          addedMapping(rep, disconnected);
        }
      }
    }
    return rep.intValue();
  }

  protected void addedMapping(Integer rep, ObjectStreamClass disconnected) {
    //no-op
  }

  class OOS extends ObjectOutputStream {

    public OOS(OutputStream out) throws IOException {
      super(out);
    }
    
    @Override
    protected void writeClassDescriptor(final ObjectStreamClass desc) throws IOException {
      writeInt(getOrAddMapping(desc));
    }
  }

  class OIS extends ObjectInputStream {

    private final ClassLoader loader;

    public OIS(InputStream in, ClassLoader loader) throws IOException {
      super(in);
      this.loader = loader;
    }
    
    @Override
    protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException {
      return (ObjectStreamClass) lookup.get(readInt());
    }
    
    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
      try {
        final ClassLoader cl = loader == null ? Thread.currentThread().getContextClassLoader() : loader;
        if (cl == null) {
          return super.resolveClass(desc);
        } else {
          try {
            return Class.forName(desc.getName(), false, cl);
          } catch (ClassNotFoundException e) {
            return super.resolveClass(desc);
          }
        }
      } catch (SecurityException ex) {
        return super.resolveClass(desc);
      }
    }
  }
  
  protected static class SerializableDataKey {
    private final ObjectStreamClass osc;
    private final int hashCode;

    private transient WeakReference<Class<?>> klazz;

    public SerializableDataKey(ObjectStreamClass desc, boolean store) throws IOException {
      Class<?> forClass = desc.forClass();
      if (forClass != null) {
        if (store) {
          throw new AssertionError("Must not store ObjectStreamClass instances with strong references to classes");
        } else if (ObjectStreamClass.lookup(forClass) == desc) {
          this.klazz = new WeakReference<Class<?>>(forClass);
        }
      }
      this.hashCode = (3 * desc.getName().hashCode()) ^ (7 * (int) (desc.getSerialVersionUID() >>> 32))
          ^ (11 * (int) desc.getSerialVersionUID());
      this.osc = desc;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof SerializableDataKey) {
        return SerializablePortability.equals(this, (SerializableDataKey) o);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    public Class<?> forClass() {
      if (klazz == null) {
        return null;
      } else {
        return klazz.get();
      }
    }

    public void setClass(Class<?> clazz) {
      klazz = new WeakReference<Class<?>>(clazz);
    }

    public ObjectStreamClass getObjectStreamClass() {
      return osc;
    }
  }

  private static boolean equals(SerializableDataKey k1, SerializableDataKey k2) {
    Class<?> k1Clazz = k1.forClass();
    Class<?> k2Clazz = k2.forClass();
    if (k1Clazz != null && k2Clazz != null) {
      return k1Clazz == k2Clazz;
    } else if (SerializablePortability.equals(k1.getObjectStreamClass(), k2.getObjectStreamClass())) {
      if (k1Clazz != null) {
        k2.setClass(k1Clazz);
      } else if (k2Clazz != null) {
        k1.setClass(k2Clazz);
      }
      return true;
    } else {
      return false;
    }
  }

  private static boolean equals(ObjectStreamClass osc1, ObjectStreamClass osc2) {
    if (osc1 == osc2) {
      return true;
    } else if (osc1.getName().equals(osc2.getName()) && osc1.getSerialVersionUID() == osc2.getSerialVersionUID() && osc1.getFields().length == osc2.getFields().length) {
      try {
        return Arrays.equals(getSerializedForm(osc1), getSerializedForm(osc2));
      } catch (IOException e) {
        throw new AssertionError(e);
      }
    } else {
      return false;
    }
  }
  
  protected static ObjectStreamClass disconnect(ObjectStreamClass desc) {
    try {
      ObjectInputStream oin = new ObjectInputStream(new ByteArrayInputStream(getSerializedForm(desc))) {

        @Override
        protected Class<?> resolveClass(ObjectStreamClass osc) throws IOException, ClassNotFoundException {
          //Our stored OSC instances should not reference classes - doing so could cause perm-gen leaks
          return null;
        }
      };

      return (ObjectStreamClass) oin.readObject();
    } catch (ClassNotFoundException e) {
      throw new AssertionError(e);
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  private static byte[] getSerializedForm(ObjectStreamClass desc) throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try {
      ObjectOutputStream oout = new ObjectOutputStream(bout);
      try {
        oout.writeObject(desc);
      } finally {
        oout.close();
      }
    } finally {
      bout.close();
    }
    return bout.toByteArray();
  }
}
