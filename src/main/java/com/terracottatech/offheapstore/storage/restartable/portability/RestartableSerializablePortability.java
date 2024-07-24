/*
 * Copyright 2014-2023 Terracotta, Inc., a Software AG company.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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
package com.terracottatech.offheapstore.storage.restartable.portability;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.object.ObjectManagerEntry;
import com.terracottatech.frs.object.ObjectManagerSegment;
import com.terracottatech.frs.object.ObjectManagerStripe;
import com.terracottatech.frs.object.RestartableObject;
import com.terracottatech.frs.object.SimpleObjectManagerEntry;
import com.terracottatech.frs.object.heap.HeapValueSortedMap;
import org.terracotta.offheapstore.storage.portability.SerializablePortability;
import org.terracotta.offheapstore.util.ByteBufferInputStream;
import org.terracotta.offheapstore.util.FindbugsSuppressWarnings;
import static org.terracotta.offheapstore.util.Validation.shouldValidate;
import static org.terracotta.offheapstore.util.Validation.validate;

public class RestartableSerializablePortability<I> extends SerializablePortability implements ObjectManagerStripe<I, ByteBuffer, ByteBuffer>, ObjectManagerSegment<I, ByteBuffer, ByteBuffer>, RestartableObject<I, ByteBuffer, ByteBuffer> {

  private static final boolean VALIDATING = shouldValidate(RestartableSerializablePortability.class);
  
  private final I identifier;
  private final RestartStore<I, ByteBuffer, ByteBuffer> restartability;
  private final HeapValueSortedMap<Integer, Long> lsnMap = new HeapValueSortedMap<Integer, Long>();
  private final Collection<ObjectManagerSegment<I, ByteBuffer, ByteBuffer>> segments;
  private final boolean synchronous;

  private ObjectManagerEntry<I, ByteBuffer, ByteBuffer> compactingEntry;
  private long sizeInBytes = 0;
  
  public RestartableSerializablePortability(I identifier, RestartStore<I, ByteBuffer, ByteBuffer> restartability, boolean synchronous) {
    this.identifier = identifier;
    this.restartability = restartability;
    this.segments = Collections.<ObjectManagerSegment<I, ByteBuffer, ByteBuffer>>singleton(this);
    this.synchronous = synchronous;
  }

  public RestartableSerializablePortability(I identifier, RestartStore<I, ByteBuffer, ByteBuffer> restartability, boolean synchronous,
                                            ClassLoader classLoader) {
    super(classLoader);
    this.identifier = identifier;
    this.restartability = restartability;
    this.segments = Collections.<ObjectManagerSegment<I, ByteBuffer, ByteBuffer>>singleton(this);
    this.synchronous = synchronous;
  }

  @Override
  protected void addedMapping(Integer rep, ObjectStreamClass disconnected) {
    try {
      restartability.beginTransaction(synchronous).put(identifier, encodeInteger(rep), encodeObjectStreamClass(disconnected)).commit();
    } catch (TransactionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @FindbugsSuppressWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
  public Long getLowestLsn() {
    synchronized (lookup) {
      return lsnMap.firstValue();
    }
  }

  @Override
  @FindbugsSuppressWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
  public Long getLsn(ByteBuffer key) {
    synchronized (lookup) {
      return lsnMap.get(decodeInteger(key));
    }
  }

  @Override
  @FindbugsSuppressWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
  public Long getLsn(int hash, ByteBuffer key) {
    return getLsn(key);
  }

  @Override
  @FindbugsSuppressWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
  public void put(ByteBuffer key, ByteBuffer value, long lsn) {
    int representation = decodeInteger(key);
    if (lookup.containsKey(representation)) {
      synchronized (lookup) {
        sizeInBytes += (key.remaining() + value.remaining());
        lsnMap.put(representation, lsn);
      }
    } else {
      throw new AssertionError();
    }
  }

  @Override
  public void put(int hash, ByteBuffer key, ByteBuffer value, long lsn) {
    put(key, value, lsn);
  }

  @Override
  public void remove(ByteBuffer key) {
    throw new AssertionError();
  }

  @Override
  public void remove(int hash, ByteBuffer key) {
    remove(key);
  }

  @Override
  public void delete() {
    throw new AssertionError();
  }
  
  @Override
  @FindbugsSuppressWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
  public void replayPut(ByteBuffer binaryKey, ByteBuffer binaryValue, long lsn) {
    try {
      int representation = decodeInteger(binaryKey);
      ObjectStreamClass osc = decodeObjectStreamClass(binaryValue);
      SerializableDataKey key = new SerializableDataKey(osc, true);
      
      synchronized (lookup) {
        sizeInBytes += (binaryKey.remaining() + binaryValue.remaining());
        ObjectStreamClass oldOsc =
                (ObjectStreamClass) lookup.putIfAbsent(representation, osc);
        Integer oldRep = (Integer) lookup.putIfAbsent(key, representation);
        lsnMap.put(representation, lsn);
        if (oldRep != null && !oldRep.equals(representation)) {
          throw new AssertionError("Existing colliding class mapping detected");
        } else if (oldOsc != null && !oldOsc.getName().equals(osc.getName())) {
          throw new AssertionError("Existing colliding class mapping detected");
        } else {
          nextStreamIndex = Math.max(nextStreamIndex, representation + 1);
        }
      }
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public void replayPut(int hash, ByteBuffer key, ByteBuffer value, long lsn) {
    replayPut(key, value, lsn);
  }

  @Override
  @FindbugsSuppressWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
  public ObjectManagerEntry<I, ByteBuffer, ByteBuffer> acquireCompactionEntry(long ceilingLsn) {
    synchronized (lookup) {
      if (compactingEntry != null) {
        throw new AssertionError("Compaction Entry Already Acquired : " + compactingEntry);
      }
      Integer key = lsnMap.firstKey();
      if (key == null) {
        return null;
      }
      long lsn = lsnMap.get(key);
      if (lsn >= ceilingLsn) {
        return null;
      }
      ByteBuffer value = encodeObjectStreamClass((ObjectStreamClass) lookup.get(key));
      compactingEntry = new SimpleObjectManagerEntry<I, ByteBuffer, ByteBuffer>(identifier, encodeInteger(key), value, lsn);
      return compactingEntry;
    }
  }

  @Override
  public void updateLsn(int pojoHash, ObjectManagerEntry<I, ByteBuffer, ByteBuffer> entry, long newLsn) {
    updateLsn(entry, newLsn);
  }

  @Override
  @FindbugsSuppressWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
  public void updateLsn(ObjectManagerEntry<I, ByteBuffer, ByteBuffer> entry, long newLsn) {
    synchronized (lookup) {
      if (entry != compactingEntry) {
        throw new IllegalArgumentException(
                "Tried to update the LSN on an entry that was not first acquired.");
      }
      int i = decodeInteger(entry.getKey());
      Long previous = lsnMap.get(i);
      validate(!VALIDATING || previous == entry.getLsn());
      lsnMap.put(i, newLsn);
    }
  }

  @Override
  @FindbugsSuppressWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
  public void releaseCompactionEntry(ObjectManagerEntry<I, ByteBuffer, ByteBuffer> entry) {
    synchronized (lookup) {
      if (entry == null) throw new NullPointerException("Tried to release a null entry.");
      if (entry != compactingEntry) {
        throw new AssertionError("Released entry was not previously acquired.");
      }
      compactingEntry = null;
    }
  }

  @Override
  public Collection<ObjectManagerSegment<I, ByteBuffer, ByteBuffer>> getSegments() {
    return segments;
  }
  
  @Override
  @FindbugsSuppressWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
  public long size() {
    synchronized (lookup) {
      return lsnMap.size();
    }
  }
  
  private static ByteBuffer encodeInteger(int integer) {
    ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
    buffer.putInt(integer);
    return (ByteBuffer) buffer.flip();
  }
  
  private static int decodeInteger(ByteBuffer data) {
    return data.getInt(0);
  }
  
  private static ByteBuffer encodeObjectStreamClass(ObjectStreamClass osc) {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try {
      ObjectOutputStream oout = new ObjectOutputStream(bout);
      try {
        oout.writeObject(osc);
      } finally {
        oout.close();
      }
    } catch (IOException e) {
      throw new AssertionError(e);
    }
    return ByteBuffer.wrap(bout.toByteArray());
  }
  
  private static ObjectStreamClass decodeObjectStreamClass(ByteBuffer data) {
    ByteBufferInputStream bin = new ByteBufferInputStream(data.duplicate());
    try {
      ObjectInputStream oin = new ObjectInputStream(bin) {

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
          return null;
        }
      };
      try {
        return (ObjectStreamClass) oin.readObject();
      } finally {
        oin.close();
      }
    } catch (IOException e) {
      throw new AssertionError(e);
    } catch (ClassNotFoundException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public long sizeInBytes() {
    return sizeInBytes;
  }

  @Override
  public I getId() {
    return identifier;
  }

  @Override
  public ObjectManagerStripe<I, ByteBuffer, ByteBuffer> getObjectManagerStripe() {
    return this;
  }
}
