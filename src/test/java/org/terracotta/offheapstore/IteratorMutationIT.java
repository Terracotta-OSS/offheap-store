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
package org.terracotta.offheapstore;

import org.terracotta.offheapstore.ReadWriteLockedOffHeapHashMap;
import org.terracotta.offheapstore.OffHeapHashMap;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.storage.HalfStorageEngine;
import org.terracotta.offheapstore.storage.OffHeapBufferStorageEngine;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.SplitStorageEngine;
import org.terracotta.offheapstore.storage.StorageEngine;
import org.terracotta.offheapstore.storage.StorageEngine.Owner;
import org.terracotta.offheapstore.storage.portability.SerializablePortability;

public class IteratorMutationIT {

  @Test
  public void testConcurrentUpdateDoesntMiss() {
    PageSource source = new UnlimitedPageSource(new HeapBufferSource());
    ReadWriteLockedOffHeapHashMap<Value, Value> map = new ReadWriteLockedOffHeapHashMap<Value, Value>(source, ValueStorage.INSTANCE, 8);

    map.put(new Value(0), new Value(0));
    map.put(new Value(1), new Value(1));
    map.put(new Value(2), new Value(2));

    assertThat(map.getAtTableOffset(2 * OffHeapHashMap.ENTRY_SIZE), is(new Value(0)));
    assertThat(map.getAtTableOffset(3 * OffHeapHashMap.ENTRY_SIZE), is(new Value(1)));
    assertThat(map.getAtTableOffset(4 * OffHeapHashMap.ENTRY_SIZE), is(new Value(2)));
    
    Iterator<Value> keyIterator = map.keySet().iterator();
    
    assertTrue(keyIterator.hasNext());
    assertThat(keyIterator.next(), is(new Value(0)));
    
    map.remove(new Value(0));
    map.put(new Value(2), new Value(2));
    
    assertTrue(keyIterator.hasNext());
    assertThat(keyIterator.next(), is(new Value(1)));
    assertTrue(keyIterator.hasNext());
    assertThat(keyIterator.next(), is(new Value(2)));
  }

  @Test
  public void testConcurrentUpdateDoesntDuplicate() {
    PageSource source = new UnlimitedPageSource(new HeapBufferSource());
    ReadWriteLockedOffHeapHashMap<Value, Value> map = new ReadWriteLockedOffHeapHashMap<Value, Value>(source, ValueStorage.INSTANCE, 4);

    map.put(new Value(0), new Value(0));
    map.put(new Value(1), new Value(1));
    map.put(new Value(2), new Value(2));

    assertThat(map.getAtTableOffset(0 * OffHeapHashMap.ENTRY_SIZE), is(new Value(2)));
    assertThat(map.getAtTableOffset(2 * OffHeapHashMap.ENTRY_SIZE), is(new Value(0)));
    assertThat(map.getAtTableOffset(3 * OffHeapHashMap.ENTRY_SIZE), is(new Value(1)));
    
    Iterator<Value> keyIterator = map.keySet().iterator();
    
    assertTrue(keyIterator.hasNext());
    assertThat(keyIterator.next(), is(new Value(2)));

    
    map.remove(new Value(1));
    map.put(new Value(2), new Value(2));
    
    assertTrue(keyIterator.hasNext());
    assertThat(keyIterator.next(), is(new Value(0)));
    try {
      assertFalse(keyIterator.hasNext());
    } catch (AssertionError e) {
      throw new AssertionError("Expected no next value, seeing : " + keyIterator.next());
    }
  }
  
  @Test
  public void testConcurrentResizeAndUpdate() {
    PageSource source = new UnlimitedPageSource(new HeapBufferSource());
    ReadWriteLockedOffHeapHashMap<Value, Value> map = new ReadWriteLockedOffHeapHashMap<Value, Value>(source, new OffHeapBufferStorageEngine<Value, Value>(PointerSize.INT, source, 1024, new SerializablePortability(), new SerializablePortability()), 2);

    map.put(new Value(0), new Value(0));
    map.put(new Value(1), new Value(1));

    assertThat(map.getAtTableOffset(0 * OffHeapHashMap.ENTRY_SIZE), is(new Value(0)));
    assertThat(map.getAtTableOffset(1 * OffHeapHashMap.ENTRY_SIZE), is(new Value(1)));
    
    Iterator<Value> keyIterator = map.keySet().iterator();
    
    map.put(new Value(2), new Value(2));
    map.put(new Value(0), new Value(0));
    
    assertThat(map.getAtTableOffset(0 * OffHeapHashMap.ENTRY_SIZE), is(new Value(2)));
    assertThat(map.getAtTableOffset(2 * OffHeapHashMap.ENTRY_SIZE), is(new Value(0)));
    assertThat(map.getAtTableOffset(3 * OffHeapHashMap.ENTRY_SIZE), is(new Value(1)));
    
    Collection<Value> iteratorKeys = new ArrayList<Value>();
    while (keyIterator.hasNext()) {
      iteratorKeys.add(keyIterator.next());
    }
    assertThat(iteratorKeys, new TypeSafeMatcher<Collection<?>>() {

      @Override
      public void describeTo(Description description) {
        description.appendText("a collection with no duplicates");
      }

      @Override
      public boolean matchesSafely(Collection<?> objects) {
        return new HashSet<Object>(objects).size() == objects.size();
      }});
  }
  
  static class Value implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private final int value;
    
    public Value(int value) {
      this.value = value;
    }
    
    @Override
    public boolean equals(Object o) {
      if (o instanceof Value) {
        return value == ((Value) o).value;
      } else {
        return false;
      }
    }
    
    @Override
    public int hashCode() {
      return 0;
    }
    
    @Override
    public String toString() {
      return "Value(" + value + ")";
    }
  }
  
  static class ValueStorage implements HalfStorageEngine<Value> {

    private static final ValueStorage HALF_INSTANCE = new ValueStorage();
    public static final StorageEngine<Value, Value> INSTANCE = new SplitStorageEngine<IteratorMutationIT.Value, IteratorMutationIT.Value>(HALF_INSTANCE, HALF_INSTANCE);
        
    @Override
    public Integer write(Value object, int hash) {
      return object.value;
    }

    @Override
    public void free(int encoding) {
      //no-op
    }

    @Override
    public Value read(int encoding) {
      return new Value(encoding);
    }

    @Override
    public boolean equals(Object object, int encoding) {
      if (object instanceof Value) {
        return ((Value) object).value == encoding;
      } else {
        return false;
      }
    }

    @Override
    public void clear() {
      //no-op
    }

    @Override
    public long getAllocatedMemory() {
      return 0L;
    }

    @Override
    public long getOccupiedMemory() {
      return 0L;
    }

    @Override
    public long getVitalMemory() {
      return 0L;
    }

    @Override
    public long getDataSize() {
      return 0L;
    }

    @Override
    public void invalidateCache() {
      //no-op
    }

    @Override
    public void bind(Owner owner, long mask) {
      //no-op
    }

    @Override
    public void destroy() {
      //no-op
    }

    @Override
    public boolean shrink() {
      return false;
    }
  }
}
