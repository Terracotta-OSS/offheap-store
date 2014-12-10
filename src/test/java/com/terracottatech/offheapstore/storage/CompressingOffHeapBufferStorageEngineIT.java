/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.offheapstore.storage;

import com.terracottatech.offheapstore.OffHeapHashMap;
import com.terracottatech.offheapstore.buffersource.OffHeapBufferSource;
import com.terracottatech.offheapstore.exceptions.OversizeMappingException;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.paging.UnlimitedPageSource;
import com.terracottatech.offheapstore.paging.UpfrontAllocatingPageSource;
import com.terracottatech.offheapstore.storage.portability.Portability;
import com.terracottatech.offheapstore.storage.portability.SerializablePortability;
import com.terracottatech.offheapstore.util.MemoryUnit;
import com.terracottatech.offheapstore.util.PointerSizeParameterizedTest;

import java.io.Serializable;
import java.util.Arrays;

import org.junit.Test;

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsEqual.*;
import static org.hamcrest.number.OrderingComparison.*;
import static org.junit.Assert.assertThat;

/**
 *
 * @author cdennis
 */
public class CompressingOffHeapBufferStorageEngineIT extends PointerSizeParameterizedTest {

  @Test
  public void testCompressionWithExplicitHoles() {
    PageSource source = new UnlimitedPageSource(new OffHeapBufferSource());
    Portability<Serializable> portability = new SerializablePortability();
    
    OffHeapHashMap<Integer, String> regular = new OffHeapHashMap<Integer, String>(source, new OffHeapBufferStorageEngine<Integer, String>(getPointerSize(), source, MemoryUnit.KILOBYTES.toBytes(1), portability, portability));
    OffHeapHashMap<Integer, String> compressed = new OffHeapHashMap<Integer, String>(source, new OffHeapBufferStorageEngine<Integer, String>(getPointerSize(), source, MemoryUnit.KILOBYTES.toBytes(1), portability, portability, 1.0f));

    for (int i = 0; i < 8 * 1024; i++) {
      regular.put(i, "foobar");
      compressed.put(i, "foobar");
    }

    assertThat(compressed, equalTo(regular));
    assertThat(compressed.getDataAllocatedMemory(), is(regular.getDataAllocatedMemory()));
    
    for (int i = 1; i < 8 * 1024; i += 2) {
      regular.remove(i);
      compressed.remove(i);
    }

    assertThat(compressed, equalTo(regular));
    assertThat(compressed.getDataAllocatedMemory(), lessThan(regular.getDataAllocatedMemory()));
  }

  @Test
  public void testCompressionWithLargeTerminalValue() {
    PageSource source1 = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), MemoryUnit.KILOBYTES.toBytes(8), MemoryUnit.KILOBYTES.toBytes(8));
    PageSource source2 = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), MemoryUnit.KILOBYTES.toBytes(8), MemoryUnit.KILOBYTES.toBytes(8));
    Portability<Serializable> portability = new SerializablePortability();
    
    OffHeapHashMap<Integer, String> regular = new OffHeapHashMap<Integer, String>(source1, new OffHeapBufferStorageEngine<Integer, String>(getPointerSize(), source1, MemoryUnit.KILOBYTES.toBytes(1), portability, portability));
    OffHeapHashMap<Integer, String> compressed = new OffHeapHashMap<Integer, String>(source2, new OffHeapBufferStorageEngine<Integer, String>(getPointerSize(), source2, MemoryUnit.KILOBYTES.toBytes(1), portability, portability, 1.0f));

    for (int i = 0; ; i++) {
      try {
        regular.put(i, "foobar");
        compressed.put(i, "foobar");
      } catch (OversizeMappingException e) {
        char[] big = new char[1 * 1024];
        Arrays.fill(big, '!');
        String bigString = new String(big);
        for (int j = i; j >= 0; j--) {
          regular.remove(j);
          compressed.remove(j);
          try {
            regular.put(-1, bigString);
            compressed.put(-1, bigString);
            break;
          } catch (OversizeMappingException f) {
            //ignore
          }
        }
        break;
      }
    }

    assertThat(compressed, equalTo(regular));
    assertThat(compressed.getDataAllocatedMemory(), is(regular.getDataAllocatedMemory()));
    
    int limit = regular.size() - 1;
    for (int i = 1; i < limit; i += 2) {
      regular.remove(i);
      compressed.remove(i);
    }

    assertThat(compressed, equalTo(regular));
    //assertThat(compressed.getDataAllocatedMemory(), lessThan(regular.getDataAllocatedMemory()));
  }
}
