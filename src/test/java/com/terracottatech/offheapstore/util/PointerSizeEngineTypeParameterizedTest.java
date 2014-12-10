/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.offheapstore.util;

import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.storage.OffHeapBufferStorageEngine;
import com.terracottatech.offheapstore.storage.PointerSize;
import com.terracottatech.offheapstore.storage.portability.Portability;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 * @author cdennis
 */
@RunWith(ParallelParameterized.class)
public abstract class PointerSizeEngineTypeParameterizedTest {
  
  @ParallelParameterized.Parameters(name = "pointer-size={0}, type={1}")
  public static Collection<Object[]> data() {
    Collection<Object[]> combinations = new ArrayList<Object[]>();
    for (PointerSize width : PointerSize.values()) {
      combinations.add(new Object[] {width, StorageEngineType.REGULAR});
      combinations.add(new Object[] {width, StorageEngineType.COMPRESSING});
    }
    return combinations;
  }
  
  @Parameterized.Parameter(0)
  public volatile PointerSize pointerSize;
  
  @Parameterized.Parameter(1)
  public volatile StorageEngineType storageEngineType;
  
  public <K, V> Factory<? extends OffHeapBufferStorageEngine<K, V>> createFactory(PageSource source, int pageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean thief, boolean victim) {
    return storageEngineType.createFactory(pointerSize, source, pageSize, keyPortability, valuePortability, thief, victim);
  }
  
  public <K,V> OffHeapBufferStorageEngine<K, V> create(PageSource source, int pageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability) {
    return storageEngineType.create(pointerSize, source, pageSize, keyPortability, valuePortability);
  }
  
  public <K,V> OffHeapBufferStorageEngine<K, V> create(PageSource source, int pageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean thief, boolean victim) {
    return storageEngineType.create(pointerSize, source, pageSize, keyPortability, valuePortability, thief, victim);
  }
  
  public enum StorageEngineType {
    REGULAR {
      @Override
      public <K,V> OffHeapBufferStorageEngine<K, V> create(PointerSize width, PageSource source, int pageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability) {
        return new OffHeapBufferStorageEngine<K, V>(width, source, pageSize, keyPortability, valuePortability);
      }
      
      @Override
      public <K,V> OffHeapBufferStorageEngine<K, V> create(PointerSize width, PageSource source, int pageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean thief, boolean victim) {
        return new OffHeapBufferStorageEngine<K, V>(width, source, pageSize, keyPortability, valuePortability, thief, victim);
      }
      
      @Override
      public <K, V> Factory<? extends OffHeapBufferStorageEngine<K, V>> createFactory(final PointerSize width, final PageSource source, final int pageSize, final Portability<? super K> keyPortability, final Portability<? super V> valuePortability, final boolean thief, final boolean victim) {
        return OffHeapBufferStorageEngine.createFactory(width, source, pageSize, keyPortability, valuePortability, thief, victim);
      }
    },
    COMPRESSING{
      @Override
      public <K,V> OffHeapBufferStorageEngine<K, V> create(PointerSize width, PageSource source, int pageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability) {
        return new OffHeapBufferStorageEngine<K, V>(width, source, pageSize, keyPortability, valuePortability, 1.0f);
      }

      @Override
      public <K,V> OffHeapBufferStorageEngine<K, V> create(PointerSize width, PageSource source, int pageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean thief, boolean victim) {
        return new OffHeapBufferStorageEngine<K, V>(width, source, pageSize, keyPortability, valuePortability, thief, victim, 1.0f);
      }

      @Override
      public <K, V> Factory<? extends OffHeapBufferStorageEngine<K, V>> createFactory(final PointerSize width, final PageSource source, final int pageSize, final Portability<? super K> keyPortability, final Portability<? super V> valuePortability, final boolean thief, final boolean victim) {
        return OffHeapBufferStorageEngine.createFactory(width, source, pageSize, keyPortability, valuePortability, thief, victim, 1.0f);
      }
    };

    public abstract <K,V> OffHeapBufferStorageEngine<K, V> create(PointerSize width, PageSource source, int pageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability);
    public abstract <K,V> OffHeapBufferStorageEngine<K, V> create(PointerSize width, PageSource source, int pageSize, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean thief, boolean victim);
    public abstract <K, V> Factory<? extends OffHeapBufferStorageEngine<K, V>> createFactory(final PointerSize width, final PageSource source, final int pageSize, final Portability<? super K> keyPortability, final Portability<? super V> valuePortability, final boolean thief, final boolean victim);
}
}
