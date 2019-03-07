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
package org.terracotta.offheapstore.disk.persistent;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.MappedByteBuffer;
import java.util.concurrent.locks.Lock;

import org.terracotta.offheapstore.AbstractLockedOffHeapHashMap;
import org.terracotta.offheapstore.disk.paging.MappedPageSource;
import org.terracotta.offheapstore.util.FindbugsSuppressWarnings;

import java.nio.IntBuffer;

/**
 *
 * @author Chris Dennis
 */
public abstract class AbstractPersistentLockedOffHeapHashMap<K, V> extends AbstractLockedOffHeapHashMap<K, V> implements Persistent {

  private static final int MAGIC = 0x48455257;

  public AbstractPersistentLockedOffHeapHashMap(MappedPageSource tableSource, PersistentStorageEngine<? super K, ? super V> storageEngine, boolean bootstrap) {
    super(tableSource, storageEngine, bootstrap);
  }

  public AbstractPersistentLockedOffHeapHashMap(MappedPageSource tableSource, PersistentStorageEngine<? super K, ? super V> storageEngine, int tableSize, boolean bootstrap) {
    super(tableSource, storageEngine, tableSize, bootstrap);
  }

  @Override
  public void flush() throws IOException {
    Lock l = writeLock();
    l.lock();
    try {
      ((MappedByteBuffer) hashTablePage.asByteBuffer()).force();
      ((Persistent) storageEngine).flush();
    } finally {
      l.unlock();
    }
  }

  @Override
  public void close() throws IOException {
    Lock l = writeLock();
    l.lock();
    try {
      ((MappedPageSource) tableSource).close();
      ((Persistent) storageEngine).close();
    } finally {
      l.unlock();
    }
  }

  @Override
  public void persist(ObjectOutput output) throws IOException {
    Lock l = writeLock();
    l.lock();
    try {
      output.writeInt(MAGIC);
      output.writeLong(((MappedPageSource) tableSource).getAddress(hashTablePage));
      output.writeInt(hashTablePage.size());
      output.writeInt(reprobeLimit);
      ((Persistent) storageEngine).persist(output);
    } finally {
      l.unlock();
    }
  }

  @Override
  @FindbugsSuppressWarnings("VO_VOLATILE_INCREMENT")
  public void bootstrap(ObjectInput input) throws IOException {
    Lock l = writeLock();
    l.lock();
    try {
      if (hashtable != null) {
        throw new IllegalStateException();
      }

      if (input.readInt() != MAGIC) {
        throw new IOException("Wrong magic number");
      }
      long tableAddress = input.readLong();
      long tableCapacity = input.readInt();

      hashTablePage = ((MappedPageSource) tableSource).claimPage(tableAddress, tableCapacity);
      hashtable = hashTablePage.asIntBuffer();
      reprobeLimit = input.readInt();

      //need to scan the incoming table
      for (hashtable.clear(); hashtable.hasRemaining(); hashtable.position(hashtable.position() + ENTRY_SIZE)) {
        IntBuffer entry = (IntBuffer) hashtable.slice().limit(ENTRY_SIZE);

        if (isPresent(entry)) {
          size++;
          added(hashtable.position(), entry);
        } else if (isRemoved(entry)) {
          removedSlots++;
        }
      }

      ((Persistent) storageEngine).bootstrap(input);
    } finally {
      l.unlock();
    }
  }
}
