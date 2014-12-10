/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.disk.persistent;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.MappedByteBuffer;
import java.util.concurrent.locks.Lock;

import com.terracottatech.offheapstore.AbstractOffHeapClockCache;
import com.terracottatech.offheapstore.disk.paging.MappedPageSource;
import com.terracottatech.offheapstore.util.FindbugsSuppressWarnings;

import java.nio.IntBuffer;

/**
 *
 * @author Chris Dennis
 */
public abstract class AbstractPersistentOffHeapCache<K, V> extends AbstractOffHeapClockCache<K, V> implements Persistent {

  private static final int MAGIC = 0x494c4c49;

  public AbstractPersistentOffHeapCache(MappedPageSource tableSource, PersistentStorageEngine<? super K, ? super V> storageEngine, boolean bootstrap) {
    super(tableSource, storageEngine, bootstrap);
  }

  public AbstractPersistentOffHeapCache(MappedPageSource tableSource, PersistentStorageEngine<? super K, ? super V> storageEngine, int tableSize, boolean bootstrap) {
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
          added(entry);
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
