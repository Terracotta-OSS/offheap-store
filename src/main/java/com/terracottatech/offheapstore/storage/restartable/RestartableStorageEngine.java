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
package com.terracottatech.offheapstore.storage.restartable;

import static org.terracotta.offheapstore.util.Validation.shouldValidate;
import static org.terracotta.offheapstore.util.Validation.validate;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.object.ObjectManagerEntry;
import com.terracottatech.frs.object.ObjectManagerSegment;
import com.terracottatech.frs.object.SimpleObjectManagerEntry;
import org.terracotta.offheapstore.Metadata;
import org.terracotta.offheapstore.disk.storage.FileBackedStorageEngine;
import org.terracotta.offheapstore.storage.BinaryStorageEngine;
import org.terracotta.offheapstore.storage.StorageEngine;
import org.terracotta.offheapstore.storage.StorageEngine.Owner;
import org.terracotta.offheapstore.storage.listener.AbstractListenableStorageEngine;
import org.terracotta.offheapstore.util.Factory;

public class RestartableStorageEngine<T extends StorageEngine<K, LinkedNode<V>> & BinaryStorageEngine, I, K, V> extends AbstractListenableStorageEngine<K, V> implements StorageEngine<K, V>, BinaryStorageEngine, ObjectManagerSegment<I, ByteBuffer, ByteBuffer>, Owner {

  private static final boolean VALIDATING = shouldValidate(RestartableStorageEngine.class);
  
  public static final long NULL_ENCODING = Long.MIN_VALUE;
  
  public static <T extends StorageEngine<K, LinkedNode<V>> & BinaryStorageEngine, I, K, V> Factory<RestartableStorageEngine<T, I, K, V>> createFactory(final I identifier, final RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, final Factory<T> delegateFactory, final boolean synchronous) {
    return new Factory<RestartableStorageEngine<T,I,K,V>>() {

      @Override
      public RestartableStorageEngine<T, I, K, V> newInstance() {
        return new RestartableStorageEngine<T, I, K, V>(identifier, transactionSource, delegateFactory.newInstance(), synchronous);
      }
    };
  }

  protected final T delegateStorageEngine;
  protected final I identifier;
  protected final RestartStore<I, ByteBuffer, ByteBuffer> transactionSource;
  protected final boolean synchronous;
  
  protected volatile Owner owner;

  private long first = NULL_ENCODING;
  private long last = NULL_ENCODING;
  private ObjectManagerEntry<I, ByteBuffer, ByteBuffer> compactingEntry;

  public RestartableStorageEngine(I identifier, RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, T storageEngine, boolean synchronous) {
    this.identifier = identifier;
    this.transactionSource = transactionSource;
    this.delegateStorageEngine = storageEngine;
    this.synchronous = synchronous;
  }
  
  // Called under segment lock
  @Override
  public Long writeMapping(final K key, V value, int hash, int metadata) {
    Long result = delegateStorageEngine.writeMapping(key, new DetachedLinkedNode<V>(value), hash, metadata);
    if (result != null && hasListeners()) {
      fireWritten(key, value, readBinaryKey(result), readBinaryValue(result), hash, metadata, result);
    }
    return result;
  }

  // Called under segment lock
  @Override
  public void attachedMapping(long encoding, int hash, int metadata) {
    restartabilityPut(encoding, hash, metadata);
  }

  protected void restartabilityPut(long encoding, int pojoHash, int metadata) {
    ByteBuffer frsBinaryKey = encodeKey(readBinaryKey(encoding), pojoHash);
    ByteBuffer frsBinaryValue = encodeValue(readBinaryValue(encoding), encoding, metadata);
    try {
      transactionSource.beginTransaction(synchronous).put(identifier, frsBinaryKey, frsBinaryValue).commit();
    } catch (TransactionException e) {
      throw new RuntimeException(e);
    }
  }
  
  // Called under segment lock
  @Override
  public void freeMapping(long encoding, int hash, boolean removal) {
    LinkedNode<V> node = delegateStorageEngine.readValue(encoding);
    unlinkNode(node, encoding);
    node.flush();
    if (removal) {
      restartabilityRemove(encoding);
    }
    if (hasListeners()) {
      ByteBuffer rawKey = readBinaryKey(encoding);
      
      delegateStorageEngine.freeMapping(encoding, hash, removal);
      validChain();
      fireFreed(encoding, hash, rawKey, removal);
    }
    else {
      delegateStorageEngine.freeMapping(encoding, hash, removal);
      validChain();
    }
  }

  private void restartabilityRemove(long encoding) {
    ByteBuffer frsBinaryKey = encodeKey(readBinaryKey(encoding), readKeyHash(encoding));
    try {
      transactionSource.beginTransaction(synchronous).remove(identifier, frsBinaryKey).commit();
    } catch (TransactionException e) {
      throw new RuntimeException(e);
    }
  }
  
  // Called under segment lock
  @Override
  public V readValue(long encoding) {
    return delegateStorageEngine.readValue(encoding).getValue();
  }

  // Called under segment lock
  @Override
  public boolean equalsValue(Object value, long encoding) {
    /*
     * In order to avoid having to wrap this value in a temporary dummy node object
     * the LinkedNodePortability equals methods will short-circuit this comparison through for us.
     */
    return delegateStorageEngine.equalsValue(value, encoding);
  }

  // Called under segment lock
  @Override
  public K readKey(long encoding, int hashCode) {
    return delegateStorageEngine.readKey(encoding, hashCode);
  }

  // Called under segment lock
  @Override
  public boolean equalsKey(Object key, long encoding) {
    return delegateStorageEngine.equalsKey(key, encoding);
  }

  // Called under segment lock
  public boolean equalsBinaryKey(ByteBuffer offheapBinaryKey, long encoding) {
    return delegateStorageEngine.equalsBinaryKey(offheapBinaryKey, encoding);
  }
  
  // Called under segment lock  
  @Override
  public void clear() {
    first = NULL_ENCODING;
    last = NULL_ENCODING;
    restartabilityDelete();
    delegateStorageEngine.clear();
    validChain();
    fireCleared();
  }

  private void restartabilityDelete() {
    try {
      transactionSource.beginTransaction(synchronous).delete(identifier).commit();
    } catch (TransactionException e) {
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public long getAllocatedMemory() {
    return delegateStorageEngine.getAllocatedMemory();
  }

  @Override
  public long getOccupiedMemory() {
    return delegateStorageEngine.getOccupiedMemory();
  }

  @Override
  public long getVitalMemory() {
    return delegateStorageEngine.getVitalMemory();
  }

  @Override
  public long getDataSize() {
    //TODO This is an overestimate.
    return getOccupiedMemory();
  }

  @Override
  public void invalidateCache() {
    delegateStorageEngine.invalidateCache();
  }

  @Override
  public void bind(Owner owner) {
    this.owner = owner;
    delegateStorageEngine.bind(this);
  }

  @Override
  public void destroy() {
    delegateStorageEngine.destroy();
  }

  @Override
  public boolean shrink() {
    return delegateStorageEngine.shrink();
  }

  protected final void assignLsn(long encoding, long lsn) {
    LinkedNode<V> node = delegateStorageEngine.readValue(encoding);
    unlinkNode(node, encoding);
    linkNodeExpectingLast(node, encoding, lsn);
    node.setLsn(lsn);
    node.flush();
    validChain();
  }
  
  protected final void unlinkNode(LinkedNode<?> node, long encoding) {
    if (last == encoding) {
      last = node.getPrevious();
    }
    if (first == encoding) {
      first = node.getNext();
    }
    if (node.getNext() != NULL_ENCODING) {
      LinkedNode<V> next = delegateStorageEngine.readValue(node.getNext());
      next.setPrevious(node.getPrevious());
      next.flush();
    }
    if (node.getPrevious() != NULL_ENCODING) {
      LinkedNode<V> previous = delegateStorageEngine.readValue(node.getPrevious());
      previous.setNext(node.getNext());
      previous.flush();
    }

    node.setNext(NULL_ENCODING);
    node.setPrevious(NULL_ENCODING);
  }

  protected final void linkNodeExpectingLast(LinkedNode<?> node, long encoding, long lsn) {
    if (lsn < 0) {
      throw new AssertionError("Received illegal lsn " + lsn);
    }

    if (last == NULL_ENCODING) {
      //insertion in empty list
      validate(!VALIDATING || first == NULL_ENCODING);
      last = encoding;
      first = encoding;
      return;
    }

    //insertion in non-empty list
    long previousEncoding = last;
    long nextEncoding;
    LinkedNode<?> previous = delegateStorageEngine.readValue(previousEncoding);
    while (true) {
      if (previous.getLsn() < lsn) {
        nextEncoding = previous.getNext();
        break;
      } else if (previous.getPrevious() == NULL_ENCODING) {
        nextEncoding = previousEncoding;
        previousEncoding = NULL_ENCODING;
        previous = null;
        break;
      }
      previousEncoding = previous.getPrevious();
      previous = delegateStorageEngine.readValue(previousEncoding);
    }

    if (nextEncoding == NULL_ENCODING) {
      //insertion at last
      validate(!VALIDATING || previousEncoding == last);
      last = encoding;
      node.setPrevious(previousEncoding);
      previous.setNext(encoding);
      previous.flush();
    } else {
      LinkedNode<?> next = delegateStorageEngine.readValue(nextEncoding);
      if (previousEncoding == NULL_ENCODING) {
        //insertion at first
        validate(!VALIDATING || nextEncoding == first);
        first = encoding;
        node.setNext(nextEncoding);
        next.setPrevious(encoding);
        next.flush();
      } else {
        //insertion in middle
        node.setNext(nextEncoding);
        node.setPrevious(previousEncoding);

        previous.setNext(encoding);
        next.setPrevious(encoding);
        previous.flush();
        next.flush();
      }
    }
  }
  
  protected final void linkNodeExpectingFirst(LinkedNode<?> node, long encoding, long lsn) {
    if (lsn < 0) {
      throw new AssertionError("Received illegal lsn " + lsn);
    }
    
    if (last == NULL_ENCODING) {
      //insertion in empty list
      validate(!VALIDATING || first == NULL_ENCODING);
      last = encoding;
      first = encoding;
      return;
    }
    
    //insertion in non-empty list
    long nextEncoding = first;
    long previousEncoding;
    LinkedNode<?> next = delegateStorageEngine.readValue(nextEncoding);
    while (true) {
      if (next.getLsn() > lsn) {
        previousEncoding = next.getPrevious();
        break;
      } else if (next.getNext() == NULL_ENCODING) {
        previousEncoding = nextEncoding;
        nextEncoding = NULL_ENCODING;
        next = null;
        break;
      }
      nextEncoding = next.getNext();
      next = delegateStorageEngine.readValue(nextEncoding);
    }

    if (previousEncoding == NULL_ENCODING) {
      //insertion at first
      validate(!VALIDATING || nextEncoding == first);
      first = encoding;
      node.setNext(nextEncoding);
      next.setPrevious(encoding);
      next.flush();
    } else {
      LinkedNode<?> previous = delegateStorageEngine.readValue(previousEncoding);
      if (nextEncoding == NULL_ENCODING) {
        //insertion at first
        validate(!VALIDATING || previousEncoding == last);
        last = encoding;
        node.setPrevious(previousEncoding);
        previous.setNext(encoding);
        previous.flush();
      } else {
        //insertion in middle
        node.setPrevious(previousEncoding);
        node.setNext(nextEncoding);
        
        next.setPrevious(encoding);
        previous.setNext(encoding);
        previous.flush();
        next.flush();
      }
    }
  }
  
  protected final void validChain() {
    if (VALIDATING) {
      //skip this expensive assertion for disk based storage engines
      if (delegateStorageEngine instanceof FileBackedStorageEngine<?, ?>) {
        return;
      }

      long previous = NULL_ENCODING;
      LinkedNode<?> previousNode = null;
      long current = first;

      while (true) {
        if (current == NULL_ENCODING) {
          validate(last == previous);
          break;
        }

        LinkedNode<?> currentNode = delegateStorageEngine.readValue(current);
        validate(!VALIDATING || currentNode.getPrevious() == previous);
        if (previousNode == null) {
          validate(previous == NULL_ENCODING);
        } else {
          validate(previousNode.getNext() == current);
        }
        previous = current;
        previousNode = currentNode;
        current = currentNode.getNext();
      }
    }
  }

  //Expect locking in caller...
  protected long firstEncoding() {
    return first;
  }
  
  //Expect locking in caller...
  protected long lastEncoding() {
    return last;
  }

  @Override
  public long size() {
    return owner.getSize();
  }

  @Override
  public Long getLowestLsn() {
    Lock l = readLock();
    l.lock();
    try {
      long lowest = firstEncoding();
      if (lowest == NULL_ENCODING) {
        return null;
      } else {
        return delegateStorageEngine.readValue(lowest).getLsn();
      }
    } finally {
      l.unlock();
    }
  }

  @Override
  public Long getLsn(int pojoHash, ByteBuffer frsBinaryKey) {
    ByteBuffer offheapBinaryKey = decodeKey(frsBinaryKey);
    Lock l = readLock();
    l.lock();
    try {
      Long encoding = lookupEncoding(pojoHash, offheapBinaryKey);
      if (encoding == null) {
        return null;
      } else {
        return delegateStorageEngine.readValue(encoding).getLsn();
      }
    } finally {
      l.unlock();
    }
  }

  @Override
  public void put(int pojoHash, ByteBuffer frsBinaryKey, ByteBuffer frsBinaryValue, long lsn) {
    //this encoding *must* be valid... this is not installed in the map yet...
    long encoding = extractEncoding(frsBinaryValue);
    Lock l = writeLock();
    l.lock();
    try {
      assignLsn(encoding, lsn);
    } finally {
      l.unlock();
    }
  }

  @Override
  public void remove(int pojoHash, ByteBuffer frsBinaryKey) {
    //lookupEncoding calls to the owner map which will do it's own locking
    validate(!VALIDATING || lookupEncoding(pojoHash, decodeKey(frsBinaryKey)) != null);
  }

  @Override
  public void replayPut(final int pojoHash, ByteBuffer frsBinaryKey, ByteBuffer frsBinaryValue, long lsn) {
    int metadata = extractMetadata(frsBinaryValue);
    ByteBuffer offheapBinaryKey = decodeKey(frsBinaryKey);
    ByteBuffer offheapBinaryValue = decodeValue(frsBinaryValue);
    
    Lock l = writeLock();
    l.lock();
    try {
      final long encoding = owner.installMappingForHashAndEncoding(pojoHash, offheapBinaryKey, offheapBinaryValue, metadata);
      LinkedNode<?> node = delegateStorageEngine.readValue(encoding);
      linkNodeExpectingFirst(node, encoding, lsn);
      node.setLsn(lsn);
      node.flush();
      validChain();
      if (hasRecoveryListeners()) {
        final Thread caller = Thread.currentThread();
        fireRecovered(new Callable<K>() {
          @Override
          public K call() throws Exception {
            if (caller == Thread.currentThread()) {
              return readKey(encoding, pojoHash);
            } else {
              throw new IllegalStateException();
            }
          }
        }, new Callable<V>() {
          @Override
          public V call() throws Exception {
            if (caller == Thread.currentThread()) {
              return readValue(encoding);
            } else {
              throw new IllegalStateException();
            }
          }
        }, readBinaryKey(encoding), readBinaryValue(encoding), pojoHash, metadata, encoding);
      }
    } finally {
      l.unlock();
    }
  }

  @Override
  public Long writeBinaryMapping(ByteBuffer binaryKey, ByteBuffer binaryValue, int pojoHash, int metadata) {
    return delegateStorageEngine.writeBinaryMapping(new ByteBuffer[] {binaryKey.duplicate()}, new ByteBuffer[] {LinkedNodePortability.emptyHeader(), binaryValue.duplicate()}, pojoHash, metadata);
  }

  @Override
  public Long writeBinaryMapping(ByteBuffer[] binaryKey, ByteBuffer[] binaryValue, int pojoHash, int metadata) {
    return delegateStorageEngine.writeBinaryMapping(duplicate(binaryKey), duplicate(LinkedNodePortability.emptyHeader(), binaryValue), pojoHash, metadata);
  }

  @Override
  public int readKeyHash(long encoding) {
    return delegateStorageEngine.readKeyHash(encoding);
  }

  @Override
  public ByteBuffer readBinaryKey(long encoding) {
    return delegateStorageEngine.readBinaryKey(encoding);
  }

  @Override
  public ByteBuffer readBinaryValue(long encoding) {
    return ((ByteBuffer) delegateStorageEngine.readBinaryValue(encoding).position(LinkedNodePortability.VALUE_OFFSET)).slice();
  }

  @Override
  public ObjectManagerEntry<I, ByteBuffer, ByteBuffer> acquireCompactionEntry(long ceilingLsn) {
    Lock l = writeLock();
    l.lock();
    long encoding = firstEncoding();
    if (encoding == NULL_ENCODING) {
      l.unlock();
      return null;
    }
    try {
      LinkedNode<?> node = delegateStorageEngine.readValue(encoding);

      if (node.getLsn() >= ceilingLsn) {
        l.unlock();
        return null;
      }

      // This exit means that the current thread will be holding the segment lock.
      return compactingEntry = new SimpleObjectManagerEntry<I, ByteBuffer, ByteBuffer>(identifier,
                                                                                encodeKey(readBinaryKey(encoding), readKeyHash(encoding)),
                                                                                encodeValue(readBinaryValue(encoding), encoding, deriveMetadata(encoding)),
                                                                                node.getLsn());
    } catch (RuntimeException e) {
      l.unlock();
      throw e;
    } catch (Error e) {
      l.unlock();
      throw e;
    } catch (Throwable e) {
      l.unlock();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void releaseCompactionEntry(ObjectManagerEntry<I, ByteBuffer, ByteBuffer> entry) {
    if (entry == null) {
      throw new NullPointerException("Tried to release a null entry.");
    }
    if (entry != compactingEntry) {
      throw new IllegalArgumentException("Released entry is not the same as acquired entry.");
    }
    compactingEntry = null;
    // Releasing the lock acquired in acquireFirstEntry().
    writeLock().unlock();
  }

  @Override
  public void updateLsn(int pojoHash, ObjectManagerEntry<I, ByteBuffer, ByteBuffer> entry, long newLsn) {
    // Expected to be called under the segment lock.
    if (entry != compactingEntry) {
      throw new IllegalArgumentException(
              "Tried to update the LSN on an entry that was not acquired.");
    }
    long encoding = extractEncoding(entry.getValue());
    validate(!VALIDATING || delegateStorageEngine.readValue(encoding).getLsn() == entry.getLsn());
    assignLsn(encoding, newLsn);
  }

  private Long lookupEncoding(int hash, ByteBuffer offHeapBinaryKey) {
    return owner.getEncodingForHashAndBinary(hash, offHeapBinaryKey);
  }
  
  protected int deriveMetadata(long encoding) {
    return 0;
  }

  public static ByteBuffer encodeKey(ByteBuffer offheapBinaryKey, int pojoHash) {
    //encode hashcode in first 4 bytes of key form
    ByteBuffer frsBinaryKey = ByteBuffer.allocate(offheapBinaryKey.remaining() + 4);
    frsBinaryKey.putInt(pojoHash).put(offheapBinaryKey).flip();
    return frsBinaryKey;
  }
  
  public static ByteBuffer decodeKey(ByteBuffer frsBinaryKey) {
    int origPos = frsBinaryKey.position();
    try {
      return ((ByteBuffer) frsBinaryKey.position(origPos + 4)).slice();
    } finally {
      frsBinaryKey.position(origPos);
    }
  }
  
  public static int extractHashcode(ByteBuffer frsBinaryKey) {
    return frsBinaryKey.getInt(0);
  }
  
  public static ByteBuffer encodeValue(ByteBuffer offheapBinaryValue, long encoding, int metadata) {
    //encode encoding in first 8 bytes of key form
    ByteBuffer frsBinaryValue = ByteBuffer.allocate(offheapBinaryValue.remaining() + 12);
    frsBinaryValue.putLong(encoding).putInt(metadata).put(offheapBinaryValue).flip();
    return frsBinaryValue;
  }
  
  public static ByteBuffer decodeValue(ByteBuffer frsBinaryValue) {
    int origPos = frsBinaryValue.position();
    try {
      return ((ByteBuffer) frsBinaryValue.position(origPos + 12)).slice();
    } finally {
      frsBinaryValue.position(origPos);
    }
  }
  
  public static int extractMetadata(ByteBuffer frsBinaryValue) {
    return frsBinaryValue.getInt(8) & ~Metadata.PINNED;
  }
  
  public static long extractEncoding(ByteBuffer frsBinaryValue) {
    return frsBinaryValue.getLong(0);
  }

  @Override
  public long sizeInBytes() {
    return delegateStorageEngine.getOccupiedMemory();
  }
  
  @Override
  public Long getEncodingForHashAndBinary(int hash, ByteBuffer offHeapBinaryKey) {
    return owner.getEncodingForHashAndBinary(hash, offHeapBinaryKey);
  }

  @Override
  public long getSize() {
    return owner.getSize();
  }

  @Override
  public long installMappingForHashAndEncoding(int pojoHash, ByteBuffer offheapBinaryKey, ByteBuffer offheapBinaryValue, int metadata) {
    return owner.installMappingForHashAndEncoding(pojoHash, offheapBinaryKey, offheapBinaryValue, metadata);
  }

  @Override
  public Iterable<Long> encodingSet() {
    return owner.encodingSet();
  }

  @Override
  public boolean updateEncoding(int hashCode, long lastAddress, long compressed, long mask) {
    if (owner.updateEncoding(hashCode, lastAddress, compressed, mask)) {
      LinkedNode<V> node = delegateStorageEngine.readValue(compressed);
      if (last == lastAddress) {
        last = compressed;
      }
      if (first == lastAddress) {
        first = compressed;
      }
      if (node.getNext() != NULL_ENCODING) {
        LinkedNode<V> next = delegateStorageEngine.readValue(node.getNext());
        next.setPrevious(compressed);
        next.flush();
      }
      if (node.getPrevious() != NULL_ENCODING) {
        LinkedNode<V> previous = delegateStorageEngine.readValue(node.getPrevious());
        previous.setNext(compressed);
        previous.flush();
      }
      node.flush();
      validChain();
      return true;
    } else {
      return false;
    }
  }

  @Override
  public Integer getSlotForHashAndEncoding(int hash, long address, long mask) {
    return owner.getSlotForHashAndEncoding(hash, address, mask);
  }

  @Override
  public boolean evict(int slot, boolean b) {
    return owner.evict(slot, b);
  }

  @Override
  public boolean isThiefForTableAllocations() {
    return owner.isThiefForTableAllocations();
  }

  @Override
  public Lock readLock() {
    return owner.readLock();
  }

  @Override
  public Lock writeLock() {
    return owner.writeLock();
  }
  
  private static ByteBuffer[] duplicate(ByteBuffer[] buffers) {
    ByteBuffer[] duplicate = new ByteBuffer[buffers.length];
    for (int i = 0; i < duplicate.length; i++) {
      duplicate[i] = buffers[i].duplicate();
    }
    return duplicate;
  }
  
  private static ByteBuffer[] duplicate(ByteBuffer pre, ByteBuffer[] buffers) {
    ByteBuffer[] duplicate = new ByteBuffer[buffers.length + 1];
    duplicate[0] = pre.duplicate();
    for (int i = 1; i < duplicate.length; i++) {
      duplicate[i] = buffers[i - 1].duplicate();
    }
    return duplicate;
  }
}
