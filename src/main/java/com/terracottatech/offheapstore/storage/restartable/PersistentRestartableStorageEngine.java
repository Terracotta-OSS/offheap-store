package com.terracottatech.offheapstore.storage.restartable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.offheapstore.disk.persistent.PersistentStorageEngine;
import com.terracottatech.offheapstore.storage.BinaryStorageEngine;
import com.terracottatech.offheapstore.util.Factory;

public class PersistentRestartableStorageEngine<T extends PersistentStorageEngine<K, LinkedNode<V>> & BinaryStorageEngine, I, K, V> extends RestartableStorageEngine<T, I, K, V> implements PersistentStorageEngine<K, V> {

  public static <T extends PersistentStorageEngine<K, LinkedNode<V>> & BinaryStorageEngine, I, K, V> Factory<PersistentRestartableStorageEngine<T, I, K, V>> createPersistentFactory(final I identifier, final RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, final Factory<T> delegateFactory, final boolean synchronous) {
    return new Factory<PersistentRestartableStorageEngine<T,I,K,V>>() {

      @Override
      public PersistentRestartableStorageEngine<T, I, K, V> newInstance() {
        return new PersistentRestartableStorageEngine<T, I, K, V>(identifier, transactionSource, delegateFactory.newInstance(), synchronous);
      }
    };
  }

  public PersistentRestartableStorageEngine(I identifier, RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, T storageEngine, boolean synchronous) {
    super(identifier, transactionSource, storageEngine, synchronous);
  }

  @Override
  public void flush() throws IOException {
    delegateStorageEngine.flush();
  }

  @Override
  public void close() throws IOException {
    delegateStorageEngine.close();
  }

  @Override
  public void persist(ObjectOutput output) throws IOException {
    delegateStorageEngine.persist(output);
  }

  @Override
  public void bootstrap(ObjectInput input) throws IOException {
    delegateStorageEngine.bootstrap(input);
  }
}
