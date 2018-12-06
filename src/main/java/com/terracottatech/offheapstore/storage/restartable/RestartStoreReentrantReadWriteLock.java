package com.terracottatech.offheapstore.storage.restartable;

import com.terracottatech.frs.NotPausedException;
import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.RestartStoreException;
import com.terracottatech.frs.Snapshot;
import com.terracottatech.frs.Statistics;
import com.terracottatech.frs.Transaction;
import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.Tuple;
import com.terracottatech.frs.recovery.RecoveryException;
import org.terracotta.offheapstore.util.FindbugsSuppressWarnings;

import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RestartStoreReentrantReadWriteLock<I, K, V> extends ReentrantReadWriteLock {

  private static final long serialVersionUID = -1119986092399074087L;
  
  private final LockScopedRestartStore<I, K, V> txnWriteLock;
  
  public RestartStoreReentrantReadWriteLock(RestartStore<I, K, V> restartability) {
    super();
    txnWriteLock = new LockScopedRestartStore<I, K, V>(restartability, this);
  }

  @Override
  public LockScopedRestartStore<I, K, V> writeLock() {
    return txnWriteLock;
  }
  
  static class LockScopedRestartStore<I, K, V> extends WriteLock implements RestartStore<I, K, V> {

    private static final long serialVersionUID = 1126153721546674833L;

    private final RestartStore<I, K, V> restartability;
    @FindbugsSuppressWarnings("SE_BAD_FIELD") //not expecting serialization
    private final ThreadLocal<WrappedTransaction<I, K, V>> transaction = new ThreadLocal<WrappedTransaction<I, K, V>>();
    
    protected LockScopedRestartStore(RestartStore<I, K, V> restartability, ReentrantReadWriteLock rrwl) {
      super(rrwl);
      this.restartability = restartability;
    }

    @Override
    public Future<Void> startup() throws InterruptedException, RecoveryException {
      return restartability.startup();
    }

    @Override
    public Snapshot snapshot() throws RestartStoreException {
      return restartability.snapshot();
    }

    @Override
    public void unlock() {
      super.unlock();
      WrappedTransaction<I, K, V> txn = transaction.get();
      if (txn == null) {
        transaction.remove();
      } else {
        if (!isHeldByCurrentThread()) {
          transaction.remove();
          try {
            txn.delegate.commit();
          } catch (TransactionException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    @Override
    public Transaction<I, K, V> beginTransaction(boolean synchronous) {
      if (isHeldByCurrentThread()) {
        WrappedTransaction<I, K, V> active = transaction.get();
        if (active == null) {
          active = new WrappedTransaction<I, K, V>(restartability.beginTransaction(synchronous));
          transaction.set(active);
        }
        return active;
      } else {
        throw new IllegalStateException();
      }
    }

    @Override
    public Transaction<I, K, V> beginAutoCommitTransaction(boolean synchronous) {
      return restartability.beginAutoCommitTransaction(synchronous);
    }

    @Override
    public void shutdown() throws InterruptedException {
      restartability.shutdown();
    }

    @Override
    public Tuple<I, K, V> get(long marker) {
      return restartability.get(marker);
    }

    @Override
    public Statistics getStatistics() {
      return restartability.getStatistics();
    }

    @Override
    public Future<Future<Snapshot>> pause() {
      return restartability.pause();
    }

    @Override
    public void resume() throws NotPausedException {
      restartability.resume();
    }

    @Override
    public Future<Future<Void>> freeze() {
      return restartability.freeze();
    }
  }

  static class WrappedTransaction<I, K, V> implements Transaction<I, K, V> {

    private Transaction<I, K, V> delegate;
    
    public WrappedTransaction(Transaction<I, K, V> delegate) {
      this.delegate = delegate;
    }

    @Override
    public Transaction<I, K, V> put(I id, K key, V value) throws TransactionException {
      delegate = delegate.put(id, key, value);
      return this;
    }

    @Override
    public Transaction<I, K, V> delete(I id) throws TransactionException {
      delegate = delegate.delete(id);
      return this;
    }

    @Override
    public Transaction<I, K, V> remove(I id, K key) throws TransactionException {
      delegate = delegate.remove(id, key);
      return this;
    }

    @Override
    public void commit() throws TransactionException {
      //no-op
    }
    
  }
}
