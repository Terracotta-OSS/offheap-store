package com.terracottatech.offheapstore.storage.restartable;

import org.junit.Test;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.RestartStoreException;
import com.terracottatech.frs.Snapshot;
import com.terracottatech.frs.Statistics;
import com.terracottatech.frs.Transaction;
import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.Tuple;
import com.terracottatech.frs.util.NullFuture;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

public class RestartStoreReentrantReadWriteLockIT {

  @Test
  public void testNoTransactionWhenUnlocked() {
    RestartStoreReentrantReadWriteLock<Object, Object, Object> lock = new RestartStoreReentrantReadWriteLock<Object, Object, Object>(new DummyRestartStore());
    try {
      lock.writeLock().beginTransaction(true);
      Assert.fail();
    } catch (IllegalStateException e) {
      //expected
    }
  }
  
  @Test
  public void testNoTransactionWhenReadLocked() {
    RestartStoreReentrantReadWriteLock<Object, Object, Object> lock = new RestartStoreReentrantReadWriteLock<Object, Object, Object>(new DummyRestartStore());
    lock.readLock().lock();
    try {
      lock.writeLock().beginTransaction(true);
      Assert.fail();
    } catch (IllegalStateException e) {
      //expected
    } finally {
      lock.readLock().unlock();
    }
  }
  
  @Test
  public void testTransactionWhenLocked() throws TransactionException, InterruptedException {
    RestartStoreReentrantReadWriteLock<Object, Object, Object> lock = new RestartStoreReentrantReadWriteLock<Object, Object, Object>(new DummyRestartStore());
    Transaction<Object, Object, Object> txn;
    lock.writeLock().lock();
    try {
      txn = lock.writeLock().beginTransaction(true);
      Assert.assertNotNull(txn);
      Assert.assertSame(txn, lock.writeLock().beginTransaction(true));
      txn.commit();
      txn.commit();
    } finally {
      lock.writeLock().unlock();
    }
    try {
      lock.writeLock().beginTransaction(true);
      Assert.fail();
    } catch (IllegalStateException e) {
      //expected
    }
  }

  static class DummyRestartStore implements RestartStore<Object, Object, Object> {
    @Override
    public Future<Void> startup() {
      return new NullFuture();
    }

    @Override
    public Transaction<Object, Object, Object> beginTransaction(boolean synchronous) {
      return new DummyTransaction();
    }

    @Override
    public Transaction<Object, Object, Object> beginAutoCommitTransaction(boolean synchronous) {
      throw new AssertionError();
    }

    @Override
    public void shutdown() throws InterruptedException {
      throw new AssertionError();
    }

    @Override
    public Snapshot snapshot() throws RestartStoreException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Tuple<Object, Object, Object> get(long marker) {
      throw new AssertionError();
    }

    @Override
    public Statistics getStatistics() {
      throw new UnsupportedOperationException();
    }
  }
  
  static class DummyTransaction implements Transaction<Object, Object, Object> {

    public final AtomicBoolean committed = new AtomicBoolean();
    
    @Override
    public Transaction<Object, Object, Object> put(Object id, Object key, Object value) throws TransactionException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Transaction<Object, Object, Object> delete(Object id) throws TransactionException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Transaction<Object, Object, Object> remove(Object id, Object key) throws TransactionException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void commit() throws TransactionException {
      Assert.assertTrue(committed.compareAndSet(false, true));
    }
    
  }
}
