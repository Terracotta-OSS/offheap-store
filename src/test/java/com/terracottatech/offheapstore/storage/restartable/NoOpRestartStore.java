/*
 * Copyright 2014-2023 Terracotta, Inc., a Software AG company.
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

import com.terracottatech.frs.NotPausedException;
import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.RestartStoreException;
import com.terracottatech.frs.Snapshot;
import com.terracottatech.frs.Statistics;
import com.terracottatech.frs.Transaction;
import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.Tuple;
import com.terracottatech.frs.util.NullFuture;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Future;

public class NoOpRestartStore<I, K, V> implements RestartStore<I, K, V> {
  @Override
  public Future<Void> startup() {
    return new NullFuture();
  }

  @Override
  public void shutdown() {
  }

  @Override
  public Transaction<I, K, V> beginTransaction(boolean synchronous) {
    return new NoOpTransaction<I, K, V>();
  }

  @Override
  public Transaction<I, K, V> beginAutoCommitTransaction(boolean synchronous) {
    return new NoOpTransaction<I, K, V>();
  }

  @Override
  public Snapshot snapshot() throws RestartStoreException {
    return new Snapshot() {

      @Override
      public void close() throws IOException {
        // no op
      }

      @Override
      public Iterator<File> iterator() {
        return new Iterator<File>() {
          @Override
          public boolean hasNext() {
            return false;
          }

          @Override
          public File next() {
            throw new NoSuchElementException();
          }

          @Override
          public void remove() {
            throw new IllegalStateException();
          }
        };
      }
    };
  }

  @Override
  public Tuple<I, K, V> get(long marker) {
    return null;
  }

  @Override
  public Statistics getStatistics() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Future<Future<Snapshot>> pause() {
    return null;
  }

  @Override
  public void resume() throws NotPausedException {

  }

  @Override
  public Future<Future<Void>> freeze() {
    return null;
  }

  public static class NoOpTransaction<I, K, V> implements Transaction<I, K, V> {

    @Override
    public Transaction<I, K, V> put(I id, K key, V value) throws TransactionException {
      return this;
    }

    @Override
    public Transaction<I, K, V> delete(I id) throws TransactionException {
      return this;
    }

    @Override
    public Transaction<I, K, V> remove(I id, K key) throws TransactionException {
      return this;
    }

    @Override
    public void commit() throws TransactionException {
    }
  }
}
