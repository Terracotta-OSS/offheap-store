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
package org.terracotta.offheapstore.disk.storage;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.terracotta.offheapstore.disk.paging.MappedPageSource;
import org.terracotta.offheapstore.disk.persistent.Persistent;
import org.terracotta.offheapstore.disk.persistent.PersistentStorageEngine;
import org.terracotta.offheapstore.storage.PortabilityBasedStorageEngine;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.storage.portability.WriteContext;
import org.terracotta.offheapstore.util.Factory;
import org.terracotta.offheapstore.util.ReopeningInterruptibleChannel;
import org.terracotta.offheapstore.util.MemoryUnit;

import static java.lang.Long.highestOneBit;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 *
 * @author Chris Dennis
 */
public class FileBackedStorageEngine<K, V> extends PortabilityBasedStorageEngine<K, V> implements PersistentStorageEngine<K, V> {

  private static final int MAGIC = 0x414d4445;
  private static final int MAGIC_CHUNK = 0x4e4e4953;

  private static final Logger LOGGER = LoggerFactory.getLogger(FileBackedStorageEngine.class);

  private static final int KEY_HASH_OFFSET = 0;
  private static final int KEY_LENGTH_OFFSET = 4;
  private static final int VALUE_LENGTH_OFFSET = 8;
  private static final int KEY_DATA_OFFSET = 12;

  private final ConcurrentHashMap<Long, FileWriteTask> pendingWrites = new ConcurrentHashMap<Long, FileWriteTask>();
  private final ExecutorService writeExecutor;

  private final MappedPageSource source;

  private final ReopeningInterruptibleChannel<FileChannel> writeChannel;
  private final ReopeningInterruptibleChannel<FileChannel> readChannel;

  private final TreeMap<Long, FileChunk> chunks = new TreeMap<Long, FileChunk>();
  private final long maxChunkSize;

  private volatile Owner owner;

  public static <K, V> Factory<FileBackedStorageEngine<K, V>> createFactory(final MappedPageSource source, final long maxChunkSize, final MemoryUnit maxChunkUnit, final Portability<? super K> keyPortability, final Portability<? super V> valuePortability) {
    return createFactory(source, maxChunkSize, maxChunkUnit, keyPortability, valuePortability, true);
  }

  public static <K, V> Factory<FileBackedStorageEngine<K, V>> createFactory(final MappedPageSource source, final long maxChunkSize, final MemoryUnit maxChunkUnit, final Portability<? super K> keyPortability, final Portability<? super V> valuePortability, final boolean bootstrap) {
    Factory<ExecutorService> executorFactory = new Factory<ExecutorService>() {
      @Override
      public ExecutorService newInstance() {
        return new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
      }
    };
    return createFactory(source, maxChunkSize, maxChunkUnit, keyPortability, valuePortability, executorFactory, bootstrap);
  }

  public static <K, V> Factory<FileBackedStorageEngine<K, V>> createFactory(final MappedPageSource source, final long maxChunkSize, final MemoryUnit maxChunkUnit, final Portability<? super K> keyPortability, final Portability<? super V> valuePortability, final Factory<ExecutorService> executorFactory, final boolean bootstrap) {
    return new Factory<FileBackedStorageEngine<K, V>>() {
      @Override
      public FileBackedStorageEngine<K, V> newInstance() {
        return new FileBackedStorageEngine<K, V>(source, maxChunkSize, maxChunkUnit, keyPortability, valuePortability, executorFactory.newInstance(), bootstrap);
      }
    };
  }

  public FileBackedStorageEngine(MappedPageSource source, final long maxChunkSize, final MemoryUnit maxChunkUnit, Portability<? super K> keyPortability, Portability<? super V> valuePortability) {
    this(source, maxChunkSize, maxChunkUnit, keyPortability, valuePortability, true);
  }

  public FileBackedStorageEngine(MappedPageSource source, final long maxChunkSize, final MemoryUnit maxChunkUnit, Portability<? super K> keyPortability, Portability<? super V> valuePortability, ExecutorService writer) {
    this(source, maxChunkSize, maxChunkUnit, keyPortability, valuePortability, writer, true);
  }

  public FileBackedStorageEngine(MappedPageSource source, final long maxChunkSize, final MemoryUnit maxChunkUnit, Portability<? super K> keyPortability, Portability<? super V> valuePortability, boolean bootstrap) {
    this(source, maxChunkSize, maxChunkUnit, keyPortability, valuePortability, new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>()), bootstrap);
  }

  public FileBackedStorageEngine(MappedPageSource source, long maxChunkSize, MemoryUnit maxChunkUnit, Portability<? super K> keyPortability, Portability<? super V> valuePortability, ExecutorService writer, boolean bootstrap) {
    super(keyPortability, valuePortability);

    this.writeExecutor = writer;
    this.writeChannel = ReopeningInterruptibleChannel.create(source::getWritableChannel);
    this.readChannel = ReopeningInterruptibleChannel.create(source::getReadableChannel);

    this.source = source;
    this.maxChunkSize = highestOneBit(maxChunkUnit.toBytes(maxChunkSize));
  }

  @Override
  protected void clearInternal() {
    Iterator<Entry<Long, FileChunk>> it = chunks.entrySet().iterator();
    while (it.hasNext()) {
      it.next().getValue().clear();
      it.remove();
    }
    if (!chunks.isEmpty()) {
      throw new AssertionError("Concurrent modification while clearing!");
    }
  }

  @Override
  public void destroy() {
    try {
      close();
    } catch (IOException e) {
      LOGGER.warn("Exception while trying to close file backed storage engine", e);
    }
  }

  @Override
  public void flush() throws IOException {
    Future<Void> flush = writeExecutor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        return writeChannel.execute(channel -> {
          channel.force(true);
          return null;
        });
      }
    });

    boolean interrupted = false;
    try {
      while (true) {
        try {
          flush.get();
          break;
        } catch (InterruptedException ex) {
          interrupted = true;
        } catch (ExecutionException ex) {
          Throwable cause = ex.getCause();
          if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
          } else if (cause instanceof IOException) {
            throw (IOException) cause;
          } else {
            throw new RuntimeException(cause);
          }
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      writeExecutor.shutdownNow();
      if (writeExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
        LOGGER.debug("FileBackedStorageEngine for " + source.getFile().getName() + " terminated successfully");
      } else {
        LOGGER.warn("FileBackedStorageEngine for " + source.getFile().getName() + " timed-out during termination");
      }
    } catch (InterruptedException e) {
      LOGGER.warn("FileBackedStorageEngine for " + source.getFile().getName() + " interrupted during termination");
      Thread.currentThread().interrupt();
    } finally {
      try {
        writeChannel.close();
      } finally {
        readChannel.close();
      }
    }
  }

  @Override
  public void persist(ObjectOutput output) throws IOException {
    output.writeInt(MAGIC);
    ((Persistent) keyPortability).persist(output);
    ((Persistent) valuePortability).persist(output);
    output.writeInt(chunks.size());
    for (FileChunk c : chunks.values()) {
      c.persist(output);
    }
  }

  @Override
  public void bootstrap(ObjectInput input) throws IOException {
    if (!chunks.isEmpty()) {
      throw new IllegalStateException();
    }
    if (input.readInt() != MAGIC) {
      throw new IOException("Wrong magic number");
    }

    ((Persistent) keyPortability).bootstrap(input);
    ((Persistent) valuePortability).bootstrap(input);

    int n = input.readInt();
    for (int i = 0; i < n; i++) {
      FileChunk chunk = new FileChunk(input);
      chunks.put(chunk.baseAddress(), chunk);
    }

    if (hasRecoveryListeners()) {
      for (Long encoding : owner.encodingSet()) {
        ByteBuffer binaryKey = readBinaryKey(encoding);
        ByteBuffer binaryValue = readBinaryValue(encoding);
        int hash = readKeyHash(encoding);

        final ByteBuffer binaryKeyForDecode = binaryKey.duplicate();
        final ByteBuffer binaryValueForDecode = binaryValue.duplicate();
        final Thread caller = Thread.currentThread();
        fireRecovered(new Callable<K>() {

          @Override
          public K call() throws Exception {
            if (caller == Thread.currentThread()) {
              return (K) keyPortability.decode(binaryKeyForDecode.duplicate());
            } else {
              throw new IllegalStateException();
            }
          }
        }, new Callable<V>() {

          @Override
          public V call() throws Exception {
            if (caller == Thread.currentThread()) {
              return (V) valuePortability.decode(binaryValueForDecode.duplicate());
            } else {
              throw new IllegalStateException();
            }
          }
        }, binaryKey, binaryValue, hash, 0, encoding);
      }
    }
  }

  @Override
  protected void free(long address) {
    FileChunk chunk = findChunk(address);
    chunk.free(address - chunk.baseAddress());
  }

  @Override
  protected ByteBuffer readKeyBuffer(long address) {
    FileChunk chunk = findChunk(address);
    return chunk.readKeyBuffer(address - chunk.baseAddress());
  }

  @Override
  protected WriteContext getKeyWriteContext(long address) {
    FileChunk chunk = findChunk(address);
    return chunk.getKeyWriteContext(address - chunk.baseAddress());
  }

  @Override
  protected ByteBuffer readValueBuffer(long address) {
    FileChunk chunk = findChunk(address);
    return chunk.readValueBuffer(address - chunk.baseAddress());
  }

  @Override
  protected WriteContext getValueWriteContext(long address) {
    FileChunk chunk = findChunk(address);
    return chunk.getValueWriteContext(address - chunk.baseAddress());
  }

  @Override
  protected Long writeMappingBuffers(ByteBuffer keyBuffer, ByteBuffer valueBuffer, int hash) {
    for (FileChunk c : chunks.values()) {
      Long address = c.writeMappingBuffers(keyBuffer, valueBuffer, hash);
      if (address != null) {
        return address + c.baseAddress();
      }
    }

    while (true) {
      long requiredSize = keyBuffer.remaining() + valueBuffer.remaining() + KEY_DATA_OFFSET;
      if (Long.bitCount(requiredSize) != 1) {
          requiredSize = Long.highestOneBit(requiredSize) << 1;
      }

      long nextChunkSize;
      long nextChunkBaseAddress;
      if (chunks.isEmpty()) {
        nextChunkSize = requiredSize;
        nextChunkBaseAddress = 0L;
      } else {
        FileChunk last = chunks.lastEntry().getValue();
        nextChunkSize = max(min(last.capacity() << 1, maxChunkSize), requiredSize);
        nextChunkBaseAddress = last.baseAddress() + last.capacity();

        if (nextChunkSize < 0) {
          return null;
        }
      }

      FileChunk c;
      try {
        c = new FileChunk(nextChunkSize, nextChunkBaseAddress);
      } catch (OutOfMemoryError e) {
        return null;
      }
      chunks.put(c.baseAddress(), c);
      Long address = c.writeMappingBuffers(keyBuffer, valueBuffer, hash);
      if (address != null) {
        return address + c.baseAddress();
      }
    }
  }

  @Override
  public long getAllocatedMemory() {
    long sum = 0;
    for (FileChunk c : chunks.values()) {
      sum += c.capacity();
    }
    return sum;
  }

  @Override
  public long getOccupiedMemory() {
    long sum = 0;
    for (FileChunk c : chunks.values()) {
      sum += c.occupied();
    }
    return sum;
  }

  @Override
  public long getVitalMemory() {
    return getAllocatedMemory();
  }

  @Override
  public long getDataSize() {
    long sum = 0;
    for (FileChunk c : chunks.values()) {
      sum += c.occupied();
    }
    return sum;
  }

  private FileChunk findChunk(long address) {
    return chunks.floorEntry(address).getValue();
  }

  private void writeLongToChannel(long position, long data) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE / Byte.SIZE);
    buffer.putLong(data).flip();
    writeToChannel(position, buffer);
  }

  private void writeToChannel(long position, ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      final long pos = position;
      int written = writeChannel.execute(channel -> channel.write(buffer, pos));
      if (written < 0) {
        throw new EOFException();
      } else {
        position += written;
      }
    }
  }

  private int readFromChannel(ByteBuffer buffer, long position) throws IOException {
    return readChannel.execute(channel -> readFromChannel(channel, buffer, position));
  }

  private int readFromChannel(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
    int ret = channel.read(buffer, position);
    if (ret < 0) {
      ret = channel.read(buffer, position);
    }
    return ret;
  }

  @Override
  public boolean shrink() {
    final Lock ownerLock = owner.writeLock();
    ownerLock.lock();
    try {
      if (chunks.isEmpty()) {
        return false;
      } else {
        FileChunk candidate = chunks.lastEntry().getValue();

        for (FileChunk c : chunks.descendingMap().values()) {
          c.evictAll();
          compress(c);

          compress(candidate);
          if (candidate.occupied() == 0) {
            chunks.remove(candidate.baseAddress()).clear();
            return true;
          }
        }
        return false;
      }
    } finally {
      ownerLock.unlock();
    }
  }

  private void compress(FileChunk from) {
    for (Long encoding : from.encodings()) {
      ByteBuffer keyBuffer = readKeyBuffer(encoding);
      int keyHash = readKeyHash(encoding);
      ByteBuffer valueBuffer = readValueBuffer(encoding);
      for (FileChunk to : chunks.headMap(from.baseAddress(), true).values()) {
        Long address = to.writeMappingBuffers(keyBuffer, valueBuffer, keyHash);
        if (address != null) {
          long compressed = address + to.baseAddress();
          if (compressed < encoding && owner.updateEncoding(keyHash, encoding, compressed, ~0)) {
            free(encoding);
          } else {
            free(compressed);
          }
          break;
        }
      }
    }
  }

  @Override
  public void bind(Owner m) {
    if (owner != null) {
      throw new AssertionError();
    }
    owner = m;
  }

  class FileChunk {
    private final AATreeFileAllocator allocator;
    private final long filePosition;
    private final long baseAddress;

    private boolean valid = true;

    FileChunk(long size, long baseAddress) {
      Long newOffset = source.allocateRegion(size);
      if (newOffset == null) {
        StringBuilder sb = new StringBuilder("Storage engine file data area allocation failed:\n");
        sb.append("Allocator: ").append(source);
        throw new OutOfMemoryError(sb.toString());
      } else {
        this.filePosition = newOffset;
      }
      this.allocator = new AATreeFileAllocator(size);
      this.baseAddress = baseAddress;
    }

    FileChunk(ObjectInput input) throws IOException {
      if (input.readInt() != MAGIC_CHUNK) {
        throw new IOException("Wrong magic number");
      }
      this.filePosition = input.readLong();
      this.baseAddress = input.readLong();

      long size = input.readLong();
      source.claimRegion(filePosition, size);

      this.allocator = new AATreeFileAllocator(size, input);
    }

    ByteBuffer readKeyBuffer(long address) {
      long position = filePosition + address;
      FileWriteTask pending = pendingWrites.get(position);
      if (pending == null) {
        int keyLength = readBuffer(position + KEY_LENGTH_OFFSET, 4).getInt(0);
        return readBuffer(position + KEY_DATA_OFFSET, keyLength);
      } else {
        return pending.getKeyBuffer();
      }
    }

    protected int readPojoHash(long address) {
      long position = filePosition + address;
      FileWriteTask pending = pendingWrites.get(position);
      if (pending == null) {
        return readBuffer(position + KEY_HASH_OFFSET, 4).getInt(0);
      } else {
        return pending.pojoHash;
      }
    }

    ByteBuffer readValueBuffer(long address) {
      long position = filePosition + address;
      FileWriteTask pending = pendingWrites.get(position);
      if (pending == null) {
        ByteBuffer lengths = readBuffer(position + KEY_LENGTH_OFFSET, 8);
        int keyLength = lengths.getInt(0);
        int valueLength = lengths.getInt(VALUE_LENGTH_OFFSET - KEY_LENGTH_OFFSET);
        return readBuffer(position + keyLength + KEY_DATA_OFFSET, valueLength);
      } else {
        return pending.getValueBuffer();
      }
    }

    ByteBuffer readBuffer(long position, int length) {
      try {
        ByteBuffer data = ByteBuffer.allocate(length);
        for (int i = 0; data.hasRemaining(); ) {
          int read = readFromChannel(data, position + i);
          if (read < 0) {
            throw new EOFException();
          } else {
            i += read;
          }
        }
        return (ByteBuffer) data.rewind();
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (OutOfMemoryError e) {
        LOGGER.error("Failed to allocate direct buffer for FileChannel read.  "
                + "Consider increasing the -XX:MaxDirectMemorySize property to "
                + "allow enough space for the FileChannel transfer buffers");
        throw e;
      }
    }

    Long writeMappingBuffers(ByteBuffer keyBuffer, ByteBuffer valueBuffer, int pojoHash) {
      int keyLength = keyBuffer.remaining();
      int valueLength = valueBuffer.remaining();
      long address = allocator.allocate(keyLength + valueLength + KEY_DATA_OFFSET);

      if (address >= 0) {
        long position = filePosition + address;
        FileWriteTask task = new FileWriteTask(this, position, keyBuffer, valueBuffer, pojoHash);
        pendingWrites.put(position, task);
        writeExecutor.execute(task);
        return address;
      } else {
        return null;
      }
    }

    WriteContext getKeyWriteContext(long address) {
      long position = filePosition + address;
      FileWriteTask pending = pendingWrites.get(position);
      if (pending == null) {
        int keyLength = readBuffer(position + KEY_LENGTH_OFFSET, 4).getInt(0);
        return getDiskWriteContext(position + KEY_DATA_OFFSET, keyLength);
      } else {
        if (pendingWrites.get(position) != pending) {
          return getKeyWriteContext(address);
        } else {
          return getQueuedWriteContext(pending, pending.getKeyBuffer());
        }
      }
    }

    WriteContext getValueWriteContext(long address) {
      long position = filePosition + address;
      FileWriteTask pending = pendingWrites.get(position);
      if (pending == null) {
        ByteBuffer lengths = readBuffer(position + KEY_LENGTH_OFFSET, 8);
        int keyLength = lengths.getInt(0);
        int valueLength = lengths.getInt(VALUE_LENGTH_OFFSET - KEY_LENGTH_OFFSET);
        return getDiskWriteContext(position + keyLength + KEY_DATA_OFFSET, valueLength);
      } else {
        if (pendingWrites.get(position) != pending) {
          return getValueWriteContext(address);
        } else {
          return getQueuedWriteContext(pending, pending.getValueBuffer());
        }
      }
    }

    private WriteContext getDiskWriteContext(final long address, final int max) {
      return new WriteContext() {
        @Override
        public void setLong(int offset, long value) {
          if (offset < 0 || offset >= max) {
            throw new IllegalArgumentException();
          } else {
            try {
              writeLongToChannel(address + offset, value);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }

        @Override
        public void flush() {
          //no-op
        }
      };
    }

    private WriteContext getQueuedWriteContext(final FileWriteTask current, final ByteBuffer queuedBuffer) {
      return new WriteContext() {
        @Override
        public void setLong(int offset, long value) {
          queuedBuffer.putLong(offset, value);
        }

        @Override
        public void flush() {
          FileWriteTask flush = new FileWriteTask(current.chunk, current.position, current.keyBuffer, current.valueBuffer, current.pojoHash);
          pendingWrites.put(flush.position, flush);
          writeExecutor.execute(flush);
        }
      };
    }

    void free(long address) {
      long position = filePosition + address;
      int keyLength;
      int valueLength;
      FileWriteTask pending = pendingWrites.remove(position);
      if (pending == null) {
        ByteBuffer lengths = readBuffer(position + KEY_LENGTH_OFFSET, 8);
        keyLength = lengths.getInt(0);
        valueLength = lengths.getInt(VALUE_LENGTH_OFFSET - KEY_LENGTH_OFFSET);
      } else {
        keyLength = pending.getKeyBuffer().remaining();
        valueLength = pending.getValueBuffer().remaining();
      }
      allocator.free(address, keyLength + valueLength + KEY_DATA_OFFSET);
    }

    synchronized void clear() {
      source.freeRegion(filePosition);
      valid = false;
    }

    long capacity() {
      return allocator.capacity();
    }

    long occupied() {
      return allocator.occupied();
    }

    long baseAddress() {
      return baseAddress;
    }

    void persist(ObjectOutput output) throws IOException {
      output.writeInt(MAGIC_CHUNK);
      output.writeLong(filePosition);
      output.writeLong(baseAddress);
      output.writeLong(allocator.capacity());
      allocator.persist(output);
    }

    synchronized boolean isValid() {
      return valid;
    }

    Set<Long> encodings() {
      Set<Long> encodings = new HashSet<Long>();
      for (Long encoding : owner.encodingSet()) {
        long relative = encoding - baseAddress();
        if (relative >= 0 && relative < capacity()) {
          encodings.add(encoding);
        }
      }
      return encodings;
    }

    void evictAll() {
      for (long encoding : encodings()) {
        int slot = owner.getSlotForHashAndEncoding(readPojoHash(encoding - baseAddress()), encoding, ~0L);
        owner.evict(slot, true);
      }
    }
  }

  class FileWriteTask implements Runnable {

    private final FileChunk chunk;
    private final ByteBuffer keyBuffer;
    private final ByteBuffer valueBuffer;
    private final int pojoHash;
    private final long position;

    FileWriteTask(FileChunk chunk, long position, ByteBuffer keyBuffer, ByteBuffer valueBuffer, int pojoHash) {
      this.chunk = chunk;
      this.position = position;
      this.keyBuffer = keyBuffer;
      this.valueBuffer = valueBuffer;
      this.pojoHash = pojoHash;
    }

    @Override
    public void run() {
      if (pendingWrites.get(position) == this) {
        try {
          synchronized (chunk) {
            if (chunk.isValid()) {
              try {
                try {
                  write();
                } catch (IOException e) {
                  LOGGER.warn("Received IOException '{}' while trying to write @ {} : trying again", e.getMessage(), position);
                  write();
                }
              } catch (ClosedChannelException e) {
                LOGGER.debug("DiskWriteTask terminated due to closed channel - we must be shutting down", e);
              } catch (IOException e) {
                LOGGER.warn("Received IOException '{}' during write @ {} : giving up", e.getMessage(), position);
              } catch (OutOfMemoryError e) {
                LOGGER.error("Failed to allocate a direct buffer for a FileChannel write.  "
                        + "Consider increasing the -XX:MaxDirectMemorySize property to "
                        + "allow enough space for the FileChannel transfer buffers");
                throw e;
              }
            }
          }
        } finally {
          pendingWrites.remove(position, this);
        }
      }
    }

    private void write() throws IOException {
      ByteBuffer key = getKeyBuffer();
      ByteBuffer value = getValueBuffer();
      int keyLength = key.remaining();
      int valueLength = value.remaining();

      ByteBuffer header = ByteBuffer.allocate(12);
      header.putInt(KEY_HASH_OFFSET, pojoHash);
      header.putInt(KEY_LENGTH_OFFSET, keyLength);
      header.putInt(VALUE_LENGTH_OFFSET, valueLength);
      writeToChannel(position + KEY_HASH_OFFSET, header);
      writeToChannel(position + KEY_DATA_OFFSET, key);
      writeToChannel(position + KEY_DATA_OFFSET + keyLength, value);


      long size = writeChannel.execute(FileChannel::size);
      long expected = position + keyLength + valueLength + KEY_DATA_OFFSET;
      if (size < expected) {
        throw new IOException("File size does not encompass last write [size:" + size + " end-of-write:" + expected);
      }
    }

    ByteBuffer getKeyBuffer() {
      return keyBuffer.duplicate();
    }

    ByteBuffer getValueBuffer() {
      return valueBuffer.duplicate();
    }
  }

  @Override
  public int readKeyHash(long address) {
    FileChunk chunk = findChunk(address);
    return chunk.readPojoHash(address - chunk.baseAddress());
  }
}
